"""
OECD Dataset Downloader
-----------------------
Downloads all OECD datasets (data + metadata) via the SDMX REST API.

Each dataset is a plain table with multiple columns.
The columns can be of three types:
 - Measure: typically with the most important observation value stored under OBS_VALUE.
 - Dimension: fixed properties like TIME_PERIOD, LOCATION, SUBJECT, MEASURE etc.
 - Attribute: metadata about the observation such as OBS_STATUS, UNIT_MULT, DECIMALS, etc.
 - DATAFLOW: the source dataflow from which the data got serialized.

To make the data easy to be read by the downstream agent we apply these transformation:
 - We convert the value codes in a given column with their long descriptions (the conversion to the compressed parquet keep the data size under control)
 - We remove all the attributes, and the DATAFLOW column.
 - We keep only the OBS_VALUE measure, renamed to "observation".
 """
import json
import time
import logging
from io import StringIO
from typing import Optional

import requests
import pandas as pd
from urllib.parse import quote
from xml.etree import ElementTree as ET

from aws_data_analyst.datasets.oecd import OECD_DATASETS
from aws_data_analyst.datasets import normalize_dataset_id
from aws_data_analyst.datasets import standard_dataset_decription


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://sdmx.oecd.org/public/rest"
LOG_LEVEL = logging.INFO

# SDMX namespaces (Clark notation)
NS_STR = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
NS_COM = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"

# xml:lang uses the W3C XML namespace – must be expressed in Clark notation
# because Python's ElementTree never auto-registers the "xml" prefix.
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------
def find_en(parent: ET.Element, clark_tag: str) -> ET.Element | None:
    """
    Return the first descendant matching clark_tag whose xml:lang starts with
    'en', falling back to the very first match regardless of language.
    Never uses XPath attribute predicates (avoids the 'xml' prefix SyntaxError).
    """
    first = None
    for el in parent.iter(clark_tag):
        if first is None:
            first = el
        if el.get(XML_LANG, "").lower().startswith("en"):
            return el
    return first


def text_en(parent: ET.Element, clark_tag: str, fallback: str = "") -> str:
    el = find_en(parent, clark_tag)
    return el.text.strip() if el is not None and el.text else fallback


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = 120     # seconds per HTTP request
MAX_RETRIES = 3


def http_get(url: str, params: Optional[dict] = None,
             accept: str = "application/xml") -> requests.Response | None:
    """GET with retries."""
    headers = {
        "Accept": accept,
        "User-Agent": "oecd-data-downloader/1.0"
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            
            print("Wait for 1 minute: API access is currently restricted to a maximum of 60 requests per hour")
            time.sleep(60)

            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                log.warning("404 Not Found: %s", url)
                return None
            elif resp.status_code == 429 or resp.status_code >= 500:
                log.warning(f"Too Many Requests: {resp.headers}")
                continue
            log.warning("HTTP %s: %s", resp.status_code, url)
            return None
        except requests.RequestException as exc:
            log.warning(f"Request error ({exc}) attempt {attempt}/{MAX_RETRIES}: {url}")
    log.error("Giving up after %d attempts: %s", MAX_RETRIES, url)
    return None


# ---------------------------------------------------------------------------
# Step 1 - Enumerate all dataflows
# ---------------------------------------------------------------------------
def list_dataflows(allow_list=None) -> list[dict]:
    log.info("Fetching full dataflow catalogue ...")
    resp = http_get(f"{BASE_URL}/dataflow/all/all/latest")
    if resp is None:
        raise RuntimeError("Could not retrieve dataflow catalogue")

    root = ET.fromstring(resp.content)
    dataflows = []

    for df_el in root.iter(f"{{{NS_STR}}}Dataflow"):
        df_id   = df_el.get("id", "")
        if allow_list is not None and df_id not in allow_list:
            continue
        
        agency  = df_el.get("agencyID", "")
        version = df_el.get("version", "1.0")

        title       = text_en(df_el, f"{{{NS_COM}}}Name",        fallback=df_id)
        description = text_en(df_el, f"{{{NS_COM}}}Description", fallback="")

        # The DSD reference is a <com:Ref> inside <str:Structure>.
        # Its attributes are:  agencyID, id, version, class="DataStructure"
        # We must use those verbatim – do NOT derive the id from the dataflow id.
        # <Ref> inside <Structure> is inconsistently namespaced across OECD
        # dataflows: sometimes ns2:Ref (NS_COM), sometimes a bare <Ref> with
        # no namespace.  Try all variants in order.
        dsd_ref = (
            df_el.find(f".//{{{NS_STR}}}Structure/{{{NS_COM}}}Ref")
            or df_el.find(f".//{{{NS_STR}}}Structure/Ref")
            or df_el.find(f".//{{{NS_COM}}}Ref")
            or df_el.find(".//Ref")
        )

        if dsd_ref is not None:
            dsd_agency  = dsd_ref.get("agencyID", agency)
            dsd_id      = dsd_ref.get("id", "")
            dsd_version = dsd_ref.get("version", "latest")
        else:
            # Last resort: guess – but log so we can see it
            dsd_agency  = agency
            dsd_id      = f"DSD_{df_id}"
            dsd_version = "latest"
            log.debug("No DSD Ref found for dataflow %s – guessing %s", df_id, dsd_id)

        dataflows.append({
            "id":          df_id,
            "agencyID":    agency,
            "version":     version,
            "title":       title,
            "description": description,
            "dsd_agency":  dsd_agency,
            "dsd_id":      dsd_id,
            "dsd_version": dsd_version,
        })

    log.info(f"Found {len(dataflows)} dataflows", )
    return dataflows


# ---------------------------------------------------------------------------
# Step 2 - Metadata / DSD
# ---------------------------------------------------------------------------
def fetch_dsd(dsd_agency: str, dsd_id: str,
              dsd_version: str = "latest") -> ET.Element | None:
    # The id may contain characters like '@' that must be percent-encoded
    # in the URL path segment.
    safe_agency  = quote(dsd_agency,  safe="")
    safe_id      = quote(dsd_id,      safe="")
    safe_version = quote(dsd_version, safe="")
    url = f"{BASE_URL}/datastructure/{safe_agency}/{safe_id}/{safe_version}"
    log.debug("Fetching DSD: %s", url)
    resp = http_get(url, params={"references": "all"})
    return ET.fromstring(resp.content) if resp else None


def parse_dimensions(dsd_root: ET.Element) -> dict:
    # concept id -> English description
    concept_desc: dict[str, str] = {}
    for concept in dsd_root.iter(f"{{{NS_STR}}}Concept"):
        cid  = concept.get("id", "")
        name = text_en(concept, f"{{{NS_COM}}}Name")
        if cid and name:
            concept_desc[cid] = name

    # codelist id -> {code -> label}
    codelists: dict[str, dict[str, str]] = {}
    for cl in dsd_root.iter(f"{{{NS_STR}}}Codelist"):
        cl_id = cl.get("id", "")
        codes: dict[str, str] = {}
        for code in cl.iter(f"{{{NS_STR}}}Code"):
            code_val = code.get("id", "")
            label    = text_en(code, f"{{{NS_COM}}}Name", fallback=code_val)
            codes[code_val] = label
        codelists[cl_id] = codes

    # walk DimensionList
    dimensions: dict = {}
    dim_list = dsd_root.find(f".//{{{NS_STR}}}DimensionList")
    if dim_list is None:
        return dimensions

    for dim in dim_list:
        tag = dim.tag.split("}")[-1]
        if tag not in ("Dimension", "TimeDimension"):
            continue

        dim_id = dim.get("id", "")

        # first <com:Ref> inside ConceptIdentity gives us the concept id
        concept_ref = dim.find(f".//{{{NS_COM}}}Ref") or dim.find(".//Ref")
        concept_id  = concept_ref.get("id", dim_id) if concept_ref is not None else dim_id
        desc = concept_desc.get(concept_id) or concept_desc.get(dim_id, "")

        # codelist for allowed values
        enum_ref = (
            dim.find(f".//{{{NS_STR}}}Enumeration/{{{NS_COM}}}Ref")
            or dim.find(f".//{{{NS_STR}}}Enumeration/Ref")
        )
        dim_values: dict | None = None
        if enum_ref is not None:
            cl_id = enum_ref.get("id", "")
            dim_values = codelists.get(cl_id)

        dimensions[dim_id] = {
            "dimension-description": desc,
            "dimension-values":      dim_values,
        }

    return dimensions


# ---------------------------------------------------------------------------
# Step 3 - Data -> Parquet
# ---------------------------------------------------------------------------
def download_data(agency: str, df_id: str) -> pd.DataFrame | None:
    safe_agency = quote(agency, safe="")
    safe_id     = quote(df_id,  safe="")
    for url in [
        f"{BASE_URL}/data/{safe_agency},{safe_id}/all?format=csvfile",
        f"{BASE_URL}/data/{safe_id}/all?format=csvfile",
    ]:
        resp = http_get(url, accept="text/csv,application/csv,*/*")
        if resp is None:
            continue
        try:
            df = pd.read_csv(StringIO(resp.text), low_memory=False)
            if not df.empty:
                return df
        except Exception as exc:
            log.warning("CSV parse error for %s: %s", df_id, exc)
    return None


def dimension_description(name, data, max_dim_items):
    if not data["dimension-values"]:
        values_str = ""
    else:
        values = sorted(data["dimension-values"].values())
        if len(values) <= max_dim_items:
            values_enum = ", ".join([f'"{v}"' for v in values])
        else:
            index = max_dim_items // 2
            start = ", ".join([f'"{v}"' for v in values[:index]])
            index *= -1
            end = ", ".join([f'"{v}"' for v in values[index:]])
            values_enum = f"{start}, ..., {end}"
        values_str = f" Possible values: {values_enum}."
    return f"{name}: {data['dimension-description']}.{values_str}"


def metadata_to_description(metadata, max_dim_items):
    description_buffer = [
        metadata['description'],
        "Dimensions:",
    ]
    for name, dim in sorted( metadata['dimensions'].items()):
        dim_description = dimension_description(name, dim, max_dim_items)
        description_buffer.append(f"\t- {dim_description}")
    description = '\n'.join(description_buffer)

    return standard_dataset_decription(
        f"{metadata['namespace']}.{metadata['id']}",
        metadata['url'],
        f"OECD Dataset - {metadata['title']}",
        description)


def oecd_explorer_url(dataset_id: str, agency_id: str, version: str) -> str:
    """
    Build the OECD Data Explorer URL for a given dataset.

    Args:
        dataset_id: e.g. "DSD_REV_OECD@DF_REVGBR"
        agency_id:  e.g. "OECD.CTP.TPS"
        version:    e.g. "2.0"

    Returns:
        Full URL to the dataset's page on data-explorer.oecd.org
    """
    base = "https://data-explorer.oecd.org/vis"

    # Bracket-notation params must be manually encoded since urlencode
    # would percent-encode the brackets themselves, which the site doesn't expect.
    params = (
        "lc=en"
        "&df[ds]=dsDisseminateFinalDMZ"
        f"&df[id]={quote(dataset_id, safe='')}"
        f"&df[ag]={quote(agency_id, safe='')}"
        f"&df[vs]={quote(version, safe='')}"
        "&dq=.......A"          # default query: all dims wildcarded, annual frequency
        "&lom=LASTNPERIODS"
        "&lo=10"
        "&to[TIME_PERIOD]=false"
    )

    return f"{base}?{params}"


# ---------------------------------------------------------------------------
# Per-dataset orchestration
# ---------------------------------------------------------------------------
def process_dataset(flow: dict, save_csv=False) -> None:
    original_id    = flow["id"]
    agency   = flow["agencyID"]
    # Sanitise the dataset id for use as a directory name (@, / etc.)
    df_id = normalize_dataset_id(original_id)

    out_dir  = OECD_DATASETS / df_id
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = out_dir / "data.parquet"
    json_path    = out_dir / "dataset.json"

    log.info(f"{flow['title']}: {flow['description']}")
    if json_path.exists():
        log.info("  -> Metadata exists")
        metadata = json.loads(json_path.read_text(encoding="utf-8"))
    else:
        dimensions: dict = {}
        dsd_root = fetch_dsd(flow["dsd_agency"], flow["dsd_id"], flow["dsd_version"])
        if dsd_root is not None:
            dimensions = parse_dimensions(dsd_root)
            log.info("  -> %d dimensions", len(dimensions))
        else:
            log.warning("  -> DSD unavailable for %s", df_id)
        metadata = {
            'original-id': original_id,
            "namespace":   "oecd",
            "id":          df_id,
            "version":     flow["version"],
            "title":       flow["title"],
            "description": flow['description'],
            'url': oecd_explorer_url(original_id, flow["agencyID"], flow["version"]),
            'dimensions': dimensions
        }
        metadata["indexing-description"] = metadata_to_description(metadata, max_dim_items=2)
        metadata["usage-description"] = metadata_to_description(metadata, max_dim_items=20)
        json_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info("  -> Metadata -> %s", json_path)

    # --- data ---
    if parquet_path.exists():
        log.info("  -> Parquet exists")
    else:
        df = download_data(agency, original_id)
        if df is not None:
            keep = [c for c in df.columns if c in metadata['dimensions'] or c == "OBS_VALUE"]
            dropped = sorted(set(df.columns) - set(keep))
            if dropped:
                log.info("  -> Dropping columns: %s", dropped)
            df = df[keep]
            df = df.rename(columns={"OBS_VALUE": "observation"})
            for col, dim in metadata['dimensions'].items():
                mapping = dim.get("dimension-values")
                if mapping and col in df.columns:
                    df[col] = df[col].map(lambda v, m=mapping: m.get(v, v))
            if save_csv:
                df.to_csv(out_dir / "data.csv", index=False)
            df.to_parquet(parquet_path, index=False)
            log.info("  -> Data -> %s  (%d rows x %d cols)",
                     parquet_path, len(df), len(df.columns))
        else:
            log.warning("  -> No data for %s", df_id)


def main() -> None:
    debug_allow_list = None

    OECD_DATASETS.mkdir(parents=True, exist_ok=True)

    dataflows = list_dataflows(debug_allow_list)

    total = len(dataflows)
    for i, flow in enumerate(dataflows, 1):
        log.info(f"[{i}/{total}] {flow['id']}")
        try:
            process_dataset(flow)
        except Exception as exc:
            log.error(f"Unhandled error for {flow['id']}: {exc}")
            if debug_allow_list is not None:
                raise exc

    log.info(f"Done. Processed {total} datasets into {OECD_DATASETS}")


if __name__ == "__main__":
    main()
