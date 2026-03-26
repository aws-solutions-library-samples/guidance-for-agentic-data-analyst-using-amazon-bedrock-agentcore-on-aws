import re
from pathlib import Path


DATASETS_DIR =  Path.home() / 'workspace' / 'data'


DATASETS = [
    'ons',
    'oecd',
]


def normalize_dataset_id(dataset_id: str) -> str:
    tmp_str = dataset_id.strip().lower()
    tmp_str = re.sub(r'[^a-z0-9]', '_', tmp_str)
    return re.sub(r'_+', '_', tmp_str)


def standard_dataset_decription(dataset_id, url, title, usage_description):
    return f"# ID [{dataset_id}]({url}): {title}\n{usage_description}"


def iterate_datasets(target_namespace=None):
    for namespace in DATASETS:
        if target_namespace is not None and namespace != target_namespace:
            continue

        namespace_dir = DATASETS_DIR / namespace
        for dataset_dir in namespace_dir.iterdir():
            if not dataset_dir.is_dir():
                continue

            data_file = dataset_dir / "data.parquet"
            metadata_file = dataset_dir / "dataset.json"
            if data_file.exists() and metadata_file.exists():
                yield {
                    "namespace": namespace,
                    "id": dataset_dir.name,
                    "data_file": data_file,
                    "metadata_file": metadata_file
                }