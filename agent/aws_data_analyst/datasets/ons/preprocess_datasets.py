import re
import json
from collections import Counter
from os import path, listdir

from tqdm import tqdm
import pandas as pd

from aws_data_analyst.datasets.ons import ONS_DATASETS


OBSERVATION_PATTERN = re.compile(r"[vV]4_\d")


def dataset_data(dataset_id):
    dataset_dir = ONS_DATASETS / dataset_id
    metadata_path = dataset_dir / "metadata.json"
    data_path = dataset_dir / "data.csv"
    if not metadata_path.exists() or not data_path.exists():
        return None
    else:
        return {
            'id': dataset_id,
            'data': data_path,
            'metadata': metadata_path, 
        }


def iterate_datasets():
    for dataset_id in sorted(listdir(ONS_DATASETS)):
        data = dataset_data(dataset_id)
        if data is None:
            continue
        yield data


def find_observation(headers):
    if 'Observation' in headers:
        return 'Observation'
    for name in headers:
        if OBSERVATION_PATTERN.match(name):
            return name
    raise ValueError("No observation field found")


def map_dimension_to_headers(dim, headers):
    id = dim['id']
    dim_labels = {id, dim['name']}

    if id == 'yyyy-yy':
        dim_labels.add('yyyy-to-yyyy-yy')
    elif id == 'sic':
        dim_labels.add('sic-unofficial')
    
    if 'label' in dim:
        label = dim['label'].lower()
        dim_labels.add(label)
        dim_labels.add(label + ' code')
        if label == 'standard industrial classification':
            dim_labels.add('unofficialstandardindustrialclassification')

    mapped = set()
    for header in headers:
        if header.lower() in dim_labels:
            mapped.add(header)
    return mapped


def select_field(fields):
    # Heuristics to select the field that should be more human/ai readable
    a, b = list(fields.keys())
    if a.lower().endswith(' code'):
        return b, a
    elif b.lower().endswith(' code'):
        return a, b
    for name in ['Time', 'Geography']:
        if name == a:
            return a, b
        elif name == b:
            return b, a
    
    a_keys = list(map(str, fields[a].keys()))
    b_keys = list(map(str, fields[b].keys()))

    if len(''.join(a_keys)) > len(''.join(b_keys)):
        return a, b
    elif len(''.join(a_keys)) < len(''.join(b_keys)):
        return b, a

    if any([ak[0].isupper() for ak in a_keys]):
        return a, b
    
    if any([bk[0].isupper() for bk in b_keys]):
        return b, a

    return b, a


def get_csv_header(file_path):
    with open(file_path, 'r') as f:
        header = f.readline().strip()
    return header.split(',')


def dimension_description(name, data, max_dim_items):
    values = sorted(data['values'].keys())
    if len(values) <= max_dim_items:
        values_str = ", ".join([f'"{v}"' for v in values])
    else:
        index = max_dim_items // 2
        start = ", ".join([f'"{v}"' for v in values[:index]])
        index *= -1
        end = ", ".join([f'"{v}"' for v in values[index:]])
        values_str = f"{start}, ..., {end}"
    return f"{name}: {data['label']}. Possible values: {values_str}."


def metadata_to_description(data, max_dim_items=20):
    obs = data['observation']

    buffer = [
        f"UK Office for National Statistics Dataset ID {data['id']}: {data['title']}",
        data['description'],
        "Fields:",
        f"\t- observation: Unit of Measure \"{obs['unit']}\", Max {obs['max']}, Min {obs['min']}"
    ]
    
    for name, dim in sorted(data["dimensions"].items()):
        description = dimension_description(name, dim, max_dim_items)
        buffer.append(f"\t- {description}")

    return '\n'.join(buffer)


def preprocess_dataset(dataset):
    dataset_dir = path.dirname(dataset['data'])
    
    # Load Metadata
    metadata = json.load(open(dataset['metadata']))

    # Generated files
    dataset_dir = path.dirname(dataset['data'])
    dataset_path = path.join(dataset_dir, "dataset.json")
    parquet_path = path.join(dataset_dir, "data.parquet")
    if all(map(path.exists, [dataset_path, parquet_path])):
        dataset_information = json.load(open(dataset_path))
        if 'version' in dataset_information and dataset_information['version'] == metadata['latest_version_metadata']['version']:
            return

    headers = set(get_csv_header(dataset['data']))
    observation = find_observation(headers)
    headers.remove(observation)

    # Load Data dropping rows with NaN in the Observation field
    # We do this chunk by chunk, to limit the memory requirement
    chunk_size = 100000  # Adjust based on your memory
    dtype={column: "string" for column in headers}
    chunks = []
    for chunk in pd.read_csv(dataset['data'], dtype=dtype, chunksize=chunk_size, on_bad_lines='skip'):
        chunk = chunk.dropna(subset=[observation]).copy()     
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)

    df = df.rename(columns={observation: 'observation'})
    obs_min, obs_max =  df['observation'].agg(['min', 'max'])
    columns = {'observation'}

    # Dimensions
    dimensions_values = {}
    dimensions_data = {}
    for dim in metadata["latest_version_metadata"]["dimensions"]:
        name = dim['name']
        dimensions_data[name] = dim
        mapped = map_dimension_to_headers(dim , headers)
        assert len(mapped) == 2
        
        # For each column count the frequency of its Enum
        dimensions_values[name] = {}
        for field in mapped:
            counts = df[field].value_counts()
            dimensions_values[name][field] = Counter(counts.to_dict())

    # For each dimension, select one of the two columns
    dimensions = {}
    for name, values in dimensions_values.items():
        dimension_data = dimensions_data[name]
        field, code_field = select_field(values)
        columns.add(field)
        dimensions[name] = {
            'id': dimension_data['id'],
            'label': dimension_data.get('label', name),
            'field': field,
            'code_field': code_field,
            'values': values[field]
        }

    information = {
        'id': dataset['id'],
        'title': metadata['title'],
        'description': metadata['description'],
        'observation': {
                'field': observation,
                'min': obs_min,
                'max': obs_max,
                'unit': metadata.get('unit_of_measure', 'Number')
            },
        'dimensions': dimensions,
    }
    
    # Save the dataset information
    json.dump({
        'namespace': 'ons',
        'id': dataset['id'],
        'title': metadata['title'],
        'version': metadata['latest_version_metadata']['version'],
        'indexing-description': metadata_to_description(information, max_dim_items=2),
        'usage-description': metadata_to_description(information, max_dim_items=20)
    }, open(dataset_path, 'w'), indent=4)
    
    # Keep only the selected columns 
    df = df[list(columns)]

    # Rename the columns to the dimension names
    column_map = {}
    for dim_name, data in dimensions.items():
        column_map[data['field']] = dim_name
    df = df.rename(columns=column_map)

    # Save to parquet format
    df.to_parquet(parquet_path)


def preprocess_datasets():
    for dataset in tqdm(list(iterate_datasets())):
        try:
            preprocess_dataset(dataset)
        except Exception as e:
            print(f"[{dataset['id']}] Error: {e}")


if __name__ == "__main__":
    preprocess_datasets()
