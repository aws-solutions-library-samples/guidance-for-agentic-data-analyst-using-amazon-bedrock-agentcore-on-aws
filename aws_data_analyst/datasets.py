import json
from os import listdir
from collections import Counter
from abc import ABC, abstractmethod

import pandas as pd

from aws_data_analyst import DATASETS_DIR
import time



def dataset_data(dataset_id):
    dataset_dir = DATASETS_DIR / dataset_id
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
    for dataset_id in sorted(listdir(DATASETS_DIR)):
        data = dataset_data(dataset_id)
        if data is None:
            continue
        yield data


def load_dataset_metadata(dataset_id):
    return json.load(open(DATASETS_DIR / dataset_id / "dataset.json"))


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


def metadata_to_description(data, max_dim_items=20, skip_examples=False):
    obs = data['observation']

    buffer = [
        f"[ID: {data['id']}] {data['title']}",
        data['description'],
        "Fields:",
        f"\t- observation: Unit of Measure \"{obs['unit']}\", Max {obs['max']}, Min {obs['min']}"
    ]
    
    for name, dim in sorted(data["dimensions"].items()):
        description = dimension_description(name, dim, max_dim_items)
        buffer.append(f"\t- {description}")

    if not skip_examples and 'example_questions' in data and data['example_questions']:
        buffer.append("Example Questions:")
        for question in data['example_questions']:
            buffer.append(f"\t- {question}")
    
    return '\n'.join(buffer)


def load_description(data_id, max_dim_items=20, skip_examples=False):
    metadata = load_dataset_metadata(data_id)
    return metadata_to_description(metadata, max_dim_items, skip_examples)


class QueryHandler(ABC):
    def __init__(self) -> None:
        self.datasets = Counter()
        self.latencies = []

    def metrics(self):
        return {
            'datasets': dict(self.datasets),
            'latencies': self.latencies
        }

    @abstractmethod
    def query_ons_dataset(self, dataset_id, dimension_filters=None):
        pass


class LocalQueryHandler(QueryHandler):
    def __init__(self) -> None:
        self.datasets = Counter()
        self.latencies = []
    
    def query_ons_dataset(self, dataset_id, dimensions):
        self.datasets[dataset_id] += 1
        data_path = DATASETS_DIR / dataset_id / "data.parquet"
        if not data_path.exists():
            return None
        else:
            start = time.time()
            df = pd.read_parquet(data_path)
            for dim, value in dimensions.items():
                df = df[df[dim] == value]
            latency = time.time() - start
            self.latencies.append(latency)
            return df


class LocalDatasetLoader:
    def load_metadata(self, dataset_id):
        return load_dataset_metadata(dataset_id)
