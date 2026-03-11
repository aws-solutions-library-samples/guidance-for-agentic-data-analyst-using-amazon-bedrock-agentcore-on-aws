import json
import threading
import time
from collections import Counter

import s3fs

from aws_data_analyst.infrastructure import S3_DATASETS_METADATA
from aws_data_analyst.athena_query import athena_query


S3 = s3fs.S3FileSystem()

LIMIT_RESULTS = 10000


class CloudDatasetLoader:
    def __init__(self) -> None:
        self.metadata = {}
        
        # Run a non-blocking pre-loading thread
        loading_thread = threading.Thread(target=self.__load_metadata, daemon=True)
        loading_thread.start()

    def __load_metadata(self):
        for dataset_uri in S3.ls(S3_DATASETS_METADATA):
            dataset_id = dataset_uri.split('/')[-1]
            self.load_metadata(dataset_id)

    def load_metadata(self, dataset_id):
        if dataset_id in self.metadata:
            return self.metadata[dataset_id]

        metadata_uri = f"{S3_DATASETS_METADATA}{dataset_id}/dataset.json"
        try:
            metadata = json.load(S3.open(metadata_uri))
            self.metadata[dataset_id] = metadata
        except Exception as e:
            print(f"[{dataset_id}] Error loading dataset metadata from: {metadata_uri}\n{e}")
            metadata = None
        
        return metadata


class CloudQueryHandler:
    def __init__(self) -> None:
        self.datasets = Counter()
        self.latencies = []

    def metrics(self):
        return {
            'datasets': dict(self.datasets),
            'latencies': self.latencies
        }

    def query_ons_dataset(self, dataset_id, dimension_filters=None):
        self.datasets[dataset_id] += 1
        start = time.time()
        df = athena_query(dataset_id, dimension_filters=dimension_filters, limit=LIMIT_RESULTS)
        query_latency = time.time() - start
        self.latencies.append(query_latency)
        return df
