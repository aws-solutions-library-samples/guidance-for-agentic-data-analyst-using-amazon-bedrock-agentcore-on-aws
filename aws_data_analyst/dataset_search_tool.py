from strands import tool

from aws_data_analyst.datasets import load_dataset_metadata, metadata_to_description


DATASET_RETRIEVAL_TOPK = 3


class DatasetSearch:
    def __init__(self, datasets_db) -> None:
        self.datasets_db = datasets_db

    def get_tool(self):
        @tool
        def search_datasets(query):
            """
            Discover ONS datasets relevant to a given query.

            Args:
                query: The query for the ONS dataset
            """
            datasets = self.datasets_db.search_entries(query, topK=DATASET_RETRIEVAL_TOPK)
            
            result = ["ONS Datasets:"]
            for dataset in datasets['entries']:
                metadata = load_dataset_metadata(dataset['key'])
                result.append(metadata_to_description(metadata, max_dim_items=400))
            return '\n'.join(result)
        
        return search_datasets
