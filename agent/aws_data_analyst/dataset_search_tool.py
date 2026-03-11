from strands import tool


DATASET_RETRIEVAL_TOPK = 3


class DatasetSearch:
    def __init__(self, datasets_db, datasets_loader) -> None:
        self.datasets_db = datasets_db
        self.datasets_loader = datasets_loader

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
                metadata = self.datasets_loader.load_metadata(dataset['key'])
                result.append(metadata['usage-description'])
            return '\n\n'.join(result)
        
        return search_datasets


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("query")
    args = parser.parse_args()

    from aws_data_analyst.datasets_db import DatasetsDB
    from aws_data_analyst.cloud_datasets import CloudDatasetLoader
    datasets_db = DatasetsDB()
    datasets_loader = CloudDatasetLoader()
    
    search_tool = DatasetSearch(datasets_db, datasets_loader).get_tool()

    print(search_tool(args.query))
 