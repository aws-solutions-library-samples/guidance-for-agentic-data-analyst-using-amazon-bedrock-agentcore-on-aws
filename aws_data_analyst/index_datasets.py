import statistics

from tqdm import tqdm

from aws_data_analyst.datasets import iterate_datasets, load_description
from aws_data_analyst.datasets_db import DatasetsDB


def index_datasets(embedder):
    db = DatasetsDB(embedder_id=embedder)
    for dataset in tqdm(list(iterate_datasets())):
        embedding_times = []
        description = load_description(dataset['id'])
        metrics = db.add_entry(dataset['id'], description)
        embedding_times.append(metrics['embedding'])
    embedding_tp50 = statistics.median(embedding_times)
    print(f"Embedding TP50: {embedding_tp50:.3f} s")
    return embedding_tp50


if __name__ == "__main__":
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument("--embedder", default="nova")
    args = parser.parse_args()

    index_datasets(args.embedder)
