import json
import statistics 

from tqdm import tqdm
from irmetrics.topk import rr, recall

from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.data_analyst_agent import DATASET_RETRIEVAL_TOPK
from aws_data_analyst.datasets import load_dataset_metadata
from aws_data_analyst.evaluation.index_datasets import index_datasets
from aws_data_analyst.evaluation import QUERIES_PATH
from aws_data_analyst.infrastructure import get_vectordb_configuration


def print_dataset(dataset):
    metadata = load_dataset_metadata(dataset)
    description = metadata['description'].split('\n')[0]
    print(f" - [{dataset}] {metadata['title']}: {description}")


def evaluate_retrieval(embedder, index, verbose=True):
    results = {}
    if index:
        results['embedding_tp50'] = index_datasets(embedder)

    conf = get_vectordb_configuration(dev=True)
    conf['embedder_id'] = embedder
    db = DatasetsDB(conf)
    rr_scores, recall_scores, search_latency = [], [], []
    for test in tqdm(json.load(QUERIES_PATH.open())):
        result = db.search_entries(test["query"], topK=DATASET_RETRIEVAL_TOPK)
        datasets = [entry["key"] for entry in result['entries']]
        rr_score = rr(test["datasets"], datasets)
        recall_score = recall(test["datasets"], datasets)
        
        missing = set(test["datasets"]) - set(datasets)
        if verbose and missing:
            print(f"\nQuery: {test['query']}")
            print("Retrieved Datasets:")
            for dataset in datasets:
                print_dataset(dataset)
            print("Missing Datasets:")
            for dataset in missing:
                print_dataset(dataset)
        
        rr_scores.append(rr_score)
        recall_scores.append(recall_score)
        search_latency.append(result['metrics']['search'])

    results['mean_reciprocal_rank'] = statistics.mean(rr_scores)
    results['mean_recall'] = statistics.mean(recall_scores)
    results['median_search_latency'] = statistics.median(search_latency)

    if verbose:
        print(f"Mean Reciprocal Rank: {results['mean_reciprocal_rank']:.2f}")
        print(f"Mean Recall: {results['mean_recall']:.2f}")
        print(f"Median Search Latency: {results['median_search_latency'] * 1000:.0f} ms")
    
    return results


if __name__ == "__main__":
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument("--embedder", default="nova")
    parser.add_argument("--index", action="store_true")
    args = parser.parse_args()
    
    evaluate_retrieval(args.embedder, args.index)
