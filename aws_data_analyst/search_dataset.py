from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.datasets import load_description


def search_dataset(query, embedder_id="nova"):
    db = DatasetsDB(embedder_id=embedder_id)
    results = db.search_entries(query)
    
    for result in results['entries']:
        lines = load_description(result['key']).split('\n')
        print(f"\tDistance: {result['distance']:.3f}: {lines[0]}")
    
    print("Metrics:")
    print(f" - Embedding: {results['metrics']['embedding']:.3f} s")
    print(f" - Search   : {results['metrics']['search']:.3f} s")


if __name__ == "__main__":
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument("--embedder", default="nova")
    parser.add_argument("query")
    args = parser.parse_args()

    search_dataset(args.query, args.embedder)
