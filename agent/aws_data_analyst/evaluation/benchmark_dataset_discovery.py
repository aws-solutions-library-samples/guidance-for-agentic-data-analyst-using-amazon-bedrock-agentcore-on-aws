from rich.console import Console
from rich.table import Table

from aws_data_analyst.datasets_db import EMBEDDERS
from aws_data_analyst.evaluation.dataset_retrieval import evaluate_retrieval


def benchmark_dataset_discovery():
    results = []
    for embedder in EMBEDDERS.keys():
        print(f"Benchmarking: {embedder}")

        # Evaluate the retrieval
        eval = evaluate_retrieval(
            embedder,
            index=True,
            verbose=False)
        
        results.append((embedder, eval['embedding_tp50'], eval['mean_reciprocal_rank'], eval['mean_recall']))

    # Present results in a Table
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Model")
    table.add_column("Latency (ms)")
    table.add_column(" Mean Reciprocal Rank")
    table.add_column(" Mean Recall")
    for model, latency, mrr, mr in results:
        table.add_row(model, f"{latency * 1000:.0f}", f"{mrr:.2f}", f"{mr:.2f}")
    console.print(table)


if __name__ == "__main__":
    benchmark_dataset_discovery()
