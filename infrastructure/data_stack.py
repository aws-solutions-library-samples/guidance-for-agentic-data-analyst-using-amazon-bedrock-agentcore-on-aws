from aws_cdk import Stack
from constructs import Construct

# See: https://github.com/bimnett/cdk-s3-vectors/blob/main/examples/python.py
import cdk_s3_vectors as s3_vectors


EMBEDDING_DIMENSION = 1024


class DataStack(Stack):
    def __init__(
        self, 
        scope: Construct, 
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dataset_embeddings = s3_vectors.Bucket(
            self, "DatasetEmbeddings",
            vector_bucket_name="dataset-embeddings",
        )
        dataset_embeddings_index = s3_vectors.Index(
            self, "DatasetEmbeddingsIndex",
            vector_bucket_name=self.dataset_embeddings.vector_bucket_name,
            index_name="dataset-embeddings-index",
            data_type="float32",
            dimension=EMBEDDING_DIMENSION,
            distance_metric="cosine",
        )
        dataset_embeddings_index.node.add_dependency(self.dataset_embeddings)
