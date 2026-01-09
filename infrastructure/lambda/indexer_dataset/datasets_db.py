import time

import boto3

from embedding_models import EmbeddingPurpose, NovaEmbedder, CohereEmbedder


VECTOR_BUCKET = "dataset-embeddings"
INDEX_NAME = "dataset-embeddings-index"
EMBEDDING_DIMENSION = 1024

EMBEDDERS = {
    "nova": NovaEmbedder,
    "cohere": CohereEmbedder
}


class DatasetsDB:
    def __init__(self,
                 vector_bucket=VECTOR_BUCKET,
                 index_name=INDEX_NAME,
                 embedder_id="nova",
                 embedding_dimension=EMBEDDING_DIMENSION,
                 region_name=None) -> None:
        self.vector_bucket = vector_bucket
        self.index_name = index_name
        
        self.embedder = EMBEDDERS[embedder_id](embedding_dimension, region_name)
        self.s3vectors = boto3.client("s3vectors")
    
    def add_entry(self, name: str, text: str, metadata=None):
        metrics = {}

        # Embed entry text
        start = time.time()
        embedding = self.embedder.single_embedding(text, EmbeddingPurpose.INDEX)
        metrics['embedding'] = time.time() - start

        # Add vector to the index
        start = time.time()
        self.s3vectors.put_vectors(
            vectorBucketName=self.vector_bucket,
            indexName=self.index_name,
            vectors=[{
                "key": name,
                "data": {"float32": embedding},
                "metadata": metadata or {}
            }]
        )
        metrics['index'] = time.time() - start

        return metrics

    def search_entries(self, query: str, topK: int = 3):
        metrics = {}

        start = time.time()
        embedding = self.embedder.single_embedding(query, EmbeddingPurpose.RETRIEVE)
        metrics['embedding'] = time.time() - start

        start = time.time()
        response = self.s3vectors.query_vectors(
            vectorBucketName=self.vector_bucket,
            indexName=self.index_name,
            queryVector={"float32": embedding},
            topK=topK,
            returnDistance=True,
            returnMetadata=True
        )
        metrics['search'] = time.time() - start
        
        return {
            'entries': response['vectors'],
            'metrics': metrics
        }
