import time

import boto3

from aws_data_analyst.embedding_models import EmbeddingPurpose, NovaEmbedder, CohereEmbedder
from aws_data_analyst.infrastructure import get_vectordb_configuration


EMBEDDERS = {
    "nova": NovaEmbedder,
    "cohere": CohereEmbedder
}


class DatasetsDB:
    def __init__(self,
                 configuration=None,
                 region_name=None) -> None:
        if configuration is None:
            configuration = get_vectordb_configuration()
        
        self.vector_bucket = configuration['bucket']
        self.index_name = configuration['index']
        self.embedder = EMBEDDERS[configuration['embedder_id']](configuration['embedding_dimension'], region_name)
        
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
