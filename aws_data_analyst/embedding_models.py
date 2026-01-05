from abc import abstractmethod
from enum import Enum
import json

import boto3


class EmbeddingPurpose(Enum):
    INDEX = 1
    RETRIEVE = 2


class BedrockEmbedder:
    def __init__(self, model_id, embedding_dimension, region_name=None) -> None:
        self.model_id = model_id
        self.embedding_dimension = embedding_dimension
        self.bedrock_runtime = boto3.client("bedrock-runtime", region_name=region_name)

    def invoke_model(self, request):
        response = self.bedrock_runtime.invoke_model(
            modelId=self.model_id,
            body=json.dumps(request),
            contentType="application/json",
        )
        return json.loads(response["body"].read())

    @abstractmethod
    def single_embedding(self, text: str, embeddingPurpose: EmbeddingPurpose):
        pass


class NovaEmbedder(BedrockEmbedder):
    # See: https://aws.amazon.com/blogs/aws/amazon-nova-multimodal-embeddings-now-available-in-amazon-bedrock/
    PURPOSE_MAP = {
        EmbeddingPurpose.INDEX: "GENERIC_INDEX",
        EmbeddingPurpose.RETRIEVE: "GENERIC_RETRIEVAL",
    }

    def __init__(self, embedding_dimension, region_name=None) -> None:
        super().__init__("amazon.nova-2-multimodal-embeddings-v1:0", embedding_dimension, region_name)
    
    def single_embedding(self, text: str, embeddingPurpose: EmbeddingPurpose):
        response = self.invoke_model({
            "taskType": "SINGLE_EMBEDDING",
            "singleEmbeddingParams": {
                "embeddingPurpose": NovaEmbedder.PURPOSE_MAP[embeddingPurpose],
                "embeddingDimension": self.embedding_dimension,
                "text": {"truncationMode": "END", "value": text},
            },
        })
        return response["embeddings"][0]["embedding"]


class CohereEmbedder(BedrockEmbedder):
    # See: https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed-v4.html
    PURPOSE_MAP = {
        EmbeddingPurpose.INDEX: "search_document",
        EmbeddingPurpose.RETRIEVE: "search_query",
    }
    def __init__(self, embedding_dimension, region_name=None) -> None:
        super().__init__("cohere.embed-v4:0", embedding_dimension, region_name)
    
    def single_embedding(self, text: str, embeddingPurpose: EmbeddingPurpose):
        response = self.invoke_model({
            'input_type': CohereEmbedder.PURPOSE_MAP[embeddingPurpose],
            'texts': [text],
            'embedding_types': ['float'],
            "output_dimension": self.embedding_dimension
        })
        return response["embeddings"]["float"][0]
