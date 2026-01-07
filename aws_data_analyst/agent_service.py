from bedrock_agentcore.runtime import BedrockAgentCoreApp

from aws_data_analyst.bedrock_models import DEFAULT_MODEL_ID
from aws_data_analyst.data_analyst_agent import DataAnalystAgent, DATASET_RETRIEVAL_TOPK
from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.cloud_datasets import CloudDatasetLoader


app = BedrockAgentCoreApp()

datasets_db = DatasetsDB()
datasets_loader = CloudDatasetLoader()


@app.entrypoint
async def invoke(payload):
    user_message = payload['message']
    session_history = payload.get('session_history')
    model_id = payload.get('model_id', DEFAULT_MODEL_ID)
    
    datasets = datasets_db.search_entries(user_message, topK=DATASET_RETRIEVAL_TOPK)
    for dataset in datasets['entries']:
        metadata = datasets_loader.load_metadata(dataset['key'])
        dataset['title'] = metadata['title']
        dataset['description'] = metadata['description']
    yield {
        'msg_type': 'datasets',
        'datasets': datasets
    }

    ons_agent = DataAnalystAgent(model_id=model_id, session_history=session_history)
    stream = ons_agent.stream_async(user_message, datasets)
    async for msg in stream:
        yield msg


if __name__ == "__main__":
    """
    To spin-up the Agent server locally run:
    python -m aws_data_analyst.agent_service
    """
    app.run()
