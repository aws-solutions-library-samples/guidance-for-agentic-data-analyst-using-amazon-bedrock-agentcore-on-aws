from strands import tool
from strands.models import BedrockModel

from json_repair import repair_json

from strands_code_agent.code_agent import CodeAgent
from strands_code_agent.toolkits import Toolkit, VISUALIZATION_TOOLKIT, DATA_ANALYSIS_TOOLKIT
from strands_code_agent.utils import image_to_base64

from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.dataset_search_tool import DatasetSearch
from aws_data_analyst.bedrock_models import MODELS, DEFAULT_MODEL_ID, DEFAULT_TEMPERATURE
from aws_data_analyst.cloud_datasets import CloudDatasetLoader


DATASET_RETRIEVAL_TOPK = 3

SYSTEM_PROMPT = """You are an expert data-analyst answering user queries based on the available datasets.
You can use the python_repl tool to execute python code fetching data, and performing additional data processing with the pandas library.

Your answer has to be grounded on a dataset.
If you cannot find a suitable dataset to ground your question, set "supported_by_data" to `false` and explain in the answer text about the lack of a suitable dataset.

If you want to show an image to the user invoke the `visualize_image` tool.

You can search additional datasets with the `search_datasets` tool.
For example, a more generic dataset can contain specific information about the user query once you apply a filter on its dimensions.

You should return a JSON object with the following format:
{
    "answer": "answer text",
    "supported_by_data": true / false
}

Pay attention to these guidelines:
- For the latest information about current population figures (how many people live in a region, current population counts, etc.), always retrieve the `mid-year-pop-est` dataset, instead of the old census data.
- Select the larger group available for fitering the observation dimensions (i.e. for the working people select "Economically Active" instead of "In Employment").
- The time columns can be in many different formats, and they are often in string format not ready to be sorted to be able to find the latest data.
- When asked for a single value (i.e. the latest figures) you should provide the `Not Seasonally Adjusted` value, if instead you are asked for a trend through time it might make sense to provide the `Seasonally Adjusted` values.
- Provide all the available data precision, without rounding.
- For the latest information about employment, always retrieve also the `labour-market` dataset. 
- For the number of deaths report primarily the Occurrence dimension, and secondarily the Registrations dimension
- If asked for a given age, provide information from the closest age-band containing this age in the dataset.
"""

QUERY_HANDLER_TOOLKIT = Toolkit(
    libraries=['aws_data_analyst.cloud_datasets', 'scipy'],
    initialization_code="""
from aws_data_analyst.cloud_datasets import CloudQueryHandler
query_handler = CloudQueryHandler()
""",
    usage_instructions="""
To load a dataset by Dataset-ID into a pandas dataframe, use the following code:
```python
df = query_handler.query_dataset('Dataset-ID', {'dimension-name-1': 'value-1', ... , 'dimension-name-n': 'value-n'})
```
""")


@tool
def visualize_image(image_path: str):
    """
    Load the given PNG image at `image_path` and visualize it on the user client application.

    Args:
        image_path: the path of the PNG image to be visualized.
    """
    pass


class DataAnalystAgent:
    def __init__(self,
                 model_id=DEFAULT_MODEL_ID,
                 temperature=DEFAULT_TEMPERATURE,
                 history=None):
        self.model_id = model_id
        self.temperature = temperature
        self.cost = MODELS[model_id]['cost']

        self.datasets_loader = CloudDatasetLoader()
        self.datasets_db = DatasetsDB()
        self.tool_uses = {}

        messages = []
        if history is not None:
            for role, msg in history:
                messages.append({
                    'role': role,
                    'content': [{'text': msg}]
                })

        self.agent = CodeAgent(
            system_prompt=SYSTEM_PROMPT,
            tools=[
                visualize_image,
                DatasetSearch(self.datasets_db, self.datasets_loader).get_tool()
            ],
            toolkits=[
                DATA_ANALYSIS_TOOLKIT, VISUALIZATION_TOOLKIT,
                QUERY_HANDLER_TOOLKIT,
            ],
            model=BedrockModel(
                model_id=model_id,
                temperature=DEFAULT_TEMPERATURE,
            ),
            callback_handler=None,
            messages=messages,
        )

    def prepare_prompt(self, query, datasets=None):
        if datasets is None:
            datasets = self.datasets_db.search_entries(query, topK=DATASET_RETRIEVAL_TOPK)

        prompt_buffer = [f"User Query: {query}"]
        if datasets:
            prompt_buffer.append("\nThe following are datasets whose descriptions are semantically similar to this query:")
            for dataset in datasets['entries']:
                metadata = self.datasets_loader.load_metadata(dataset['key'])
                prompt_buffer.append(metadata['usage-description'])
        return '\n'.join(prompt_buffer)

    def __post_process_result(self, response):
        response_data = repair_json(str(response), return_objects=True)
        if type(response_data) is str:
            response_data = {"answer": response_data}

        metrics = response.metrics.get_summary()
        response_data['metrics'] = {
            'agent': {
                'total_cycles': metrics['total_cycles'],
                'total_duration': metrics['total_duration'],
                'on_demand_cost': metrics['accumulated_usage']['inputTokens'] * self.cost['on_demand']['input'] + metrics['accumulated_usage']['outputTokens'] * self.cost['on_demand']['output']
            }
        }
        executor_state = self.agent.python_repl.executor.state
        if 'query_handler' in executor_state:
            query_metrics = executor_state['query_handler'].metrics()
            datasets = []
            for dataset, count in query_metrics['datasets'].items():
                metadata = self.datasets_loader.load_metadata(dataset)
                datasets.append({
                    'key': dataset,
                    'title': metadata['title'],
                    'url': metadata['url'],
                    'count': count
                })
            query_metrics['datasets'] = datasets
            response_data['metrics']['query'] = query_metrics
        return response_data

    async def stream_async(self, message, datasets=None, cached=True):
        prompt = self.prepare_prompt(message, datasets=datasets)

        self.agent.python_repl.clear_state()
        agent_stream = self.agent.stream_async(prompt)
        async for event in agent_stream:
            if 'result' in event:
                yield {
                    'msg_type': 'result',
                    'result': self.__post_process_result(event['result'])
                }
            elif 'message' in event:
                for content in event['message']['content']:
                    if 'text' in content:
                        yield {"msg_type": "text", "text": content['text']}

                    elif 'toolUse' in content:
                        toolUse = content['toolUse']
                        self.tool_uses[toolUse['toolUseId']] = toolUse['name']
                        toolUse["msg_type"] = "toolUse"

                        if toolUse['name'] == 'visualize_image':
                            toolUse['image'] = image_to_base64(toolUse['input']['image_path'])

                        yield toolUse

                    elif 'toolResult' in content:
                        toolResult = content['toolResult']
                        toolResult['name'] = self.tool_uses[toolResult['toolUseId']]

                        if toolResult['name'] == 'visualize_image':
                            continue

                        toolResult["msg_type"] = "toolResult"
                        yield toolResult
