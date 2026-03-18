import base64
import tempfile
import shutil

from strands import Agent
from strands.models import BedrockModel
from strands import tool

from json_repair import repair_json
from jinja2 import Template

from aws_data_analyst.python_environment import PythonInterpreter
from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.dataset_search_tool import DatasetSearch
from aws_data_analyst.bedrock_models import MODELS, DEFAULT_MODEL_ID, DEFAULT_TEMPERATURE
from aws_data_analyst.cloud_datasets import CloudDatasetLoader


DATASET_RETRIEVAL_TOPK = 3

SYSTEM_PROMPT_TEMPLATE = Template("""You are an expert data-analyst answering user queries based on the available datasets.
You can use the python_repl tool to execute python code fetching data, and performing additional data processing with the pandas library.

Your answer has to be grounded on a dataset.
If you cannot find a suitable dataset to ground your question, set "supported_by_data" to `false` and explain in the answer text about the lack of a suitable dataset.

Do not try to show any matplotlib/seaborn images: the python_repl tool executes the code in a sub-process without a GUI.
If you need to generate a file use this temporary directory: {{TEMP_DIR}}
The user has no access to this directory.
If you want to show an image to the user invoke the `visualize_image` tool.

For each user query you will be provided with a set of datasets whose description is semantically similar to the given query.
You can search additional datasets with the `search_datasets` tool.
For example, a more generic dataset can contain specific information about the user query once you apply a filter on its dimensions.

You should return a JSON object with the following format:
{
    "answer": "answer text",
    "supported_by_data": true / false
}

The python environment of the python_repl tool was initialised with the following code (you do not need to rewrite this code):
{{CODE_PREAMBLE}}

To load a dataset by Dataset-ID into a pandas dataframe, use the following code:
```python
df = query_handler.query_dataset('Dataset-ID', {'dimension-name-1': 'value-1', ... , 'dimension-name-n': 'value-n'})
```

Pay attention to these guidelines:
- For the latest information about current population figures (how many people live in a region, current population counts, etc.), always retrieve the `mid-year-pop-est` dataset, instead of the old census data.
- Select the larger group available for fitering the observation dimensions (i.e. for the working people select "Economically Active" instead of "In Employment").
- The time columns can be in many different formats, and they are often in string format not ready to be sorted to be able to find the latest data.
- When asked for a single value (i.e. the latest figures) you should provide the `Not Seasonally Adjusted` value, if instead you are asked for a trend through time it might make sense to provide the `Seasonally Adjusted` values.
- Provide all the available data precision, without rounding.
- For the latest information about employment, always retrieve also the `labour-market` dataset. 
- For the number of deaths report primarily the Occurrence dimension, and secondarily the Registrations dimension
- If asked for a given age, provide information from the closest age-band containing this age in the dataset.
""")

CODE_PREAMBLE = Template("""
# Data and Time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Visualization Libraries
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

# Dataset Query
from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()
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
        code_preamble = CODE_PREAMBLE.render()

        self.tmp_dir = tempfile.mkdtemp(prefix='data_analyst_', dir='/tmp')
        self.python_repl = PythonInterpreter(code_preamble)
        self.datasets_db = DatasetsDB()
        self.tool_uses = {}

        messages = []
        if history is not None:
            for role, msg in history:
                messages.append({
                    'role': role,
                    'content': [{'text': msg}]
                })

        self.agent = Agent(
            model = BedrockModel(
                model_id=model_id,
                temperature=DEFAULT_TEMPERATURE,
            ),
            tools=[
                self.python_repl.get_tool(),
                visualize_image,
                DatasetSearch(self.datasets_db, self.datasets_loader).get_tool()
            ],
            callback_handler=None,
            system_prompt=SYSTEM_PROMPT_TEMPLATE.render(CODE_PREAMBLE=code_preamble, TMP_DIR=self.tmp_dir),
            messages=messages)
        
        # Render a generic system prompt prototype to be used as part of the cache-key
        self.prompt_template_prototype = SYSTEM_PROMPT_TEMPLATE.render(TMP_DIR="/tmp", CODE_PREAMBLE=code_preamble)

    def __del__(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except:
            pass

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

    def on_demand_cost(self, metrics):
        return metrics['accumulated_usage']['inputTokens'] * self.cost['on_demand']['input'] + metrics['accumulated_usage']['outputTokens'] * self.cost['on_demand']['output']

    def __post_process_result(self, response):
        response_data = repair_json(str(response), return_objects=True)
        if type(response_data) is str:
            response_data = {"answer": response_data}
        
        metrics = response.metrics.get_summary()
        response_data['metrics'] = {
            'agent': {
                'total_cycles': metrics['total_cycles'],
                'total_duration': metrics['total_duration'],
                'on_demand_cost': self.on_demand_cost(metrics)
            }
        }
        if 'query_handler' in self.python_repl.state:
            query_metrics = self.python_repl.state['query_handler'].metrics()
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
        
        self.python_repl.clear_state()
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
                            with open(toolUse['input']['image_path'], 'rb') as f:
                                toolUse['image'] = base64.b64encode(f.read()).decode('utf-8')
                        
                        yield toolUse
                    
                    elif 'toolResult' in content:
                        toolResult = content['toolResult']
                        toolResult['name'] = self.tool_uses[toolResult['toolUseId']]
                        
                        if toolResult['name'] == 'visualize_image':
                            continue

                        toolResult["msg_type"] = "toolResult"
                        yield toolResult
