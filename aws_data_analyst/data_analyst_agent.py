import base64
import tempfile
import shutil
from os import path

from strands import Agent
from strands.models import BedrockModel

from json_repair import repair_json
from diskcache import Cache
from jinja2 import Template

from aws_data_analyst.python_environment import PythonInterpreter
from aws_data_analyst.datasets_db import DatasetsDB
from aws_data_analyst.datasets import load_dataset_metadata, metadata_to_description
from aws_data_analyst.dataset_search_tool import DatasetSearch
from aws_data_analyst.bedrock_models import MODELS, DEFAULT_MODEL_ID


CACHE = Cache("/tmp/.cache/ONS_Agent")

TEMPERATURE = 0.2
DATASET_RETRIEVAL_TOPK = 3

SYSTEM_PROMPT_TEMPLATE = Template("""You are an expert data-analyst answering user queries based on the Office for National Statistics (ONS) datasets.
You can use the python_repl tool to execute python code fetching data, and performing additional data processing with the pandas library.

Your answer has to be grounded on an ONS dataset.
If you cannot find a suitable ONS dataset to ground your question, set "supported_by_data" to `false` and explain in the answer text about the lack of a suitable ONS dataset.

If the question does not need a visualization set the "visualization" field to: null. Otherwise, generate a visualization with seaborn and save an image in .png format in the {{TMP_DIR}} directory.
The text answer has to contain all the required information, the visualization is only an optional add-on.
Do not try to show the image: the python_repl tool executes the code in a sub-process without a GUI.

For each user query you will be provided with a set of datasets whose description is semantically similar to the given query.
You can search additional ONS datasets with the `search_datasets` tool.
For example, a more generic dataset can contain specific information about the user query once you apply a filter on its dimensions.

You should return a JSON object with the following format:
{
    "answer": "answer text",
    "supported_by_data": true / false,
    "visualization": "{{TMP_DIR}}/image_name.png" or null
}

The python environment of the python_repl tool was initialised with the following code (you do not need to rewrite this code):
{{CODE_PREAMBLE}}

To load an ONS dataset by Dataset-ID into a pandas dataframe, use the following code:
```python
df = query_handler.query_ons_dataset('Dataset-ID', {'dimension-name-1': 'value-1', ... , 'dimension-name-n': 'value-n'})
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

CODE_PREAMBLE = """
# Data and Time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Visualization Libraries
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

# ONS Dataset Query
from aws_data_analyst.datasets import QueryHandler

query_handler = QueryHandler()
"""

# Render a generic system prompt prototype to be used as part of the cache-key
SYSTEM_PROMPT_PROTOTYPE = SYSTEM_PROMPT_TEMPLATE.render(TMP_DIR="/tmp", CODE_PREAMBLE=CODE_PREAMBLE)


def prepare_prompt(datasets_db, query, datasets=None):
    if datasets is None:
        datasets = datasets_db.search_entries(query, topK=DATASET_RETRIEVAL_TOPK)
        
    prompt_buffer = [f"User Query: {query}"]
    if datasets:
        prompt_buffer.append("\nThe following are ONS datasets whose descriptions are semantically similar to this query:")
        for dataset in datasets['entries']:
            metadata = load_dataset_metadata(dataset['key'])
            prompt_buffer.append(metadata_to_description(metadata, max_dim_items=400))
    return '\n'.join(prompt_buffer)


class DataAnalystAgent:
    def __init__(self, model_id=DEFAULT_MODEL_ID, session_history=None):
        self.model_id = model_id
        self.cost = MODELS[model_id]['cost']

        self.tmp_dir = tempfile.mkdtemp(prefix='ons_', dir='/tmp')
        self.python_repl = PythonInterpreter(CODE_PREAMBLE)
        self.datasets_db = DatasetsDB()
        self.agent = Agent(
            model = BedrockModel(
                model_id=model_id,
                temperature=TEMPERATURE,
            ),
            tools=[
                self.python_repl.get_tool(),
                DatasetSearch(self.datasets_db).get_tool()
            ],
            callback_handler=None,
            system_prompt=SYSTEM_PROMPT_TEMPLATE.render(CODE_PREAMBLE=CODE_PREAMBLE, TMP_DIR=self.tmp_dir),
            messages=session_history)

    def __del__(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except:
            pass

    def __cache_key(self, prompt):
        return str([
            ("model_id", self.model_id),
            ("temperature", TEMPERATURE),
            ("system_prompt", SYSTEM_PROMPT_PROTOTYPE),
            ("prompt", prompt),
        ])

    def on_demand_cost(self, metrics):
        return metrics['accumulated_usage']['inputTokens'] * self.cost['on_demand']['input'] + metrics['accumulated_usage']['outputTokens'] * self.cost['on_demand']['output']

    def __post_process_result(self, response):
        response_data = repair_json(str(response), return_objects=True)
        if type(response_data) is str:
            response_data = {"answer": response_data}
        
        if 'visualization' in response_data and response_data['visualization']:
            if type(response_data['visualization']) is list:
                response_data['visualization'] = response_data['visualization'][0]
            
            if not path.exists(response_data['visualization']):
                response_data['visualization'] = None
            else:
                # Serialize the image to base64
                with open(response_data['visualization'], 'rb') as f:
                    response_data['visualization'] = base64.b64encode(f.read()).decode('utf-8')
        else:
            response_data['visualization'] = None

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
                metadata = load_dataset_metadata(dataset)
                datasets.append({
                    'key': dataset,
                    'title': metadata['title'],
                    'description': metadata['description'],
                    'count': count
                })
            query_metrics['datasets'] = datasets
            response_data['metrics']['query'] = query_metrics
        return response_data

    def handle_message(self, message, datasets=None, cached=True):
        prompt = prepare_prompt(self.datasets_db, message, datasets=datasets)
        
        cache_key = self.__cache_key(prompt)
        if cached and cache_key in CACHE:
            return CACHE[cache_key]

        self.python_repl.clear_state()
        response = self.agent(prompt)

        response_data = self.__post_process_result(response)
        CACHE[cache_key] = response_data
        return response_data

    async def stream_async(self, message, datasets=None, cached=True):
        prompt = prepare_prompt(self.datasets_db, message, datasets=datasets)
        
        cache_key = self.__cache_key(prompt)
        if cached and cache_key in CACHE:
            yield {
                'msg_type': 'result',
                'result': CACHE[cache_key]
            }
        else:
            self.python_repl.clear_state()
            agent_stream = self.agent.stream_async(prompt)
            async for event in agent_stream:
                if 'result' in event:
                    result = self.__post_process_result(event['result'])
                    CACHE[cache_key] = result
                    yield {
                        'msg_type': 'result',
                        'result': result
                    }
                elif 'message' in event:
                    yield {
                        'msg_type': 'message',
                        'message': event['message']
                    }


if __name__ == "__main__":
    from argparse import ArgumentParser
    import asyncio

    parser = ArgumentParser()
    parser.add_argument('query')
    args = parser.parse_args()

    agent = DataAnalystAgent()

    async def process_streaming_response():
        agent_stream = agent.stream_async(args.query, cached=False)
        async for msg in agent_stream:
            print(f"\n\nMESSAGE: {msg}")

    asyncio.run(process_streaming_response())
