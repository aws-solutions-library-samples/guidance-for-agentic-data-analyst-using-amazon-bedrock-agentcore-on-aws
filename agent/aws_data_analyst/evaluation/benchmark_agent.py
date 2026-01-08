"""
Expected Agent Interface:
    - init: agent = Agent(model_id)
    - inference: agent.answer(query)
    - response: {
        'answer': str,
        'metrics': {
            'agent': {
                'on_demand_cost': float,
                'total_duration': float
            }
        }
    }
"""
import json
import statistics
from collections import defaultdict

from joblib import Parallel, delayed
from joblib_progress import joblib_progress

from rich.console import Console
from rich.table import Table
from print_color import print

from aws_data_analyst.evaluation import TESTS_PATH
from aws_data_analyst.data_analyst_agent_client import AgentCoreClient
from aws_data_analyst.bedrock_models import MODELS
from aws_data_analyst.evaluation.llm_as_a_judge import ONS_Evaluator


PARALLEL_JOBS = 10


def load_tests():
    for test in json.load(TESTS_PATH.open()):
        if 'answer' in test or 'supported_by_data' in test:
            yield test


def run_test(model, test):
    agent = AgentCoreClient(model)
    evaluator = ONS_Evaluator()

    query = test['query']
    agent_response = agent.answer(query)

    if 'supported_by_data' in test and not test['supported_by_data']:
        # Question not supported by ONS data
        if agent_response['supported_by_data']:
            score = 0
            rationale = "The Agent did not flag the absence of ONS data."
        else:
            score = 1
            rationale = "The Agent did flag the absence of ONS data."

    else:
        # Question supported by ONS data
        eval_response = evaluator.evaluate(query, test['answer'], agent_response['answer'])
        score = eval_response['score']
        rationale = eval_response['rationale']

    return {
        'model_id': model,
        'test': test,
        'agent_response': agent_response,
        'score': score,
        'rationale': rationale
    }


def test_expectation(test):
    if 'supported_by_data' in test and not test['supported_by_data']:
        return "Not supported by ONS data"
    else:
        return test['answer']
    

def benchmark_agent(models, verbose=False):
    tests = [(model, test) for test in load_tests() for model in models]
    with joblib_progress("Agent Benchmark", total=len(tests)):
        processed = Parallel(n_jobs=PARALLEL_JOBS)(delayed(run_test)(model, test) for model, test in tests)

    scores = defaultdict(list)
    costs = defaultdict(list)
    latencies = defaultdict(list)
    for result in processed:
        model_id = result['model_id']
        scores[model_id].append(result['score'])
        costs[model_id].append(result['agent_response']['metrics']['agent']['on_demand_cost'])
        latencies[model_id].append(result['agent_response']['metrics']['agent']['total_duration'])
        if verbose and result['score'] < 0.7:
            test = result['test']
            print(f"\nQuestion: {test['query']}", color='yellow')
            print(f"Expected Answer: {test_expectation(test)}", color='yellow')
            print(f"Agent Response: {result['agent_response']['answer']}")
            print(f"Score [{result['score']:.0%}]: {result['rationale']}", color='red')
    
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Model")
    table.add_column("Median Latency (s)")
    table.add_column("Mean Cost ($)")
    table.add_column("Mean Score")
    for model in models:
        latency = statistics.median(latencies[model])
        cost = statistics.mean(costs[model])
        score = statistics.mean(scores[model])
        table.add_row(model, f"{latency:.1f}", f"{cost:.2f}", f"{score:.0%}")
    console.print(table)


if __name__ == '__main__':
    import boto3

    session = boto3.session.Session()
    region = session.region_name
    account_id = session.client('sts').get_caller_identity()['Account']
    print(f"Using AWS Account: {account_id}. Region: {region}", color='yellow')

    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--model', type=str, default=None, choices=MODELS.keys())
    args = parser.parse_args()
    
    if args.model:
        models = [args.model]
        verbose = True
    else:
        models = MODELS.keys()
        verbose = False
    
    benchmark_agent(models, verbose=verbose)
