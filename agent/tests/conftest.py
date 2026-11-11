"""Pytest glue for the offline agent harness (see offline_aws.py).

`offline_aws` is imported first so moto and the AWS hooks are active before any
`aws_data_analyst` module is imported by the tests.
"""
import pytest

import offline_aws


def pytest_unconfigure(config):
    offline_aws.MOCK.stop()


@pytest.fixture
def aws_calls():
    offline_aws.AWS_CALLS.clear()
    return offline_aws.AWS_CALLS


@pytest.fixture
def athena_results():
    yield offline_aws.queue_athena_result
    offline_aws.clear_athena_results()
