import boto3


ssm = boto3.client('ssm')


def get_infrastructure_param(name):
    return ssm.get_parameter(Name=f"/data-analyst/{name}")['Parameter']['Value']


S3_DATA_BUCKET = get_infrastructure_param('data-bucket')
S3_DATASETS_METADATA = f"s3://{S3_DATA_BUCKET}/metadata/"
S3_ATHENA_QUERY_RESULTS_BUCKET = get_infrastructure_param('athena-query-results-bucket')
S3_ATHENA_QUERY_RESULTS = f"s3://{S3_ATHENA_QUERY_RESULTS_BUCKET}"

AGENT_ARN = get_infrastructure_param('agent-runtime-arn')


def get_vectordb_configuration(dev=False):
    name_postfix = '-dev' if dev else ''
    return {
        'embedder_id': get_infrastructure_param('vectordb_embedder'),
        'embedding_dimension': int(get_infrastructure_param('vectordb_dimension')),
        'bucket': get_infrastructure_param('vectordb_bucket' + name_postfix),
        'index': get_infrastructure_param('vectordb_index' + name_postfix)
    }


def get_region_and_account_id():
    session = boto3.session.Session()
    region = session.region_name
    account_id = session.client('sts').get_caller_identity()['Account']
    return region, account_id
