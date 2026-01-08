import boto3

ssm = boto3.client('ssm')

S3_DATA_BUCKET = ssm.get_parameter(Name='/data-analyst/data-bucket')['Parameter']['Value']
S3_DATASETS_METADATA = f"s3://{S3_DATA_BUCKET}/metadata/"
S3_ATHENA_QUERY_RESULTS_BUCKET = ssm.get_parameter(Name='/data-analyst/athena-query-results-bucket')['Parameter']['Value']
S3_ATHENA_QUERY_RESULTS = f"s3://{S3_ATHENA_QUERY_RESULTS_BUCKET}"

AGENT_ARN = ssm.get_parameter(Name='/data-analyst/agent-runtime-arn')['Parameter']['Value']
