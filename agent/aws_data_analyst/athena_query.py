import time

import pandas as pd
import boto3

from aws_data_analyst.infrastructure import S3_ATHENA_QUERY_RESULTS


ATHENA_CLIENT = boto3.client('athena')
ATHENA_DATABASE = "datasets"
ATHENA_SLEEP = 0.05 # 50 ms
ATHENA_MAX_ATTEMPTS = 600 # Max 30 sec


def run_athena_query(query):
    response = ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_ATHENA_QUERY_RESULTS}
    )
    query_execution_id = response['QueryExecutionId']
    
    # Poll query status until completion (max 30 seconds)
    attempt = 0
    while attempt < ATHENA_MAX_ATTEMPTS:
        status_response = ATHENA_CLIENT.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = status_response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(ATHENA_SLEEP)
        attempt += 1

    if status == 'FAILED':
        reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Athena query failed: {reason}")
    elif status == 'CANCELLED':
        raise Exception("Athena query was cancelled")
    elif status != 'SUCCEEDED':
        raise Exception("Athena query timed out")
    
    response = ATHENA_CLIENT.get_query_results(
        QueryExecutionId=query_execution_id
    )
    
    # Extract column names from the first row (header)
    columns = [col['VarCharValue'] for col in response['ResultSet']['Rows'][0]['Data']]
    
    # Extract data rows (skip the header row)
    data = []
    for row in response['ResultSet']['Rows'][1:]:
        data.append([col.get('VarCharValue', None) for col in row['Data']])
    # Handle pagination if there are more results
    while 'NextToken' in response:
        response = ATHENA_CLIENT.get_query_results(
            QueryExecutionId=query_execution_id,
            NextToken=response['NextToken']
        )
        for row in response['ResultSet']['Rows']:
            data.append([col.get('VarCharValue', None) for col in row['Data']])

    return pd.DataFrame(data, columns=columns)


def athena_query(dataset_id, dimension_filters=None, limit=None):
    table_name = f"dataset_{dataset_id.replace('-', '_')}"
    query = f"SELECT * FROM {table_name}"
    if dimension_filters:
        where_clauses = []
        for column, value in dimension_filters.items():
            escaped_value = value.replace("'", "''")
            where_clauses.append(f"{column} = '{escaped_value}'")
        query += " WHERE " + " AND ".join(where_clauses)
    if limit is not None:
        query += f" LIMIT {limit}"
    
    df = run_athena_query(query)
    df['observation'] = pd.to_numeric(df['observation'])
    return df
