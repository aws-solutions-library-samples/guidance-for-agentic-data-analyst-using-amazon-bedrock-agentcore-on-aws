import re
import json
import logging
import urllib.parse
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

from datasets_db import DatasetsDB


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
db = DatasetsDB()


class S3AccessError(Exception):
    """Raised when S3 file cannot be accessed (not found, permission denied, etc.)"""
    pass


def parse_s3_event(record: Dict[str, Any]) -> tuple[str, str]:
    try:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        return bucket, key
    except KeyError as e:
        raise ValueError(f"Malformed S3 event record: missing key {e}")


def extract_dataset_id(s3_key: str) -> Optional[str]:
    # Pattern: metadata/{dataset_id}/dataset.json
    match = re.match(r'^metadata/([^/]+)/dataset\.json$', s3_key)
    if match:
        return match.group(1)
    return None


def load_metadata(bucket: str, key: str):
    try:
        # Download metadata file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read())
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.error(f"S3 file not found: s3://{bucket}/{key}")
            raise S3AccessError(f"File not found: s3://{bucket}/{key}") from e
        elif error_code == 'AccessDenied':
            logger.error(f"S3 access denied: s3://{bucket}/{key}")
            raise S3AccessError(f"Access denied: s3://{bucket}/{key}") from e
        else:
            logger.error(f"S3 error ({error_code}): {str(e)}")
            raise S3AccessError(f"S3 error: {error_code}") from e
    except Exception as e:
        logger.error(f"Unexpected error reading from S3: {str(e)}", exc_info=True)
        raise S3AccessError(f"Failed to read from S3: {str(e)}") from e


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    results = []
    try:
        for record in event['Records']:
            dataset_id = None
            bucket = None
            key = None
            
            try:
                bucket, key = parse_s3_event(record)
                logger.info(f"Processing: s3://{bucket}/{key}")
                        
                # Fall back to legacy non-versioned path
                dataset_id = extract_dataset_id(key)
                if not dataset_id:
                    logger.warning(f"Could not extract dataset_id from key: {key}")
                    results.append({
                        'status': 'skipped',
                        'reason': 'invalid_path',
                        's3_key': key
                    })
                    continue

                logger.info(f"Processing dataset: {dataset_id}")
                metadata = load_metadata(bucket, key)
                db.add_entry(dataset_id, metadata['indexing-description'])
                logger.info(f"Successfully processed {dataset_id}")
            except S3AccessError as e:
                error_msg = f"S3 access error for dataset_id={dataset_id}, s3://{bucket}/{key}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                
                results.append({
                    'status': 'error',
                    'error_type': 's3_access_error',
                    'dataset_id': dataset_id,
                    's3_key': key,
                    'error': str(e)
                })
            except Exception as e:
                error_msg = f"Unexpected error processing dataset_id={dataset_id}, s3://{bucket}/{key}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                results.append({
                    'status': 'error',
                    'error_type': 'unexpected_error',
                    'dataset_id': dataset_id,
                    's3_key': key,
                    'error': str(e)
                })

    except Exception as e:
        error_msg = f"Fatal error in lambda_handler: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Fatal error processing event',
                'error': str(e),
                'error_type': 'fatal_error'
            })
        }
    
    # Determine overall status code based on results
    has_errors = any(r['status'] == 'error' for r in results)
    has_success = any(r['status'] == 'success' for r in results)
    
    if has_errors and not has_success:
        status_code = 500  # All failed
    elif has_errors and has_success:
        status_code = 207  # Partial success (Multi-Status)
    else:
        status_code = 200  # All succeeded
    
    return {
        'statusCode': status_code,
        'body': json.dumps({
            'message': 'Processing complete',
            'total_records': len(results),
            'successful': sum(1 for r in results if r['status'] == 'success'),
            'failed': sum(1 for r in results if r['status'] == 'error'),
            'skipped': sum(1 for r in results if r['status'] == 'skipped'),
            'results': results
        })
    }
