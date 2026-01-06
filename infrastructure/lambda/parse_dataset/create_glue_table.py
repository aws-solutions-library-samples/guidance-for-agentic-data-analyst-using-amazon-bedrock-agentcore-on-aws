"""
Lambda function to automatically create Glue tables when new Parquet files are uploaded to S3.
Triggered by S3 PUT events for .parquet files in the datasets/ prefix.

This function:
1. Parses S3 events to extract bucket and object key
2. Extracts dataset_id from the S3 path (e.g., datasets/TS051/data.parquet → TS051)
3. Reads Parquet schema using PyArrow without loading data
4. Maps PyArrow types to Glue/Athena types
5. Generates table name following convention: dataset_{id}
6. Creates or updates Glue table with extracted schema
"""

import json
import boto3
import os
import urllib.parse
import re
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

# Environment variables
DATABASE_NAME = os.environ.get('GLUE_DATABASE_NAME', 'datasets')
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')


# Custom exceptions for error handling
class S3AccessError(Exception):
    """Raised when S3 file cannot be accessed (not found, permission denied, etc.)"""
    pass


class ParquetSchemaError(Exception):
    """Raised when Parquet file cannot be parsed or has invalid schema"""
    pass


class GlueAPIError(Exception):
    """Raised when Glue API calls fail (rate limiting, invalid schema, etc.)"""
    pass


def parse_s3_event(record: Dict[str, Any]) -> tuple[str, str]:
    """
    Parse S3 event to extract bucket name and object key.
    
    Args:
        record: S3 event record
        
    Returns:
        Tuple of (bucket_name, object_key)
        
    Raises:
        ValueError: If event structure is malformed
    """
    try:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        return bucket, key
    except KeyError as e:
        raise ValueError(f"Malformed S3 event record: missing key {e}")


def extract_dataset_id(s3_key: str) -> Optional[str]:
    """
    Extract dataset_id from S3 key path (legacy function for backward compatibility).
    
    Expected format: datasets/TS051/data.parquet → TS051
    
    Args:
        s3_key: S3 object key
        
    Returns:
        Dataset ID or None if pattern doesn't match
    """
    # Pattern: datasets/{dataset_id}/data.parquet
    match = re.match(r'^datasets/([^/]+)/[^/]+\.parquet$', s3_key)
    if match:
        return match.group(1)
    return None


def extract_dataset_id_and_version(s3_key: str) -> tuple[Optional[str], Optional[int]]:
    """
    Extract dataset_id and version from S3 key path.
    
    Expected format: datasets/{dataset-id}/{version}/data.parquet
    Example: datasets/ageing-population-estimates/4/data.parquet → ('ageing-population-estimates', 4)
    
    Args:
        s3_key: S3 object key
        
    Returns:
        Tuple of (dataset_id, version) or (None, None) if pattern doesn't match
    """
    # Pattern: datasets/{dataset_id}/{version}/data.parquet
    match = re.match(r'^datasets/([^/]+)/(\d+)/[^/]+\.parquet$', s3_key)
    if match:
        dataset_id = match.group(1)
        version_str = match.group(2)
        
        try:
            version = int(version_str)
            
            if version < 1 or version > 999:
                logger.error(f"[VERSION ERROR] Version {version} out of valid range (1-999) for dataset '{dataset_id}' in key: {s3_key}")
                return None, None
            
            logger.info(f"[VERSION] Extracted dataset_id='{dataset_id}', version={version} from {s3_key}")
            return dataset_id, version
            
        except ValueError as e:
            logger.error(f"[VERSION ERROR] Invalid version number in key {s3_key}: {e}")
            return None, None
    
    return None, None


def validate_parquet_file(s3_key: str) -> bool:
    """
    Validate that the file has .parquet extension.
    
    Args:
        s3_key: S3 object key
        
    Returns:
        True if file is a Parquet file, False otherwise
    """
    return s3_key.lower().endswith('.parquet')


def extract_parquet_schema(bucket: str, key: str) -> List[Dict[str, str]]:
    """
    Extract schema from Parquet file using PyArrow without loading data.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        List of column definitions with Name, Type, and Comment
        
    Raises:
        S3AccessError: If S3 file cannot be accessed
        ParquetSchemaError: If Parquet file cannot be parsed
    """
    import pyarrow.parquet as pq
    import pyarrow as pa
    import io
    from botocore.exceptions import ClientError
    
    logger.info(f"Reading Parquet schema from s3://{bucket}/{key}")
    
    try:
        # Download Parquet file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_data = response['Body'].read()
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
    
    try:
        # Read schema using PyArrow
        parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))
        schema = parquet_file.schema_arrow
        
        logger.info(f"Extracted schema with {len(schema)} columns")
        
        columns = []
        for field in schema:
            glue_type = map_pyarrow_to_glue_type(field.type)
            columns.append({
                'Name': field.name,
                'Type': glue_type,
                'Comment': f'Column from Parquet schema'
            })
        
        return columns
    except Exception as e:
        logger.error(f"Failed to parse Parquet schema: {str(e)}", exc_info=True)
        raise ParquetSchemaError(f"Invalid Parquet file: {str(e)}") from e


def map_pyarrow_to_glue_type(arrow_type) -> str:
    """
    Map PyArrow data types to Glue/Athena types.
    
    Args:
        arrow_type: PyArrow data type
        
    Returns:
        Glue/Athena type string
    """
    import pyarrow as pa
    import pyarrow.types as pat
    
    # String types
    if pat.is_string(arrow_type) or pat.is_unicode(arrow_type) or pat.is_large_string(arrow_type):
        return 'string'
    
    # Integer types
    elif pat.is_int64(arrow_type):
        return 'bigint'
    elif pat.is_int32(arrow_type) or pat.is_int16(arrow_type) or pat.is_int8(arrow_type):
        return 'int'
    
    # Floating point types
    elif pat.is_float64(arrow_type):
        return 'double'
    elif pat.is_float32(arrow_type):
        return 'float'
    
    # Boolean type
    elif pat.is_boolean(arrow_type):
        return 'boolean'
    
    # Temporal types
    elif pat.is_timestamp(arrow_type):
        return 'timestamp'
    elif pat.is_date(arrow_type):
        return 'date'
    
    # Nested types
    elif pat.is_struct(arrow_type):
        # Build struct type string
        field_types = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            field_type = map_pyarrow_to_glue_type(field.type)
            field_types.append(f"{field.name}:{field_type}")
        return f"struct<{','.join(field_types)}>"
    
    elif pat.is_list(arrow_type) or pat.is_large_list(arrow_type):
        value_type = map_pyarrow_to_glue_type(arrow_type.value_type)
        return f"array<{value_type}>"
    
    elif pat.is_map(arrow_type):
        key_type = map_pyarrow_to_glue_type(arrow_type.key_type)
        value_type = map_pyarrow_to_glue_type(arrow_type.item_type)
        return f"map<{key_type},{value_type}>"
    
    # Default to string for unknown types
    else:
        logger.warning(f"Unknown PyArrow type {arrow_type}, defaulting to string")
        return 'string'


def generate_table_name(dataset_id: str) -> str:
    """
    Generate Glue table name from dataset_id.
    
    Converts to lowercase and prefixes with "dataset_".
    Example: TS051 → dataset_ts051
    
    Args:
        dataset_id: Dataset identifier
        
    Returns:
        Valid Glue table name
        
    Raises:
        ValueError: If table name doesn't follow Glue naming rules
    """
    # Convert to lowercase
    table_name = f"dataset_{dataset_id.lower()}"
    
    # Replace hyphens with underscores for Glue compatibility
    table_name = table_name.replace('-', '_')
    
    # Validate table name follows Glue rules:
    # - Must be 1-255 characters
    # - Can contain only alphanumeric and underscore characters
    if not re.match(r'^[a-z0-9_]+$', table_name):
        raise ValueError(f"Invalid table name: {table_name}. Must contain only lowercase letters, numbers, and underscores.")
    
    if len(table_name) > 255:
        raise ValueError(f"Table name too long: {table_name} ({len(table_name)} characters)")
    
    logger.info(f"Generated table name: {table_name} from dataset_id: {dataset_id}")
    return table_name


def build_table_input(table_name: str, columns: List[Dict[str, str]], s3_location: str) -> Dict[str, Any]:
    """
    Build Glue TableInput dictionary with extracted schema (legacy non-partitioned).
    
    Args:
        table_name: Name of the Glue table
        columns: List of column definitions with Name, Type, and Comment
        s3_location: S3 location of the dataset folder (e.g., s3://bucket/datasets/TS051/)
        
    Returns:
        TableInput dictionary for Glue create_table/update_table API
    """
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': True,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            },
            'StoredAsSubDirectories': False
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'parquet.compression': 'SNAPPY',
            'classification': 'parquet',
            'typeOfData': 'file',
            'exclusions': '["*.json","*.csv","*.txt"]'
        }
    }
    
    logger.info(f"Built TableInput for {table_name} with {len(columns)} columns at {s3_location}")
    return table_input


def build_partitioned_table_input(
    table_name: str, 
    columns: List[Dict[str, str]], 
    s3_location: str,
    partition_keys: List[Dict[str, str]]
) -> Dict[str, Any]:
    """
    Build Glue TableInput with partition keys for versioned datasets.
    
    Args:
        table_name: Name of the Glue table
        columns: List of column definitions (non-partition columns)
        s3_location: S3 location of the dataset root (e.g., s3://bucket/datasets/ageing-population-estimates/)
        partition_keys: List of partition column definitions
        
    Returns:
        TableInput dictionary for Glue create_table/update_table API
    """
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': True,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            },
            'StoredAsSubDirectories': True  # Enable subdirectory support for partitions
        },
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'parquet.compression': 'SNAPPY',
            'classification': 'parquet',
            'typeOfData': 'file',
            'exclusions': '["*.json","*.csv","*.txt"]',
            'projection.enabled': 'true',
            'projection.version.type': 'integer',
            'projection.version.range': '1,999',
            'storage.location.template': f'{s3_location}${{version}}/'
        }
    }
    
    logger.info(f"Built partitioned TableInput for {table_name} with {len(columns)} columns and {len(partition_keys)} partition keys at {s3_location}")
    return table_input


def create_or_update_table(table_name: str, columns: List[Dict[str, str]], s3_location: str) -> Dict[str, Any]:
    """
    Create or update Glue table with conflict handling and schema update detection.

    Args:
        table_name: Name of the Glue table
        columns: List of column definitions
        s3_location: S3 location of the dataset folder
        
    Returns:
        Dictionary with operation result details
        
    Raises:
        GlueAPIError: If Glue API calls fail
    """
    from botocore.exceptions import ClientError
    
    table_input = build_table_input(table_name, columns, s3_location)
    
    try:
        existing_table = glue_client.get_table(
            DatabaseName=DATABASE_NAME,
            Name=table_name
        )
        
        logger.info(f"Table {table_name} already exists, checking for schema changes")
        
        existing_columns = existing_table['Table']['StorageDescriptor']['Columns']
        schema_changes = detect_schema_changes(existing_columns, columns)
        
        if schema_changes['has_changes']:
            logger.info(f"Schema changes detected for {table_name}: {schema_changes}")
            
            try:
                # Preserve the existing table's metadata
                table_input['Parameters'] = existing_table['Table'].get('Parameters', table_input['Parameters'])
                
                glue_client.update_table(
                    DatabaseName=DATABASE_NAME,
                    TableInput=table_input
                )
                
                logger.info(f"Successfully updated table {table_name} with schema changes: "
                           f"added={len(schema_changes['added_columns'])}, "
                           f"removed={len(schema_changes['removed_columns'])}, "
                           f"modified={len(schema_changes['type_changes'])}")
                
                return {
                    'operation': 'updated',
                    'table_name': table_name,
                    'column_count': len(columns),
                    'schema_changes': schema_changes
                }
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ThrottlingException':
                    logger.error(f"Glue API rate limit exceeded for table update: {table_name}")
                    raise GlueAPIError(f"Rate limit exceeded updating table {table_name}") from e
                elif error_code == 'InvalidInputException':
                    logger.error(f"Invalid schema for table {table_name}: {str(e)}")
                    raise GlueAPIError(f"Invalid schema for table {table_name}") from e
                else:
                    logger.error(f"Glue API error updating table {table_name}: {error_code}")
                    raise GlueAPIError(f"Failed to update table: {error_code}") from e
        else:
            logger.info(f"No schema changes detected for {table_name}, skipping update")
            return {
                'operation': 'unchanged',
                'table_name': table_name,
                'column_count': len(columns)
            }
            
    except glue_client.exceptions.EntityNotFoundException:
        logger.info(f"Table {table_name} does not exist, creating new table")
        
        try:
            glue_client.create_table(
                DatabaseName=DATABASE_NAME,
                TableInput=table_input
            )
            
            logger.info(f"Successfully created table {table_name} with {len(columns)} columns")
            
            return {
                'operation': 'created',
                'table_name': table_name,
                'column_count': len(columns),
                'columns': [col['Name'] for col in columns]
            }
            
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Table {table_name} was created by another process, skipping")
            return {
                'operation': 'already_exists',
                'table_name': table_name,
                'column_count': len(columns)
            }
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ThrottlingException':
                logger.error(f"Glue API rate limit exceeded for table creation: {table_name}")
                raise GlueAPIError(f"Rate limit exceeded creating table {table_name}") from e
            elif error_code == 'InvalidInputException':
                logger.error(f"Invalid schema for table {table_name}: {str(e)}")
                raise GlueAPIError(f"Invalid schema for table {table_name}") from e
            else:
                logger.error(f"Glue API error creating table {table_name}: {error_code}")
                raise GlueAPIError(f"Failed to create table: {error_code}") from e
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Glue API error checking table {table_name}: {error_code}")
        raise GlueAPIError(f"Failed to check table existence: {error_code}") from e

def get_file_size(bucket: str, key: str) -> int:
    """
    Get the size of an S3 object in bytes.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        File size in bytes, or 0 if error
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.warning(f"Could not get file size for s3://{bucket}/{key}: {e}")
        return 0


def detect_schema_changes(existing_columns: List[Dict[str, str]], new_columns: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Detect schema changes between existing and new column definitions.
    
    Args:
        existing_columns: Current table columns from Glue
        new_columns: New columns from Parquet schema
        
    Returns:
        Dictionary with schema change details
    """
    existing_map = {col['Name']: col['Type'] for col in existing_columns}
    new_map = {col['Name']: col['Type'] for col in new_columns}
    
    # Detect added columns
    added_columns = [name for name in new_map if name not in existing_map]
    
    # Detect removed columns
    removed_columns = [name for name in existing_map if name not in new_map]
    
    # Detect type changes
    type_changes = []
    for name in existing_map:
        if name in new_map and existing_map[name] != new_map[name]:
            type_changes.append({
                'column': name,
                'old_type': existing_map[name],
                'new_type': new_map[name]
            })
    
    has_changes = bool(added_columns or removed_columns or type_changes)
    
    return {
        'has_changes': has_changes,
        'added_columns': added_columns,
        'removed_columns': removed_columns,
        'type_changes': type_changes
    }

def lambda_handler(event, context):
    """
    Lambda handler triggered by S3 PUT events for Parquet files.
    
    Args:
        event: S3 event notification
        context: Lambda context
        
    Returns:
        Response with status code and processing results
    """
    request_id = context.aws_request_id if context else 'unknown'
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
                
                if not validate_parquet_file(key):
                    logger.warning(f"Skipping non-Parquet file: {key}")
                    continue
                
                # Try to extract dataset_id and version from versioned path
                dataset_id, version = extract_dataset_id_and_version(key)
                
                if dataset_id and version:
                    # Versioned path detected
                    logger.info(f"Processing versioned dataset: {dataset_id} version {version}")
                    
                    # Extract Parquet schema
                    columns = extract_parquet_schema(bucket, key)
                    logger.info(f"Extracted {len(columns)} columns from Parquet schema")
                    
                    # Get file size
                    file_size = get_file_size(bucket, key)
                    
                    # Generate table name
                    table_name = generate_table_name(dataset_id)
                    
                    # Full S3 location for this version
                    version_s3_location = f"s3://{bucket}/{key}"
                    
                    # Check if table exists
                    try:
                        existing_table = glue_client.get_table(DatabaseName=DATABASE_NAME, Name=table_name)
                        
                        # Table exists with partition projection - no action needed
                        # Partition projection automatically discovers versions 1-999
                        logger.info(f"Table {table_name} exists with partition projection enabled")
                        logger.info(f"Version {version} will be automatically discovered by Athena (no partition registration needed)")
                        
                        results.append({
                            'status': 'success',
                            'dataset_id': dataset_id,
                            'version': version,
                            'table_name': table_name,
                            'operation': 'no_action_needed',
                            'reason': 'partition_projection_enabled'
                        })
                        
                        logger.info(f"Successfully processed {dataset_id} v{version}: table exists, partition projection handles discovery")
                        
                    except glue_client.exceptions.EntityNotFoundException:
                        # Table doesn't exist - create it with partition projection
                        logger.info(f"Table {table_name} does not exist, creating with partition projection")
                        
                        dataset_root = f"datasets/{dataset_id}/"
                        s3_location = f"s3://{bucket}/{dataset_root}"
                        
                        partition_keys = [{'Name': 'version', 'Type': 'int', 'Comment': 'Dataset version number'}]
                        
                        table_input = build_partitioned_table_input(table_name, columns, s3_location, partition_keys)
                        
                        glue_client.create_table(DatabaseName=DATABASE_NAME, TableInput=table_input)
                        logger.info(f"Successfully created partitioned table {table_name} with partition projection")
                        logger.info(f"Partition projection enabled for versions 1-999 (no manual partition registration needed)")
                        
                        results.append({
                            'status': 'success',
                            'dataset_id': dataset_id,
                            'version': version,
                            'table_name': table_name,
                            'operation': 'created_with_projection',
                            'projection_range': '1-999'
                        })
                        
                        logger.info(f"Successfully created table {table_name} with partition projection for versions 1-999")
                    
                else:
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
                    
                    logger.info(f"Processing legacy non-versioned dataset: {dataset_id}")
                    
                    # Extract Parquet schema
                    columns = extract_parquet_schema(bucket, key)
                    logger.info(f"Extracted {len(columns)} columns from Parquet schema")
                    
                    # Generate table name
                    table_name = generate_table_name(dataset_id)
                    
                    # Create or update Glue table (legacy non-partitioned)
                    dataset_folder = '/'.join(key.split('/')[:-1]) + '/'
                    s3_location = f"s3://{bucket}/{dataset_folder}"
                    
                    table_result = create_or_update_table(table_name, columns, s3_location)
                    
                    results.append({
                        'status': 'success',
                        'dataset_id': dataset_id,
                        **table_result
                    })
                    
                    logger.info(f"Successfully processed {dataset_id} -> {table_name}: {table_result['operation']}")
                
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
                
            except ParquetSchemaError as e:
                error_msg = f"Parquet schema error for dataset_id={dataset_id}, s3://{bucket}/{key}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                
                results.append({
                    'status': 'error',
                    'error_type': 'parquet_schema_error',
                    'dataset_id': dataset_id,
                    's3_key': key,
                    'error': str(e)
                })
                
            except GlueAPIError as e:
                error_msg = f"Glue API error for dataset_id={dataset_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                results.append({
                    'status': 'error',
                    'error_type': 'glue_api_error',
                    'dataset_id': dataset_id,
                    's3_key': key,
                    'error': str(e)
                })
                
            except ValueError as e:
                error_msg = f"Validation error for dataset_id={dataset_id}: {str(e)}"
                logger.error(error_msg)
                results.append({
                    'status': 'error',
                    'error_type': 'validation_error',
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
