#!/usr/bin/env python3
"""Upload Parquet files from local datasets folder to S3 to trigger automatic table creation."""
import logging
from enum import Enum
from collections import Counter

import boto3
from botocore.exceptions import ClientError

from aws_data_analyst import DATASETS_DIR
from aws_data_analyst.datasets import iterate_datasets



ssm = boto3.client('ssm')
S3_BUCKET = ssm.get_parameter(Name='/data-analyst/data-bucket')['Parameter']['Value']
S3_PREFIX = "datasets"


s3_client = boto3.client('s3')
logger = logging.getLogger(__name__)


class UploadStatus(Enum):
    UPLOADED = "uploaded"
    SKIPPED = "skipped"
    FAILED = "failed"


def upload_file_to_s3(local_file, s3_key, content_type) -> UploadStatus:
    """
    Upload a single file to S3.
    
    Args:
        s3_client: Boto3 S3 client
        local_file: Path to local file
        s3_key: S3 key for the file
        content_type: Content type for the file
        
    Returns:
        UploadStatus
    """
    logger.info(f"[UPLOAD] {local_file.name} to s3://{S3_BUCKET}/{s3_key}")
    try:
        # Check if file already exists
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            logger.info("[SKIPPED]")
            return UploadStatus.SKIPPED
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                logger.error(f"[S3 ERROR] Error checking file existence for {s3_key}: {e}")
                raise
        
        # Upload the file
        s3_client.upload_file(
            str(local_file),
            S3_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.info("[UPLOAD SUCCESS]")
        return UploadStatus.UPLOADED
        
    except ClientError as e:
        logger.error(f"S3 ClientError ({e.response['Error']['Code']}): {str(e)}")
        return UploadStatus.FAILED
    except Exception as e:
        logger.error(f"Unexpected Error: {str(e)}", exc_info=True)
        return UploadStatus.FAILED


def upload_dataset(dataset_id):
    logger.info(f"[DATASET] {dataset_id}")
    dataset_dir = DATASETS_DIR / dataset_id
    s3_uri = f"{S3_PREFIX}/{dataset_id}"

    parquet_status = upload_file_to_s3(
        dataset_dir / "data.parquet",
        f"{s3_uri}/data.parquet", 
        'application/octet-stream'
    )
    metadata_status = upload_file_to_s3(
        dataset_dir / "dataset.json",
        f"{s3_uri}/dataset.json", 
        'application/json'
    )

    return parquet_status, metadata_status


def upload_datasets():
    datasets = list(iterate_datasets())
    parquet_files, metadata_files = Counter(), Counter()
    for dataset in datasets:
        parquet_status, metadata_status = upload_dataset(dataset['id'])
        parquet_files[parquet_status] += 1
        metadata_files[metadata_status] += 1
    
    print(f"\n# Total datasets found: {len(datasets)}")
    for file_type, status in [("Parquet", parquet_files), ("Metadata", metadata_files)]:
        print(f"\n{file_type} files:")
        for status, count in status.items():
            print(f"  {status}: {count}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    upload_datasets()
