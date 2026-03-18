#!/usr/bin/env python3
"""Upload Parquet files from local datasets folder to S3 to trigger automatic table creation."""
import logging
from enum import Enum
from collections import Counter

import boto3
from botocore.exceptions import ClientError

from aws_data_analyst import DATASETS_DIR
from aws_data_analyst.infrastructure import S3_DATA_BUCKET
from aws_data_analyst.datasets import DATASETS


s3_client = boto3.client('s3')
logger = logging.getLogger(__name__)


class UploadStatus(Enum):
    UPLOADED = "uploaded"
    SKIPPED = "skipped"
    FAILED = "failed"


def upload_file_to_s3(local_file, s3_key, content_type, override=False) -> UploadStatus:
    """
    Upload a single file to S3.
    
    Args:
        local_file: Path to local file
        s3_key: S3 key for the file
        content_type: Content type for the file
        
    Returns:
        UploadStatus
    """
    upload_description = f"Upload {local_file.name} to s3://{S3_DATA_BUCKET}/{s3_key}"
    try:
        # Check if file already exists
        if not override:
            try:
                response = s3_client.head_object(Bucket=S3_DATA_BUCKET, Key=s3_key)
                if response['ContentLength'] == local_file.stat().st_size:
                    logger.info(f"  SKIPPED {upload_description}")
                    return UploadStatus.SKIPPED
                logger.info(f"  Size mismatch for {s3_key}, re-uploading")
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') != '404':
                    logger.error(f"[S3 ERROR] Error checking file existence for {s3_key}: {e}")
                    raise
        
        # Upload the file
        s3_client.upload_file(
            str(local_file),
            S3_DATA_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.info(f"  SUCCESSFUL {upload_description}")
        return UploadStatus.UPLOADED
        
    except ClientError as e:
        logger.error(f"S3 ClientError ({e.response['Error']['Code']}): {str(e)}")
        return UploadStatus.FAILED
    except Exception as e:
        logger.error(f"Unexpected Error: {str(e)}", exc_info=True)
        return UploadStatus.FAILED


def upload_dataset(dataset, override_metadata=False):
    parquet_status = upload_file_to_s3(
        dataset['data_file'],
        f"datasets/{dataset['id']}/data.parquet", 
        'application/octet-stream'
    )
    metadata_status = upload_file_to_s3(
        dataset['metadata_file'],
        f"metadata/{dataset['id']}/dataset.json", 
        'application/json',
        override=override_metadata
    )

    return parquet_status, metadata_status


def iterate_datasets(target_namespace=None):
    for namespace in DATASETS:
        if target_namespace is not None and namespace != target_namespace:
            continue

        namespace_dir = DATASETS_DIR / namespace
        for dataset_dir in namespace_dir.iterdir():
            if not dataset_dir.is_dir():
                continue

            data_file = dataset_dir / "data.parquet"
            metadata_file = dataset_dir / "dataset.json"
            if data_file.exists() and metadata_file.exists():
                yield {
                    "namespace": namespace,
                    "id": dataset_dir.name,
                    "data_file": data_file,
                    "metadata_file": metadata_file
                }


def upload_datasets(namespace=None, override_metadata=False):
    datasets = list(iterate_datasets(namespace))
    parquet_files, metadata_files = Counter(), Counter()
    for i, dataset in enumerate(datasets, 1):
        logger.info(f"# DATASET {i}/{len(datasets)}: {dataset['namespace']}.{dataset['id']}")
        parquet_status, metadata_status = upload_dataset(dataset, override_metadata)
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
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--namespace")
    parser.add_argument("--metadata", default=False, action="store_true")
    args = parser.parse_args()

    upload_datasets(args.namespace, args.metadata)
