#!/usr/bin/env python3
"""Upload Parquet files from local datasets folder to S3 to trigger automatic table creation."""
import json
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ons_prototype import DATASETS_DIR


ssm = boto3.client('ssm')
S3_BUCKET = ssm.get_parameter(Name='/data-analyst/data-bucket')['Parameter']['Value']


S3_PREFIX = "datasets/"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_dataset_id(dataset_path):
    """Extract dataset ID from folder name."""
    return Path(dataset_path).name

def upload_file_to_s3(s3_client, local_file, s3_key, content_type, dry_run=False):
    """
    Upload a single file to S3.
    
    Args:
        s3_client: Boto3 S3 client
        local_file: Path to local file
        s3_key: S3 key for the file
        content_type: Content type for the file
        dry_run: If True, only print what would be uploaded
        
    Returns:
        Tuple of (success, skipped, error_message) - error_message is None on success
    """
    if dry_run:
        print(f"[DRY RUN] Would upload: {local_file} -> s3://{S3_BUCKET}/{s3_key}")
        return (True, False, None)
    
    try:
        # Check if file already exists
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            logger.info(f"[VERSION CONFLICT] File already exists, skipping: s3://{S3_BUCKET}/{s3_key}")
            print(f"[SKIP] Already exists: s3://{S3_BUCKET}/{s3_key}")
            return (False, True, "file_already_exists")
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                logger.error(f"[S3 ERROR] Error checking file existence for {s3_key}: {e}")
                raise
        
        # Upload the file
        print(f"[UPLOAD] {local_file} -> s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(
            str(local_file),
            S3_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.info(f"[UPLOAD SUCCESS] Successfully uploaded: s3://{S3_BUCKET}/{s3_key}")
        print("  ✓ Uploaded successfully")
        return (True, False, None)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = f"S3 ClientError ({error_code}): {str(e)}"
        logger.error(f"[UPLOAD ERROR] Failed to upload {local_file} to {s3_key}: {error_msg}")
        print(f"  ✗ Failed to upload: {error_msg}")
        return (False, False, error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"[UPLOAD ERROR] Failed to upload {local_file} to {s3_key}: {error_msg}", exc_info=True)
        print(f"  ✗ Failed to upload: {error_msg}")
        return (False, False, error_msg)


def upload_parquet_files(dry_run=True, limit=None):
    """
    Upload all data.parquet files to S3.
    
    Args:
        dry_run: If True, only print what would be uploaded without actually uploading
        limit: If set, only upload this many datasets (useful for testing)
    """
    s3_client = boto3.client('s3')
    
    # Find all dataset directories with data.parquet files
    parquet_files = list(DATASETS_DIR.glob("*/data.parquet"))
    print(f"Found {len(parquet_files)} datasets to upload")    
    if limit:
        parquet_files = parquet_files[:limit]
        print(f"Limiting to first {limit} datasets")
    
    parquet_uploaded = 0
    parquet_skipped = 0
    parquet_failed = 0

    for parquet_file in parquet_files:
        dataset_id = get_dataset_id(parquet_file.parent)
        
        try:
            logger.info(f"[PROCESSING] Dataset '{dataset_id}'")
            
            # Upload Parquet file
            s3_key = f"{S3_PREFIX}{dataset_id}/data.parquet"
            logger.info(f"[UPLOAD PATH] data.parquet: s3://{S3_BUCKET}/{s3_key}")
            success, skipped, error = upload_file_to_s3(
                s3_client, parquet_file, s3_key, 
                'application/octet-stream', dry_run
            )
            if success:
                parquet_uploaded += 1
            elif skipped:
                parquet_skipped += 1
                if error == "file_already_exists":
                    logger.warning(f"[VERSION CONFLICT] Parquet file for dataset '{dataset_id}' already exists in S3")
            else:
                parquet_failed += 1
                logger.error(f"[UPLOAD FAILED] Failed to upload parquet for dataset '{dataset_id}'")
        except ValueError as e:
            logger.error(f"[VALIDATION ERROR] Failed to process dataset '{dataset_id}': {e}", exc_info=True)
            parquet_failed += 1
        except Exception as e:
            logger.error(f"[PROCESSING ERROR] Unexpected error processing dataset '{dataset_id}': {e}", exc_info=True)
            parquet_failed += 1

    print("\n" + "=" * 80)
    print("UPLOAD SUMMARY")
    print("=" * 80)
    print(f"Total datasets found: {len(parquet_files)}")
    print("\nParquet files:")
    print(f"  Uploaded: {parquet_uploaded}")
    print(f"  Skipped (already exists): {parquet_skipped}")
    print(f"  Failed: {parquet_failed}")

    if dry_run:
        print("\nThis was a DRY RUN. No files were actually uploaded.")
        print("Run with --execute option to perform actual upload.")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Actually upload files")
    parser.add_argument("--limit", type=int, help="Limit to N datasets (for testing)")
    args = parser.parse_args()

    dry_run = not args.execute 
    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No files will be uploaded")
        print("=" * 80)
        print("Add --execute flag to actually upload files")
        print("Add --limit=N to upload only N files (useful for testing)")
        print("=" * 80)
        print()
    
    upload_parquet_files(dry_run=dry_run, limit=args.limit)
