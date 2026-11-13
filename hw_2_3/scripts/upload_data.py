"""Create the teaching bucket and upload a chosen bundled dataset."""
import argparse
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def s3_client():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
                        region_name='us-east-1',
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                      request_checksum_calculation='when_required',
                                      response_checksum_validation='when_required'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', choices=['demo', 'fixtures', 'full', 'all'], default='all')
    args = parser.parse_args()
    client = s3_client()
    bucket = os.environ.get('S3_BUCKET', 'hw23')
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket', 'NotFound'):
            raise
        client.create_bucket(Bucket=bucket)
    root = Path('/opt/airflow/data')
    datasets = ['demo', 'fixtures'] if args.dataset == 'all' else [args.dataset]
    for dataset in datasets:
        files = sorted((root / dataset / 'events').glob('*.csv'))
        campaigns = root / dataset / 'campaigns.csv'
        if not files or not campaigns.is_file():
            raise FileNotFoundError(f'Missing events/campaigns in {root / dataset}')
        # A smaller regenerated file set must not silently leave old source CSVs.
        expected_keys = {f'raw/{dataset}/events/{p.name}' for p in files}
        existing_keys = set()
        for page in client.get_paginator('list_objects_v2').paginate(
                Bucket=bucket, Prefix=f'raw/{dataset}/events/'):
            existing_keys.update(item['Key'] for item in page.get('Contents', [])
                                 if item['Key'].endswith('.csv'))
        stale_keys = existing_keys - expected_keys
        if stale_keys:
            raise RuntimeError(f'Unexpected source CSVs in raw/{dataset}/events/: {sorted(stale_keys)}. '
                               'Resolve the dataset mismatch before uploading; no files were deleted.')
        for path in files + [campaigns]:
            key = f'raw/{dataset}/' + path.relative_to(root / dataset).as_posix()
            client.upload_file(str(path), bucket, key)
        print(f'UPLOADED dataset={dataset} files={len(files) + 1} bucket={bucket}', flush=True)


if __name__ == '__main__':
    main()
