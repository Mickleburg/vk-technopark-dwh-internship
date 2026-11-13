"""Extract assignment inputs, generate the complete 5M-event source, upload S3."""
import json
import subprocess
import sys
import time

from common import BUCKET, ROOT, S3, s3client, save, spark_session
from datasets import extract_inputs


def main():
    extract_inputs()
    output = ROOT / 'data/event_log_22'
    if not (output / '_manifest.json').exists():
        subprocess.run([sys.executable, '/scd2project/generate_event_log.py', '--rows', '5000000',
                        '--users', '100000', '--change-probability', '0.20', '--seed', '42',
                        '--start-time', '2026-01-01T00:00:00Z', '--max-step-seconds', '1',
                        '--user-id-start', '1', '--rows-per-file', '100000', '--output-dir', str(output)], check=True)
    manifest = json.loads((output / '_manifest.json').read_text())
    s3 = s3client()
    for attempt in range(60):
        try:
            buckets = s3.list_buckets()['Buckets']
            break
        except Exception:
            if attempt == 59:
                raise
            time.sleep(1)
    if not any(b['Name'] == BUCKET for b in buckets):
        s3.create_bucket(Bucket=BUCKET)
    for folder in ['event_log_22', 'task1', 'task2', 'task3']:
        for file in sorted((ROOT / 'data' / folder).glob('*')):
            if file.is_file():
                key = f'input/{folder}/{file.name}'
                # Avoid uploading unchanged local input on every rerun.
                try:
                    if s3.head_object(Bucket=BUCKET, Key=key)['ContentLength'] == file.stat().st_size:
                        continue
                except s3.exceptions.ClientError:
                    pass
                s3.upload_file(str(file), BUCKET, key)
        print('Uploaded', folder, flush=True)
    # Source is a JSON object keyed by content id. Normalize to JSON Lines,
    # preserving every catalogue field, then read as a regular Spark table.
    catalogue = json.loads((ROOT / 'data/task2/catalogue.json').read_text())
    normalized = ROOT / 'data/task2/catalogue.jsonl'
    with normalized.open('w') as f:
        for key, value in catalogue.items():
            f.write(json.dumps(dict(element_uid=int(key), **value)) + '\n')
    s3.upload_file(str(normalized), BUCKET, 'input/task2/catalogue.jsonl')
    spark, _ = spark_session('prepare')
    sys.path.insert(0, '/scd2project')
    from scd2 import build_table
    target = S3 + '/input/task1/users_scd2'
    print('Building SCD2 with the HW 2.1 script', flush=True)
    count = build_table(spark, S3 + '/input/event_log_22/*.csv', target)
    assert count == manifest['user_count'] + manifest['applied_profile_changes']
    save('results/scd2_build.json', dict(rows=count, source_rows=5000000, path=target,
                                      script='/scd2project/scd2.py', **manifest))
    # Convert the large source CSV once; all join variants use identical Parquet.
    from pyspark.sql import types as T
    tx_schema = T.StructType([T.StructField(c, T.LongType()) for c in ['user_uid', 'element_uid', 'watched_time']])
    tx = spark.read.schema(tx_schema).option('header', True).option('mode', 'FAILFAST').csv(S3 + '/input/task2/skew_transactions.csv')
    tx.write.mode('overwrite').parquet(S3 + '/input/task2/views')
    spark.read.json(S3 + '/input/task2/catalogue.jsonl').write.mode('overwrite').parquet(S3 + '/input/task2/catalogue')
    spark.stop()
    print('Preparation complete', flush=True)


if __name__ == '__main__':
    main()
