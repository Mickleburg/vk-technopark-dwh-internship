"""Exercise real Spark -> S3A -> Parquet -> Spark, then remove its own probe."""
import os
from uuid import uuid4

from pyspark.sql import SparkSession
from upload_data import s3_client


def create_probe_session(name):
    jars = '/opt/airflow/s3-jars/'
    endpoint = os.environ['S3_ENDPOINT']
    spark = (SparkSession.builder.appName(name)
            .master(os.environ.get('SPARK_MASTER', 'local[2]'))
            .config('spark.driver.memory', '1g')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.sql.shuffle.partitions', '4')
            .config('spark.ui.enabled', 'false')
            .config('spark.jars', jars + 'hadoop-aws-3.3.4.jar,' + jars + 'aws-java-sdk-bundle-1.12.262.jar')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.endpoint', endpoint)
            .config('spark.hadoop.fs.s3a.endpoint.region', 'us-east-1')
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(endpoint.startswith('https://')).lower())
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
            .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark

def main():
    client = s3_client()
    bucket = os.environ.get('S3_BUCKET', 'hw23')
    client.head_bucket(Bucket=bucket)
    for dataset in ('demo', 'fixtures'):
        client.head_object(Bucket=bucket, Key=f'raw/{dataset}/campaigns.csv')
    prefix = f'_checks/{uuid4().hex}/'
    path = f's3a://{bucket}/{prefix}'
    spark = create_probe_session('hw23-environment-check')
    try:
        hadoop = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        spark.range(3).write.mode('overwrite').parquet(path)
        values = [row.id for row in spark.read.parquet(path).orderBy('id').collect()]
        if values != [0, 1, 2]:
            raise AssertionError(values)
        print(f'OK: Spark {spark.version}, Hadoop {hadoop}, S3A write/read, input data', flush=True)
    finally:
        spark.stop()
        # Delete only this invocation's unique probe prefix.
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
            objects = [{'Key': item['Key']} for item in page.get('Contents', [])]
            if objects:
                client.delete_objects(Bucket=bucket, Delete={'Objects': objects})


if __name__ == '__main__':
    main()
