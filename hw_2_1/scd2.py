"""Full-snapshot SCD2 build in Spark; source and destination live in S3."""
import argparse
import os

from pyspark.sql import SparkSession, Window, functions as F, types as T

ATTRIBUTES = ['user_fio', 'user_email', 'user_login']
SOURCE_SCHEMA = T.StructType([
    T.StructField('event_time', T.TimestampType()),
    # Spark has no uint32: LongType covers the complete unsigned range.
    T.StructField('user_id', T.LongType()),
    *[T.StructField(name, T.StringType()) for name in ATTRIBUTES],
])


def make_spark():
    return (SparkSession.builder.master('local[2]').appName('hw-2-1-scd2')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.sql.shuffle.partitions', '4')
            .config('spark.hadoop.fs.s3a.endpoint', os.environ['S3_ENDPOINT'])
            .config('spark.hadoop.fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
            .config('spark.hadoop.fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .getOrCreate())


def read_events(spark, source):
    return (spark.read.schema(SOURCE_SCHEMA).option('header', True)
            .option('mode', 'FAILFAST').csv(source))


def build_history(events):
    invalid = events.filter(
        F.col('event_time').isNull() | F.col('user_id').isNull()
        | (F.col('user_id') < 0) | (F.col('user_id') > 4294967295))
    if invalid.limit(1).count():
        raise ValueError('Invalid timestamp or user_id outside uint32')
    # Exact duplicate events are harmless. Conflicting states at one timestamp
    # have no defined ordering in the task: fail instead of inventing a winner.
    unique = events.dropDuplicates(['user_id', 'event_time', *ATTRIBUTES])
    conflicts = unique.groupBy('user_id', 'event_time').count().filter('count > 1')
    if conflicts.limit(1).count():
        raise ValueError('Conflicting profiles for the same user_id and event_time')
    window = Window.partitionBy('user_id').orderBy('event_time')
    profile = F.struct(*[F.col(name) for name in ATTRIBUTES])
    marked = (unique.withColumn('_position', F.row_number().over(window))
              .withColumn('_previous', F.lag(profile).over(window)))
    # Compare adjacent events, not all historical values: A -> B -> A is 3 versions.
    changes = marked.filter((F.col('_position') == 1) | ~profile.eqNullSafe(F.col('_previous')))
    # lead() must run AFTER removing unchanged events.
    return (changes.withColumn('valid_from', F.col('event_time'))
            .withColumn('valid_to', F.lead('event_time').over(window))
            .withColumn('is_current', F.col('valid_to').isNull())
            .select('user_id', *ATTRIBUTES, 'valid_from', 'valid_to', 'is_current'))


def validate_history(history):
    current = history.groupBy('user_id').agg(F.sum(F.col('is_current').cast('int')).alias('n'))
    assert current.filter('n != 1').count() == 0, 'Expected exactly one current version'
    assert history.filter('is_current != (valid_to IS NULL)').count() == 0
    assert history.filter('valid_to <= valid_from').count() == 0
    assert history.groupBy('user_id', 'valid_from').count().filter('count > 1').count() == 0
    w = Window.partitionBy('user_id').orderBy('valid_from')
    boundaries = history.withColumn('_next', F.lead('valid_from').over(w))
    assert boundaries.filter(~F.col('valid_to').eqNullSafe(F.col('_next'))).count() == 0
    adjacent = history.withColumn('_previous', F.lag(F.struct(*ATTRIBUTES)).over(w))
    assert adjacent.filter(F.struct(*ATTRIBUTES).eqNullSafe(F.col('_previous'))).count() == 0


def build_table(spark, source, target):
    if source.rstrip('/') == target.rstrip('/'):
        raise ValueError('Source and target must differ')
    history = build_history(read_events(spark, source)).cache()
    try:
        validate_history(history)
        count = history.count()
        # Full rebuild, not append: rerunning on identical input creates no duplicates.
        history.orderBy('user_id', 'valid_from').write.mode('overwrite').parquet(target)
        return count
    finally:
        history.unpersist()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='s3a://hw-2-1/source/event_log/*.csv')
    parser.add_argument('--target', default='s3a://hw-2-1/target/users_scd2')
    args = parser.parse_args()
    spark = make_spark()
    spark.sparkContext.setLogLevel('WARN')
    try:
        print('SCD2 rows:', build_table(spark, args.source, args.target))
    finally:
        spark.stop()
