import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F
from upload_data import s3_client

META = ['_dag_id', '_run_id', '_task_id', '_data_interval_start', '_data_interval_end', '_loaded_at']
ATTRIBUTES = ['user_fio', 'user_email', 'user_login']
EVENT_SCHEMA = 'event_time timestamp, user_id bigint, user_fio string, user_email string, user_login string'
CAMPAIGN_SCHEMA = 'campaign_id bigint, email_domain string, send_at timestamp, campaign_name string'
BUCKET = os.environ.get('S3_BUCKET', 'hw23')


def utc(value):
    return datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(timezone.utc)


def session():
    spark = (SparkSession.builder.master(os.environ.get('SPARK_MASTER', 'local[2]'))
             .appName('hw23').config('spark.driver.memory', '1g')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '4')
             .config('spark.sql.adaptive.enabled', 'false')
             .config('spark.ui.enabled', 'false')
             .config('spark.ui.showConsoleProgress', 'false')
             .config('spark.jars', '/opt/airflow/s3-jars/hadoop-aws-3.3.4.jar,'
                     '/opt/airflow/s3-jars/aws-java-sdk-bundle-1.12.262.jar')
             .config('spark.hadoop.fs.s3a.endpoint', os.environ['S3_ENDPOINT'])
             .config('spark.hadoop.fs.s3a.path.style.access', 'true')
             .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
             .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                     'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def bounds(ctx):
    start, end = utc(ctx['start']), utc(ctx['end'])
    assert (end - start).total_seconds() == 86400
    assert start.hour == start.minute == start.second == start.microsecond == 0
    return start.replace(tzinfo=None), end.replace(tzinfo=None)


def result_path(ctx, table):
    field = 'snapshot_date' if table == 'user_scd2' else 'calculation_date'
    return f"s3a://{BUCKET}/results/{ctx.get('dataset', 'demo')}/{table}/{field}={utc(ctx['start']).date()}/"


def source(spark, ctx, name):
    prefix = f"s3a://{BUCKET}/raw/{ctx.get('dataset', 'demo')}/"
    schema = EVENT_SCHEMA if name == 'events' else CAMPAIGN_SCHEMA
    target = 'events/*.csv' if name == 'events' else 'campaigns.csv'
    return (spark.read.schema(schema).option('header', True).option('mode', 'FAILFAST')
            .csv(prefix + target))


def history(events, end):
    events = events.filter(F.col('event_time') < F.lit(end)).dropDuplicates()
    assert events.filter(' OR '.join(f'{c} IS NULL' for c in events.columns)).limit(1).count() == 0
    assert events.groupBy('user_id', 'event_time').count().filter('count > 1').limit(1).count() == 0
    w = Window.partitionBy('user_id').orderBy('event_time')
    state = F.struct(*ATTRIBUTES)
    changes = (events.withColumn('_first', F.row_number().over(w))
               .withColumn('_previous', F.lag(state).over(w))
               .filter((F.col('_first') == 1) | ~state.eqNullSafe(F.col('_previous'))))
    return (changes.withColumn('valid_from', F.col('event_time'))
            .withColumn('valid_to', F.lead('event_time').over(w))
            .withColumn('is_current', F.col('valid_to').isNull())
            .select('user_id', *ATTRIBUTES, 'valid_from', 'valid_to', 'is_current'))


def recipients(profiles, campaigns, start, end):
    c = campaigns.filter((F.col('send_at') >= F.lit(start)) & (F.col('send_at') < F.lit(end))).alias('c')
    p = profiles.alias('p')
    domain = F.lower(F.substring_index(p.user_email, '@', -1))
    condition = ((domain == F.lower(c.email_domain)) & (p.valid_from <= c.send_at)
                 & (p.valid_to.isNull() | (c.send_at < p.valid_to)))
    return (p.join(F.broadcast(c), condition, 'inner')
            .select('c.campaign_id', 'c.campaign_name', 'c.send_at', 'c.email_domain',
                    'p.user_id', 'p.user_fio', 'p.user_email', 'p.user_login',
                    F.col('p.valid_from').alias('user_valid_from'),
                    F.col('p.valid_to').alias('user_valid_to')))


def metadata(df, ctx, date_field):
    start, end = bounds(ctx)
    values = {'_dag_id': ctx['dag_id'], '_run_id': ctx['run_id'], '_task_id': ctx['task_id'],
              '_data_interval_start': start, '_data_interval_end': end,
              '_loaded_at': datetime.now(timezone.utc).replace(tzinfo=None)}
    df = df.withColumn(date_field, F.lit(start.date()))
    for name, value in values.items():
        df = df.withColumn(name, F.lit(value))
    return df


def no_nulls(df, nullable):
    required = [c for c in df.columns if c not in nullable]
    assert df.filter(' OR '.join(f'{c} IS NULL' for c in required)).limit(1).count() == 0


def check_scd2(df, ctx):
    _, end = bounds(ctx)
    no_nulls(df, ['valid_to'])
    assert df.groupBy('user_id', 'valid_from').count().filter('count > 1').limit(1).count() == 0
    assert df.filter((F.col('valid_from') >= F.lit(end)) | (F.col('valid_to') <= F.col('valid_from'))).limit(1).count() == 0
    assert df.filter(F.col('is_current') != F.col('valid_to').isNull()).limit(1).count() == 0
    currents = df.groupBy('user_id').agg(F.sum(F.col('is_current').cast('int')).alias('n'))
    assert currents.filter('n != 1').limit(1).count() == 0
    w = Window.partitionBy('user_id').orderBy('valid_from')
    ordered = (df.withColumn('_next', F.lead('valid_from').over(w))
               .withColumn('_previous', F.lag(F.struct(*ATTRIBUTES)).over(w)))
    assert ordered.filter(~F.col('valid_to').eqNullSafe(F.col('_next'))).limit(1).count() == 0
    assert ordered.filter(F.struct(*ATTRIBUTES).eqNullSafe(F.col('_previous'))).limit(1).count() == 0


def check_recipients(df, ctx):
    start, end = bounds(ctx)
    no_nulls(df, ['user_valid_to'])
    assert df.groupBy('campaign_id', 'user_id').count().filter('count > 1').limit(1).count() == 0
    assert df.filter((F.col('send_at') < F.lit(start)) | (F.col('send_at') >= F.lit(end))).limit(1).count() == 0
    assert df.filter('send_at < user_valid_from OR (user_valid_to IS NOT NULL AND send_at >= user_valid_to)').limit(1).count() == 0
    assert df.filter(F.lower(F.substring_index('user_email', '@', -1)) != F.lower('email_domain')).limit(1).count() == 0


def meta_summary(df, ctx):
    for name in META:
        assert df.filter(F.col(name).isNull()).limit(1).count() == 0
    start, end = bounds(ctx)
    invalid = ((F.col('_run_id') != ctx['run_id']) | (F.col('_dag_id') != ctx['dag_id'])
               | (F.col('_task_id') != ctx['task_id'])
               | (F.col('_data_interval_start') != F.lit(start)) | (F.col('_data_interval_end') != F.lit(end)))
    assert df.filter(invalid).limit(1).count() == 0
    assert df.select('_loaded_at').distinct().count() <= 1
    group = META[:-1]
    return [r.asDict() for r in df.groupBy(group).agg(F.count('*').alias('rows'),
            F.min('_loaded_at').alias('min_loaded_at'), F.max('_loaded_at').alias('max_loaded_at')).collect()]


def save_artifact(ctx, name, value):
    key = f"checks/{ctx.get('dataset', 'demo')}/{utc(ctx['start']).date()}/{ctx['task_id']}/{name}"
    body = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    s3_client().put_object(Bucket=BUCKET, Key=key, Body=body.encode('utf-8'))


def execute(spark, ctx):
    start, end = bounds(ctx)
    if ctx['task_id'] == 'build_user_scd2':
        table, date_field = 'user_scd2', 'snapshot_date'
        df = history(source(spark, ctx, 'events'), end)
        df = metadata(df, ctx, date_field).cache()
        check_scd2(df, ctx)
        extra = dict(users=df.select('user_id').distinct().count())
    else:
        assert ctx['task_id'] == 'build_campaign_recipients'
        table, date_field = 'campaign_recipients', 'calculation_date'
        profiles = spark.read.parquet(result_path(ctx, 'user_scd2'))
        assert profiles.filter(F.col('_run_id') != ctx['run_id']).limit(1).count() == 0
        campaigns = source(spark, ctx, 'campaigns')
        no_nulls(campaigns, [])
        assert campaigns.groupBy('campaign_id').count().filter('count > 1').limit(1).count() == 0
        df = metadata(recipients(profiles, campaigns, start, end), ctx, date_field).cache()
        check_recipients(df, ctx)
        save_artifact(ctx, 'join_plan.txt', df._jdf.queryExecution().toString())
        counts = (campaigns.filter((F.col('send_at') >= F.lit(start)) & (F.col('send_at') < F.lit(end)))
                  .select('campaign_id').join(df.groupBy('campaign_id').count(), 'campaign_id', 'left')
                  .fillna(0, ['count']).orderBy('campaign_id'))
        extra = dict(campaigns=[r.asDict() for r in counts.collect()])
    summary = meta_summary(df, ctx)
    path = result_path(ctx, table)
    # Перезаписываю только партицию текущей даты.
    df.write.mode('overwrite').parquet(path)
    written = spark.read.parquet(path)
    assert written.count() == df.count()
    save_artifact(ctx, 'checks.json', dict(context=ctx, path=path, rows=written.count(),
                  metadata=summary, schema=written.schema.jsonValue(), **extra))
    print(json.dumps(dict(task=ctx['task_id'], path=path, rows=written.count()), default=str), flush=True)
    df.unpersist()
    return path
