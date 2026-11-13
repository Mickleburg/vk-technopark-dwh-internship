from pyspark.sql import functions as F
from common import S3, save, spark_session, configure, benchmark, finalize_metrics, bag_equal


def main():
    spark, defaults = spark_session('task1')
    schema = 'campaign_id INT, email_domain STRING, send_at TIMESTAMP, campaign_name STRING'
    campaigns = spark.read.schema(schema).option('header', True).csv(S3 + '/input/task1/campaigns.csv')
    assert campaigns.count() == 25
    assert campaigns.select('campaign_id').distinct().count() == 25
    history = spark.read.parquet(S3 + '/input/task1/users_scd2').withColumn('profile_domain', F.lower(F.element_at(F.split('user_email', '@'), -1)))
    def query(variant):
        c = campaigns
        if variant == 'merge': c = c.hint('MERGE')
        elif variant == 'broadcast': c = F.broadcast(c)
        elif variant == 'shuffle_hash': c = c.hint('SHUFFLE_HASH')
        h, c = history.alias('h'), c.alias('c')
        condition = ((h.profile_domain == c.email_domain) & (h.valid_from <= c.send_at)
                     & (h.valid_to.isNull() | (c.send_at < h.valid_to)))
        return h.join(c, condition, 'inner').select('campaign_id', 'campaign_name', 'send_at', 'email_domain',
                                                   'user_id', 'user_fio', 'user_email', 'user_login')
    builders = {name: (lambda n=name: query(n)) for name in ['auto', 'merge', 'broadcast', 'shuffle_hash']}
    timings = benchmark(spark, 'task1', builders, defaults, repeats=5)
    configure(spark, defaults, automatic=False)
    reference = spark.read.parquet(S3 + '/results/task1/auto')
    count = reference.count()
    assert reference.groupBy('campaign_id', 'user_id').count().filter('count > 1').limit(1).count() == 0
    equal = {}
    for name in builders:
        candidate = spark.read.parquet(f'{S3}/results/task1/{name}')
        bag_equal(reference, candidate)
        equal[name] = dict(rows=candidate.count(), except_all_both_directions=True)
    domains = [r.asDict() for r in history.groupBy('profile_domain').count().orderBy(F.desc('count')).collect()]
    save('results/task1/validation.json', dict(rows=count, no_duplicate_campaign_user=True, equality=equal, domains=domains))
    save('results/task1/examples.json', [r.asDict() for r in reference.orderBy('campaign_id', 'user_id').limit(10).collect()])
    summary = finalize_metrics(spark, 'task1', timings)
    print(summary, flush=True)


if __name__ == '__main__':
    main()
