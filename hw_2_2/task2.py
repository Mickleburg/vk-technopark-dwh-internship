from pyspark.sql import functions as F
from common import S3, save, spark_session, configure, benchmark, finalize_metrics, bag_equal


def main():
    spark, defaults = spark_session('task2')
    configure(spark, defaults)
    views = spark.read.parquet(S3 + '/input/task2/views')
    catalogue = spark.read.parquet(S3 + '/input/task2/catalogue')
    count = views.count()
    cat_count = catalogue.count()
    nulls = views.filter('element_uid IS NULL').count()
    cat_nulls = catalogue.filter('element_uid IS NULL').count()
    cat_duplicates = catalogue.groupBy('element_uid').count().filter('count > 1').count()
    assert cat_nulls == cat_duplicates == 0
    frequencies = views.groupBy('element_uid').count().orderBy(F.desc('count'))
    top = [dict(r.asDict(), share=r['count'] / count) for r in frequencies.limit(20).collect()]
    hot = [r.element_uid for r in frequencies.filter(F.col('count') >= count * 0.01).collect() if r.element_uid is not None]
    salts = 16
    # Оцениваю объём данных каталога; hash-таблица JVM будет занимать больше.
    cat_json_bytes = catalogue.select(F.length(F.to_json(F.struct(*catalogue.columns))).alias('n')).agg(F.sum('n')).first()[0]
    missing = views.join(catalogue.select('element_uid'), 'element_uid', 'left_anti').count()
    fact = views.withColumn('_salt', F.when(F.col('element_uid').isin(hot),
                       F.pmod(F.xxhash64('user_uid', 'watched_time', F.monotonically_increasing_id()), F.lit(salts))).otherwise(F.lit(0)))
    dimension = catalogue.withColumn('_salt', F.explode(F.when(F.col('element_uid').isin(hot),
                         F.sequence(F.lit(0), F.lit(salts - 1))).otherwise(F.array(F.lit(0)))))
    result_columns = ['element_uid', 'user_uid', 'watched_time'] + [c for c in catalogue.columns if c != 'element_uid']
    def baseline():
        return views.join(catalogue.hint('MERGE'), 'element_uid', 'inner').select(result_columns)
    def broadcast():
        return views.join(F.broadcast(catalogue), 'element_uid', 'inner').select(result_columns)
    def salted():
        return fact.join(dimension.hint('MERGE'), ['element_uid', '_salt'], 'inner').select(result_columns)
    diagnostics = dict(view_rows=count, catalogue_rows=cat_count, null_view_keys=nulls,
                       null_catalogue_keys=cat_nulls, duplicate_catalogue_keys=cat_duplicates,
                       unmatched_view_rows=missing, top_keys=top, hot_keys=hot,
                       hot_threshold_share=0.01, salt_buckets=salts,
                       catalogue_json_payload_bytes=cat_json_bytes,
                       salted_catalogue_rows=dimension.count(),
                       salt_policy='Hot keys >=1%; each fact row gets one salt; matching dimension row is replicated to all 16 salts')
    save('results/task2/diagnostics.json', diagnostics)
    print(diagnostics, flush=True)
    timings = benchmark(spark, 'task2', {'baseline': baseline, 'broadcast': broadcast, 'salt': salted}, defaults, repeats=3)
    configure(spark, defaults)
    reference = spark.read.parquet(S3 + '/results/task2/baseline')
    equality = {}
    # Bag comparison is done outside the benchmark, on complete S3 results.
    for name in ['broadcast', 'salt']:
        bag_equal(reference, spark.read.parquet(S3 + '/results/task2/' + name))
        equality[name] = True
    rows = reference.count()
    assert rows == count - missing
    save('results/task2/validation.json', dict(rows=rows, full_bag_equality=equality,
                                             expected_rows=count - missing, all_columns=result_columns))
    print(finalize_metrics(spark, 'task2', timings), flush=True)


if __name__ == '__main__':
    main()
