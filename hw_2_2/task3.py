import csv
from collections import defaultdict

from pyspark.sql import Window, functions as F
from common import ROOT, S3, save, spark_session

RATINGS = [8, 9, 10]
FEATURES = [f'feat_{agg}_watched_time_rating_{rating}' for rating in RATINGS for agg in ['min', 'max']]


def save_examples(rows):
    categories = {}
    for row in rows:
        present = sum(row[f'feat_min_watched_time_rating_{r}'] is not None for r in RATINGS)
        categories.setdefault(present, row)
    examples = [dict(ratings_present=n, **row) for n, row in sorted(categories.items())]
    save('results/task3/examples.json', examples)
    return examples


def features(views, ratings, users):
    ids = users.select('user_uid').dropDuplicates()
    # All viewing fragments of one content/user pair contribute to total duration.
    watched = (views.join(F.broadcast(ids), 'user_uid', 'left_semi')
               .groupBy('user_uid', 'element_uid').agg(F.sum('watched_time').alias('duration')))
    scoped = ratings.join(F.broadcast(ids), 'user_uid', 'left_semi')
    # If a user updates a rating, select the latest before filtering 8/9/10.
    unique = scoped.dropDuplicates()
    conflicts = unique.groupBy('user_uid', 'element_uid', 'ts').agg(F.countDistinct('rating').alias('n')).filter('n > 1')
    assert conflicts.count() == 0, 'Conflicting ratings at the same timestamp'
    latest = (unique.withColumn('_n', F.row_number().over(Window.partitionBy('user_uid', 'element_uid').orderBy(F.desc('ts'))))
              .filter('_n = 1').filter(F.col('rating').isin(RATINGS)))
    matched = watched.join(latest.select('user_uid', 'element_uid', 'rating'), ['user_uid', 'element_uid'], 'inner')
    aggs = [func(F.when(F.col('rating') == r, F.col('duration'))).alias(f'feat_{name}_watched_time_rating_{r}')
            for r in RATINGS for name, func in [('min', F.min), ('max', F.max)]]
    return ids.join(matched.groupBy('user_uid').agg(*aggs), 'user_uid', 'left').select('user_uid', *FEATURES)


def independent_reference():
    folder = ROOT / 'data/task3'
    ids = {int(r['user_uid']) for r in csv.DictReader((folder / 'ids.csv').open())}
    watched = defaultdict(int)
    for row in csv.DictReader((folder / 'cut_transactions.csv').open()):
        uid = int(row['user_uid'])
        if uid in ids:
            watched[(uid, int(row['element_uid']))] += int(row['watched_time'])
    latest = {}
    for row in csv.DictReader((folder / 'cut_ratings.csv').open()):
        key = (int(row['user_uid']), int(row['element_uid']))
        if key[0] in ids:
            if key not in latest or float(row['ts']) > latest[key][0]:
                latest[key] = (float(row['ts']), int(row['rating']))
    matched = defaultdict(list)
    for key, duration in watched.items():
        if key in latest and latest[key][1] in RATINGS:
            matched[(key[0], latest[key][1])].append(duration)
    result = []
    for uid in sorted(ids):
        row = {'user_uid': uid}
        for rating in RATINGS:
            ds = matched[(uid, rating)]
            row[f'feat_min_watched_time_rating_{rating}'] = min(ds) if ds else None
            row[f'feat_max_watched_time_rating_{rating}'] = max(ds) if ds else None
        result.append(row)
    return result


def main():
    spark, _ = spark_session('task3')
    views = spark.read.schema('element_uid BIGINT, user_uid BIGINT, watched_time BIGINT').option('header', True).csv(S3 + '/input/task3/cut_transactions.csv')
    ratings = spark.read.schema('user_uid BIGINT, element_uid BIGINT, rating INT, ts DOUBLE').option('header', True).csv(S3 + '/input/task3/cut_ratings.csv')
    users = spark.read.schema('user_uid BIGINT').option('header', True).csv(S3 + '/input/task3/ids.csv')
    diagnostics = {}
    for name, df in [('users', users), ('views', views), ('ratings', ratings)]:
        keys = ['user_uid'] if name == 'users' else ['user_uid', 'element_uid']
        diagnostics[name] = dict(rows=df.count(), unique_keys=df.select(keys).distinct().count(),
                                 duplicate_rows=df.count() - df.distinct().count(),
                                 null_keys=df.filter(F.expr(' OR '.join(f'{k} IS NULL' for k in keys))).count())
        assert diagnostics[name]['null_keys'] == 0
    assert views.filter('watched_time IS NULL OR watched_time < 0').count() == 0
    assert ratings.filter('rating IS NULL OR ts IS NULL').count() == 0
    result = features(views, ratings, users)
    result.write.mode('overwrite').parquet(S3 + '/results/task3/user_features')
    rows = [r.asDict() for r in spark.read.parquet(S3 + '/results/task3/user_features').orderBy('user_uid').collect()]
    assert rows == independent_reference()
    assert len(rows) == users.select('user_uid').distinct().count()
    assert len(rows) == len({r['user_uid'] for r in rows})
    for row in rows:
        for rating in RATINGS:
            lo, hi = row[f'feat_min_watched_time_rating_{rating}'], row[f'feat_max_watched_time_rating_{rating}']
            assert (lo is None) == (hi is None)
            assert lo is None or lo <= hi
    # A small independent fixture guarantees all edge cases even if absent in sample.
    u = spark.createDataFrame([(1,), (2,), (3,), (4,)], 'user_uid BIGINT')
    v = spark.createDataFrame([(1, 10, 2), (1, 10, 3), (2, 20, 0), (2, 21, 9)], 'user_uid BIGINT, element_uid BIGINT, watched_time BIGINT')
    r = spark.createDataFrame([(1, 10, 10, 1.), (2, 20, 8, 1.), (2, 21, 9, 1.), (2, 21, 9, 1.), (4, 99, 10, 1.)], 'user_uid BIGINT, element_uid BIGINT, rating INT, ts DOUBLE')
    edge = {x.user_uid: x.asDict() for x in features(v, r, u).collect()}
    assert edge[1]['feat_min_watched_time_rating_10'] == edge[1]['feat_max_watched_time_rating_10'] == 5
    assert edge[2]['feat_min_watched_time_rating_8'] == 0 and edge[2]['feat_min_watched_time_rating_9'] == 9
    assert all(edge[uid][c] is None for uid in [3, 4] for c in FEATURES)
    save('results/task3/diagnostics.json', diagnostics)
    save('results/task3/validation.json', dict(users=len(rows), unique_users=True, full_sample_preserved=True,
                                            independent_reference_equal=True, edge_cases_passed=True,
                                            nonnull_min_le_max=True, fixture=list(edge.values()),
                                            feature_columns=FEATURES,
                                            duration_policy='sum watched_time by (user_uid, element_uid); source units'))
    save_examples(rows)
    with (ROOT / 'results/task3/features.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['user_uid', *FEATURES])
        writer.writeheader(); writer.writerows(rows)
    print(diagnostics, flush=True)
    print('Features saved for', len(rows), 'users', flush=True)
    spark.stop()


if __name__ == '__main__':
    main()
