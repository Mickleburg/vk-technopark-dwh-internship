import hashlib
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from pyspark.sql import functions as F

from pipeline import (BUCKET, META, bounds, check_scd2, check_recipients, execute,
                      meta_summary, result_path, session, utc)
from oracle import expected, inputs
from upload_data import s3_client

ROOT = Path('/opt/airflow/results')
DAG = 'hw23_user_campaigns'
DATES = ['2026-01-23', '2026-01-24', '2026-01-25']
TASKS = ['build_user_scd2', 'build_campaign_recipients']
TABLES = ['user_scd2', 'campaign_recipients']
api_session = requests.Session()
api_session.auth = (os.environ.get('CHECK_AIRFLOW_USER', 'student'), os.environ.get('CHECK_AIRFLOW_PASSWORD', 'student'))
API = 'http://airflow-webserver:8080/api/v1'


def save(name, value):
    path = ROOT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str) + '\n', encoding='utf-8')


def request(method, path, **kwargs):
    response = api_session.request(method, API + path, timeout=30, **kwargs)
    if not response.ok:
        raise RuntimeError(f'{method} {path}: {response.status_code} {response.text}')
    response.raise_for_status()
    return response.json() if response.content else {}


def wait(check, label):
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(3)
    raise TimeoutError(label)


def runs():
    rows = request('GET', f'/dags/{DAG}/dagRuns')['dag_runs']
    return sorted(rows, key=lambda r: r['data_interval_start'])


def completed():
    rows = runs()
    if any(row['state'] == 'failed' for row in rows):
        raise RuntimeError(rows)
    if len(rows) != 3 or any(row['state'] != 'success' for row in rows):
        return None
    result = []
    for day, row in zip(DATES, rows):
        assert row['run_type'] == 'scheduled'
        start, end = utc(row['data_interval_start']), utc(row['data_interval_end'])
        assert str(start.date()) == day and end - start == timedelta(days=1)
        tasks = request('GET', f"/dags/{DAG}/dagRuns/{quote(row['dag_run_id'], safe='')}/taskInstances")['task_instances']
        if len(tasks) != 2 or any(t['state'] != 'success' for t in tasks):
            return None
        result.append(dict(run=row, tasks=tasks))
    return result


def context(day, dataset='demo', run_id=None):
    start = datetime.fromisoformat(day)
    return dict(dag_id=DAG if dataset == 'demo' else 'fixture_checks',
                run_id=run_id or f'scheduled__{day}T00:00:00+00:00', dataset=dataset,
                start=start.isoformat() + '+00:00', end=(start + timedelta(days=1)).isoformat() + '+00:00')


def canonical(rows):
    return sorted(json.dumps(dict(row), sort_keys=True, default=str, ensure_ascii=False) for row in rows)


def list_objects(prefix):
    return [o for page in s3_client().get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix)
            for o in page.get('Contents', [])]


def source_hashes():
    return {o['Key']: hashlib.sha256(s3_client().get_object(Bucket=BUCKET, Key=o['Key'])['Body'].read()).hexdigest()
            for o in list_objects('raw/demo/')}


def snapshot_before():
    for o in list_objects('results/demo/'):
        s3_client().copy_object(Bucket=BUCKET, Key='_verification/before/' + o['Key'],
                                CopySource={'Bucket': BUCKET, 'Key': o['Key']})


def export_objects(prefix, target):
    for o in list_objects(prefix):
        path = ROOT / target / o['Key'][len(prefix):]
        path.parent.mkdir(parents=True, exist_ok=True)
        s3_client().download_file(BUCKET, o['Key'], str(path))


def validate_date(spark, ctx):
    start, end = bounds(ctx)
    wanted = expected(ctx['dataset'], start, end)
    results = {}
    for table, task in zip(TABLES, TASKS):
        current = dict(ctx, task_id=task)
        df = spark.read.parquet(result_path(ctx, table)).cache()
        if table == 'user_scd2':
            check_scd2(df, current)
            assert df.filter(F.col('snapshot_date') != F.lit(start.date())).count() == 0
        else:
            check_recipients(df, current)
            assert df.filter(F.col('calculation_date') != F.lit(start.date())).count() == 0
        business = [c for c in df.columns if c not in META]
        rows = [r.asDict() for r in df.select(business).collect()]
        assert canonical(rows) == canonical(wanted[table]), (ctx, table)
        summary = meta_summary(df, current)
        extra = {}
        if table == 'user_scd2':
            extra['users'] = df.select('user_id').distinct().count()
        else:
            _, campaigns = inputs(ctx['dataset'])
            counts = {r.campaign_id: r['count'] for r in df.groupBy('campaign_id').count().collect()}
            extra['campaigns'] = [dict(campaign_id=c['campaign_id'], recipients=counts.get(c['campaign_id'], 0))
                                  for c in campaigns if start <= c['send_at'] < end]
        results[table] = dict(rows=len(rows), metadata=summary, matches_oracle=True, **extra)
        df.unpersist()
    return results


def verify_main(spark, before, after):
    events, campaigns = inputs('demo')
    assert len(events) == 6000 and len({r['user_id'] for r in events}) == 100
    save('source_stats.json', dict(events=len(events), users=100, campaigns=len(campaigns),
         min_event_time=min(r['event_time'] for r in events), max_event_time=max(r['event_time'] for r in events)))
    summary = {}
    for i, day in enumerate(DATES):
        ctx = context(day, run_id=after[i]['run']['dag_run_id'])
        summary[day] = validate_date(spark, ctx)
        for table in TABLES:
            path = result_path(ctx, table)
            old = spark.read.parquet(path.replace(f's3a://{BUCKET}/', f's3a://{BUCKET}/_verification/before/'))
            new = spark.read.parquet(path)
            columns = [c for c in new.columns if i != 0 or c not in META]
            assert old.select(columns).exceptAll(new.select(columns)).count() == 0
            assert new.select(columns).exceptAll(old.select(columns)).count() == 0
            if i == 0:
                assert old.select('_run_id').distinct().collect() == new.select('_run_id').distinct().collect()
                assert old.select('_loaded_at').first() != new.select('_loaded_at').first()
            else:
                assert canonical(before[i]['tasks']) == canonical(after[i]['tasks'])
    first_before = {t['task_id']: t for t in before[0]['tasks']}
    for task in after[0]['tasks']:
        old = first_before[task['task_id']]
        assert utc(task['start_date']) > utc(old['end_date'])
        assert task['try_number'] > old['try_number']
    save('main_checks.json', dict(dates=summary, first_date_business_equal=True,
         other_dates_all_fields_equal=True, same_run_id=True, both_tasks_reexecuted=True,
         loaded_at_changed=True, except_all_both_directions=True))


def fixture_checks(spark):
    summary = {}
    days = ['2026-01-21', *DATES, '2026-01-27', '2026-01-28']
    selected = {}
    for day in days:
        ctx = context(day, 'fixtures', run_id='fixture_' + day)
        for task in TASKS:
            execute(spark, dict(ctx, task_id=task))
        summary[day] = validate_date(spark, ctx)
        selected[day] = [r.asDict() for r in spark.read.parquet(result_path(ctx, 'campaign_recipients')).collect()]
    day23 = selected['2026-01-23']
    assert {r['user_id'] for r in day23 if r['campaign_id'] == 101} == {1001}
    assert {r['user_id'] for r in day23 if r['campaign_id'] == 102} == {1002}
    assert {r['user_id'] for r in day23 if r['campaign_id'] == 103} == {1001}
    assert not [r for r in day23 if r['campaign_id'] == 104]
    day24 = selected['2026-01-24']
    assert {r['user_id'] for r in day24 if r['campaign_id'] == 105} == {1001, 1002}
    assert {r['user_id'] for r in day24 if r['campaign_id'] == 106} == {1003}
    assert not [r for r in day24 if r['campaign_id'] == 107]
    day25 = selected['2026-01-25']
    assert next(r for r in day25 if r['campaign_id'] == 115)['user_login'] == 'alice_new'
    assert next(r for r in day25 if r['campaign_id'] == 116)['user_login'] == 'alice'
    assert not selected['2026-01-21'] and not selected['2026-01-27'] and not selected['2026-01-28']
    history = spark.read.parquet(result_path(context('2026-01-25', 'fixtures'), 'user_scd2'))
    assert history.filter('user_id=1001').count() == 5
    assert history.filter('user_id=1005').count() == 0
    # Подкладываю старую строку и проверяю, что пустой пересчёт её удалит.
    ctx = context('2026-01-27', 'fixtures', run_id='fixture_2026-01-27')
    nonempty = spark.read.parquet(result_path(context('2026-01-25', 'fixtures'), 'campaign_recipients'))
    nonempty.limit(1).write.mode('overwrite').parquet(result_path(ctx, 'campaign_recipients'))
    execute(spark, dict(ctx, task_id=TASKS[1]))
    assert spark.read.parquet(result_path(ctx, 'campaign_recipients')).count() == 0
    summary['2026-01-27'] = validate_date(spark, ctx)
    save('fixture_checks.json', dict(dates=summary, boundary_and_domain_cases=True,
         before_first_profile_excluded=True, unchanged_events_do_not_add_versions=True,
         profile_return_preserved=True, future_events_excluded=True, original_case_preserved=True,
         empty_parquet_readable=True, empty_overwrite_removes_old_rows=True, examples=selected))


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    def ready():
        try:
            response = api_session.get(API + '/dags/' + DAG, timeout=10)
            if response.status_code in (404, 502, 503):
                return False
            response.raise_for_status()
            return True
        except requests.ConnectionError:
            return False
    wait(ready, 'Airflow DAG registration')
    request('PATCH', '/dags/' + DAG, json={'is_paused': False})
    initial = wait(completed, 'three scheduled runs')
    before_file = ROOT / 'airflow_before_repeat.json'
    if not before_file.exists():
        save('source_hashes.json', source_hashes())
        snapshot_before()
        export_objects('_verification/before/results/demo/', 'tables/before_repeat/')
        save('airflow_before_repeat.json', initial)
    before = json.loads(before_file.read_text())
    if not (ROOT / 'repeat_requested.json').exists():
        response = request('POST', f'/dags/{DAG}/clearTaskInstances', json=dict(
            dag_run_id=before[0]['run']['dag_run_id'], task_ids=TASKS, dry_run=False,
            reset_dag_runs=True, only_failed=False, only_running=False))
        save('repeat_requested.json', response)
    after = wait(completed, 'first scheduled run repeated')
    save('airflow_after_repeat.json', after)
    assert source_hashes() == json.loads((ROOT / 'source_hashes.json').read_text())
    spark = session()
    try:
        verify_main(spark, before, after)
        fixture_checks(spark)
    finally:
        spark.stop()
    export_objects('results/demo/', 'tables/after_repeat/')
    export_objects('results/fixtures/', 'tables/fixtures/')
    export_objects('checks/', 'task_checks/')
    save('complete.json', dict(success=True, scheduled_runs=3, tasks_per_run=2,
         first_run_repeated=True, source_unchanged=True, independent_oracle_equal=True))
    print('All homework checks passed', flush=True)


if __name__ == '__main__':
    main()
