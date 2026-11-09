"""Reproducible generation, S3 upload, independent checks, submission evidence."""
import csv
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from scd2 import ATTRIBUTES, SOURCE_SCHEMA, build_history, validate_history

BASE = Path(__file__).resolve().parent
BUCKET = 'hw-2-1'
SOURCE = f's3a://{BUCKET}/source/event_log/*.csv'
TARGET = f's3a://{BUCKET}/target/users_scd2'


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')


def prepare_source():
    rows = random.Random(20261109).randint(5000, 7000)
    out = BASE / 'data/event_log'
    if not (out / '_manifest.json').exists():
        subprocess.run([sys.executable, str(BASE / 'generate_event_log.py'),
                        '--rows', str(rows), '--output-dir', str(out), '--seed', '42'], check=True)
    manifest = json.loads((out / '_manifest.json').read_text(encoding='utf-8'))
    assert manifest['total_rows'] == rows
    s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
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
    for p in sorted(out.glob('*.csv')):
        s3.upload_file(str(p), BUCKET, 'source/event_log/' + p.name)
    s3.upload_file(str(out / '_manifest.json'), BUCKET, 'source/event_log/_manifest.json')
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print('Uploaded source to', SOURCE)
    return manifest, s3


def canonical(rows):
    return [r.asDict() for r in sorted(rows, key=lambda r: (r.user_id, r.valid_from))]


def fingerprint(rows):
    return hashlib.sha256(json.dumps(canonical(rows), sort_keys=True, default=str).encode()).hexdigest()


def reference_history():
    """Independent stdlib implementation: no Spark windows or shared SCD logic."""
    events = []
    for p in sorted((BASE / 'data/event_log').glob('*.csv')):
        with p.open(encoding='utf-8', newline='') as f:
            for r in csv.DictReader(f):
                r['user_id'] = int(r['user_id'])
                r['event_time'] = datetime.fromisoformat(r['event_time'].replace('Z', '+00:00')).replace(tzinfo=None)
                events.append(r)
    states = {}
    history = []
    for event in sorted(events, key=lambda e: (e['user_id'], e['event_time'])):
        uid = event['user_id']
        profile = tuple(event[a] for a in ATTRIBUTES)
        if uid in states and states[uid][0] == profile:
            continue
        if uid in states:
            previous = states[uid][1]
            previous['valid_to'] = event['event_time']
            previous['is_current'] = False
        version = dict(user_id=uid, **{a: event[a] for a in ATTRIBUTES},
                       valid_from=event['event_time'], valid_to=None, is_current=True)
        history.append(version)
        states[uid] = (profile, version)
    return history


def edge_checks(spark):
    t = datetime(2026, 9, 1)
    # Unsorted input, exact duplicate, unchanged event, A -> B -> A,
    # NULL transitions and the maximum uint32 identifier.
    rows = [
        (t + timedelta(seconds=3), 1, 'A', None, 'login_a'),
        (t, 1, 'A', None, 'login_a'),
        (t + timedelta(seconds=1), 1, 'A', None, 'login_a'),
        (t + timedelta(seconds=2), 1, 'A', 'mail', 'login_b'),
        (t, 1, 'A', None, 'login_a'),
        (t, 4294967295, None, None, None),
    ]
    history = build_history(spark.createDataFrame(rows, SOURCE_SCHEMA)).cache()
    validate_history(history)
    result = history.orderBy('user_id', 'valid_from').collect()
    assert len(result) == 4
    assert [r.user_login for r in result if r.user_id == 1] == ['login_a', 'login_b', 'login_a']
    assert result[0].valid_to == t + timedelta(seconds=2)
    assert result[1].valid_to == t + timedelta(seconds=3)
    history.unpersist()
    for bad, expected in [
        ([(t, 1, 'A', None, 'a'), (t, 1, 'B', None, 'b')], 'Conflicting profiles'),
        ([(t, 4294967296, 'A', None, 'a')], 'outside uint32'),
        ([(None, 1, 'A', None, 'a')], 'Invalid timestamp'),
    ]:
        try:
            build_history(spark.createDataFrame(bad, SOURCE_SCHEMA))
        except ValueError as error:
            assert expected in str(error)
        else:
            raise AssertionError('Invalid source must fail')
    print('Edge checks passed: NULL, A→B→A, duplicates, uint32, timestamp conflicts')
    return True


def verify_result(spark, first_rows, manifest):
    history = spark.read.parquet(TARGET).cache()
    validate_history(history)
    rows = history.collect()
    assert canonical(rows) == reference_history(), 'Spark differs from independent reference'
    assert fingerprint(rows) == fingerprint(first_rows), 'Second full build changed the result'
    assert len(rows) == manifest['user_count'] + manifest['applied_profile_changes']
    history.createOrReplaceTempView('users_scd2')
    count_sql = (BASE / 'queries/count.sql').read_text(encoding='utf-8')
    count = spark.sql(count_sql).first().scd2_rows
    print(count_sql, '\nResult:', count)
    report = dict(source_rows=manifest['total_rows'], users=manifest['user_count'],
                  changes=manifest['applied_profile_changes'], scd2_rows=count,
                  snapshot_sha256=fingerprint(rows), rerun_equal=True,
                  independent_reference_equal=True, invariants_passed=True)
    save_json(BASE / 'results/validation.json', report)
    with (BASE / 'results/users_scd2.csv').open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=history.columns)
        writer.writeheader()
        writer.writerows(canonical(rows))
    save_json(BASE / 'results/count_result.json', {'query': count_sql, 'scd2_rows': count})
    return history, report


def lookup_checks(spark, history):
    query = (BASE / 'queries/login_at.sql').read_text(encoding='utf-8')
    rows = history.orderBy('user_id', 'valid_from').collect()
    # Select a user who actually changed login.
    pair = next((a, b) for a, b in zip(rows, rows[1:])
                if a.user_id == b.user_id and a.user_login != b.user_login)
    old, new = pair
    user = [r for r in rows if r.user_id == old.user_id]
    samples = [
        ('before_first', user[0].valid_from - timedelta(microseconds=1), []),
        ('at_first', user[0].valid_from, [user[0].user_login]),
        ('before_change', new.valid_from - timedelta(microseconds=1), [old.user_login]),
        ('at_change', new.valid_from, [new.user_login]),
        ('current', user[-1].valid_from + timedelta(days=1), [user[-1].user_login]),
    ]
    results = []
    for label, when, expected in samples:
        params = {'user_id': old.user_id, 'as_of': when}
        found = [r.user_login for r in spark.sql(query, args=params).collect()]
        assert found == expected, (label, found, expected)
        results.append(dict(case=label, **params, result=found))
        print(label, params, '=>', found)
    assert not spark.sql(query, args={'user_id': 4294967295, 'as_of': samples[-1][1]}).collect()
    save_json(BASE / 'results/lookup_checks.json', results)
    return results


def save_s3_objects(s3):
    objects = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET):
        objects.extend({'key': o['Key'], 'bytes': o['Size']} for o in page.get('Contents', []))
    assert any(o['key'].endswith('.parquet') for o in objects)
    save_json(BASE / 'results/s3_objects.json', objects)
    print('Список объектов S3 сохранён в results/s3_objects.json')
