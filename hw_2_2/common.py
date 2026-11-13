import csv
import json
import os
import re
import statistics
import time
from pathlib import Path

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession, functions as F

ROOT = Path('/work')
BUCKET = 'hw-2-2'
S3 = 's3a://' + BUCKET


def save(path, value):
    path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')


def s3client():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))


def spark_session(task):
    events = ROOT / 'results/events'
    events.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master('local[4]').appName('hw-2-2-' + task)
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.eventLog.enabled', 'true')
             .config('spark.eventLog.dir', str(events))
             .config('spark.eventLog.compress', 'false')
             .config('spark.hadoop.fs.s3a.endpoint', os.environ['S3_ENDPOINT'])
             .config('spark.hadoop.fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
             .config('spark.hadoop.fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
             .config('spark.hadoop.fs.s3a.path.style.access', 'true')
             .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
             .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    defaults = {k: spark.conf.get(k) for k in ['spark.sql.adaptive.enabled', 'spark.sql.autoBroadcastJoinThreshold', 'spark.sql.shuffle.partitions']}
    save(f'results/{task}/environment.json', {
        'spark': spark.version, 'master': spark.sparkContext.master,
        'driver_max_heap_bytes': spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
        'cpu_task_slots': 4, 'executors': 'local driver/executor in one JVM',
        'visible_logical_cpus': os.cpu_count(),
        'executor_memory_setting': spark.sparkContext.getConf().get('spark.executor.memory', '1g (default; local mode shares driver heap)'),
        'default_settings': defaults, 'cache': 'no DataFrame cache for any benchmark variant',
        'write': 'Parquet, snappy, overwrite, no coalesce; S3',
    })
    return spark, defaults


def configure(spark, defaults, automatic=False):
    for key, value in defaults.items():
        spark.conf.set(key, value)
    if not automatic:
        spark.conf.set('spark.sql.adaptive.enabled', 'false')
        spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
        spark.conf.set('spark.sql.shuffle.partitions', '16')


def bag_equal(left, right):
    # exceptAll preserves multiplicity; both directions, all result columns.
    cols = left.columns
    assert cols == right.columns
    assert left.exceptAll(right.select(cols)).limit(1).count() == 0
    assert right.select(cols).exceptAll(left).limit(1).count() == 0


def objects(prefix):
    return [o for page in s3client().get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix)
            for o in page.get('Contents', []) if o['Key'].endswith('.parquet')]


def benchmark(spark, task, builders, defaults, repeats):
    names = list(builders)
    rows = []
    for round_no in range(-1, repeats):
        shift = max(round_no, 0) % len(names)
        for name in names[shift:] + names[:shift]:
            configure(spark, defaults, automatic=(name == 'auto'))
            df = builders[name]()
            label = f'{task}:{name}:{round_no}'
            spark.sparkContext.setJobGroup(label, label)
            target = f'{S3}/results/{task}/{name}'
            start = time.perf_counter()
            df.write.mode('overwrite').parquet(target)
            seconds = time.perf_counter() - start
            spark.sparkContext.setJobGroup('outside', 'outside timer')
            print(label, f'{seconds:.3f}s', flush=True)
            if round_no >= 0:
                rows.append(dict(label=label, variant=name, iteration=round_no, seconds=seconds,
                                 files=len(objects(f'results/{task}/{name}/')), output_path=target))
    return rows


def finalize_metrics(spark, task, timings):
    app_id = spark.sparkContext.applicationId
    spark.stop()  # Flush the event log, including final AQE updates for writes.
    log = ROOT / 'results/events' / app_id
    stage_labels, executions, plans, adaptive, tasks = {}, {}, {}, {}, []
    with log.open(encoding='utf-8') as stream:
        for line in stream:
            e = json.loads(line)
            kind = e['Event']
            if kind == 'SparkListenerJobStart':
                props = e.get('Properties') or {}
                label = props.get('spark.jobGroup.id', 'outside')
                for sid in e['Stage IDs']:
                    stage_labels.setdefault(sid, set()).add(label)
                if props.get('spark.sql.execution.id'):
                    executions[int(props['spark.sql.execution.id'])] = label
            elif kind.endswith('SparkListenerSQLExecutionStart'):
                plans[e['executionId']] = e.get('physicalPlanDescription', '')
            elif kind.endswith('SparkListenerSQLAdaptiveExecutionUpdate'):
                adaptive[e['executionId']] = e.get('physicalPlanDescription', '')
            elif kind == 'SparkListenerTaskEnd':
                info, m = e['Task Info'], e.get('Task Metrics', {})
                if e.get('Task End Reason', {}).get('Reason') == 'Success':
                    sr, sw = m.get('Shuffle Read Metrics', {}), m.get('Shuffle Write Metrics', {})
                    tasks.append(dict(stage=e['Stage ID'], task_index=info['Index'],
                                      duration_ms=info['Finish Time'] - info['Launch Time'],
                                      shuffle_read_bytes=sr.get('Remote Bytes Read', 0) + sr.get('Local Bytes Read', 0),
                                      shuffle_write_bytes=sw.get('Shuffle Bytes Written', 0),
                                      shuffle_read_records=sr.get('Total Records Read', 0),
                                      memory_spill_bytes=m.get('Memory Bytes Spilled', 0),
                                      disk_spill_bytes=m.get('Disk Bytes Spilled', 0),
                                      input_records=m.get('Input Metrics', {}).get('Records Read', 0)))
    summaries = []
    for row in timings:
        label = row['label']
        selected = [t for t in tasks if label in stage_labels.get(t['stage'], set())]
        totals = {key: sum(t[key] for t in selected) for key in
                  ['shuffle_read_bytes', 'shuffle_write_bytes', 'memory_spill_bytes', 'disk_spill_bytes']}
        row.update(totals)
        row['task_duration_ms_median'] = statistics.median(t['duration_ms'] for t in selected) if selected else 0
        row['task_duration_ms_max'] = max((t['duration_ms'] for t in selected), default=0)
        path = ROOT / f'results/{task}/plans/{row["variant"]}_{row["iteration"]}.txt'
        path.parent.mkdir(parents=True, exist_ok=True)
        relevant = [eid for eid, lab in executions.items() if lab == label and eid in plans]
        path.write_text('\n\n'.join(f'Execution {eid}\nINITIAL\n{plans[eid]}\nFINAL\n{adaptive.get(eid, plans[eid])}' for eid in relevant), encoding='utf-8')
    for name in sorted({r['variant'] for r in timings}):
        rs = [r for r in timings if r['variant'] == name]
        summary = dict(variant=name, runs=len(rs), seconds_median=statistics.median(r['seconds'] for r in rs),
                       seconds_min=min(r['seconds'] for r in rs), seconds_max=max(r['seconds'] for r in rs),
                       files=rs[-1]['files'])
        for key in ['shuffle_read_bytes', 'shuffle_write_bytes', 'memory_spill_bytes', 'disk_spill_bytes',
                    'task_duration_ms_median', 'task_duration_ms_max']:
            summary[key] = statistics.median(r[key] for r in rs)
        summaries.append(summary)
    save(f'results/{task}/summary.json', summaries)
    save(f'results/{task}/tasks.json', [dict(t, labels=sorted(stage_labels.get(t['stage'], []))) for t in tasks
                                        if any(l.startswith(task + ':') for l in stage_labels.get(t['stage'], []))])
    stages = []
    for sid, labels in stage_labels.items():
        if not any(label.startswith(task + ':') for label in labels):
            continue
        selected = [t for t in tasks if t['stage'] == sid]
        if not selected:
            continue
        counts = [t['shuffle_read_records'] for t in selected]
        durations = [t['duration_ms'] for t in selected]
        stages.append(dict(stage=sid, labels=sorted(labels), tasks=len(selected),
                           duration_ms_median=statistics.median(durations), duration_ms_max=max(durations),
                           shuffle_read_records_median=statistics.median(counts), shuffle_read_records_max=max(counts),
                           shuffle_read_records_sum=sum(counts)))
    save(f'results/{task}/stages.json', stages)
    with (ROOT / f'results/{task}/measurements.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(timings[0]))
        writer.writeheader()
        writer.writerows(timings)
    expected = ({'auto': 'BroadcastHashJoin', 'merge': 'SortMergeJoin',
                 'broadcast': 'BroadcastHashJoin', 'shuffle_hash': 'ShuffledHashJoin'} if task == 'task1'
                else {'baseline': 'SortMergeJoin', 'broadcast': 'BroadcastHashJoin', 'salt': 'SortMergeJoin'})
    checks = []
    for row in timings:
        name = f"{row['variant']}_{row['iteration']}.txt"
        final = (ROOT / f'results/{task}/plans' / name).read_text(encoding='utf-8').split('\nFINAL\n')[-1]
        operators = sorted(set(re.findall(r'BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin', final)))
        assert expected[row['variant']] in operators, name
        if row['variant'] == 'auto':
            assert 'isFinalPlan=true' in final, name
        checks.append(dict(plan=name, variant=row['variant'], operators=operators,
                           aqe=row['variant']=='auto', final_aqe_confirmed='isFinalPlan=true' in final))
    save(f'results/{task}/plan_checks.json', checks)
    return summaries
