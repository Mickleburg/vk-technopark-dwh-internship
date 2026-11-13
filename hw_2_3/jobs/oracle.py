"""Контрольный расчёт без Spark."""
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)


def inputs(dataset):
    root = Path('/opt/airflow/data') / dataset
    events = []
    for path in sorted((root / 'events').glob('*.csv')):
        with path.open(encoding='utf-8', newline='') as stream:
            events.extend(dict(row, user_id=int(row['user_id']), event_time=stamp(row['event_time']))
                          for row in csv.DictReader(stream))
    with (root / 'campaigns.csv').open(encoding='utf-8', newline='') as stream:
        campaigns = [dict(row, campaign_id=int(row['campaign_id']), send_at=stamp(row['send_at']))
                     for row in csv.DictReader(stream)]
    return events, campaigns


def expected(dataset, start, end):
    events, campaigns = inputs(dataset)
    users = defaultdict(list)
    for row in sorted(events, key=lambda r: (r['user_id'], r['event_time'])):
        if row['event_time'] >= end:
            continue
        versions = users[row['user_id']]
        state = {k: row[k] for k in ['user_id', 'user_fio', 'user_email', 'user_login']}
        if versions and all(versions[-1][k] == state[k] for k in state):
            continue
        if versions:
            versions[-1].update(valid_to=row['event_time'], is_current=False)
        versions.append(dict(state, valid_from=row['event_time'], valid_to=None,
                             is_current=True, snapshot_date=start.date()))
    profiles = [row for versions in users.values() for row in versions]
    recipients = []
    for c in campaigns:
        if not start <= c['send_at'] < end:
            continue
        for p in profiles:
            if (p['user_email'].rsplit('@', 1)[1].lower() == c['email_domain'].lower()
                    and p['valid_from'] <= c['send_at']
                    and (p['valid_to'] is None or c['send_at'] < p['valid_to'])):
                recipients.append(dict(c, **{k: p[k] for k in ['user_id', 'user_fio', 'user_email', 'user_login']},
                                       user_valid_from=p['valid_from'], user_valid_to=p['valid_to'],
                                       calculation_date=start.date()))
    return dict(user_scd2=profiles, campaign_recipients=recipients)
