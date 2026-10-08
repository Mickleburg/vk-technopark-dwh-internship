База данных: `hw`; источник для задания 4: `homework_data.push_events_source`.
Кластер: `homework_cluster`, 2 шарда × 2 реплики (`ch11`, `ch12`; `ch21`, `ch22`).


Проверка hostName(), count() для hw.events_local:

| host | rows |
| --- | --- |
| ch11 | 500460 |
| ch12 | 500460 |
| ch21 | 499540 |
| ch22 | 499540 |

Проверка system.replicas:

| host | database | table | is_readonly | total_replicas | active_replicas | queue_size | absolute_delay |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ch11 | homework_data | push_events_source_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | homework_data | push_events_source_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | homework_data | push_events_source_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | homework_data | push_events_source_local | 0 | 2 | 2 | 0 | 0 |
| ch11 | hw | events_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | hw | events_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | hw | events_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | hw | events_local | 0 | 2 | 2 | 0 | 0 |
| ch11 | hw | push_counters_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | hw | push_counters_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | hw | push_counters_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | hw | push_counters_local | 0 | 2 | 2 | 0 | 0 |
| ch11 | hw | push_events_agg_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | hw | push_events_agg_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | hw | push_events_agg_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | hw | push_events_agg_local | 0 | 2 | 2 | 0 | 0 |
| ch11 | hw | push_events_dedup_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | hw | push_events_dedup_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | hw | push_events_dedup_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | hw | push_events_dedup_local | 0 | 2 | 2 | 0 | 0 |
| ch11 | hw | push_events_raw_local | 0 | 2 | 2 | 0 | 0 |
| ch12 | hw | push_events_raw_local | 0 | 2 | 2 | 0 | 0 |
| ch21 | hw | push_events_raw_local | 0 | 2 | 2 | 0 | 0 |
| ch22 | hw | push_events_raw_local | 0 | 2 | 2 | 0 | 0 |


Сравнение raw и agg через system.query_log: 

| variant | runs | server_ms_median | read_rows_median | read_bytes_median |
| --- | --- | --- | --- | --- |
| agg_distributed | 10 | 14.5 | 6.0 | 888.0 |
| raw_distributed | 10 | 14.5 | 12.0 | 364.0 |

Примеры отдельных записей system.query_log:

Запрос к raw:

```sql
SELECT platform, count() AS cnt, uniq(user_id) AS uniq_users,
       avg(latency_ms) AS avg_latency
FROM hw.push_events_raw
WHERE event_date >= '2026-10-02' AND event_date < '2026-10-09'
GROUP BY platform
ORDER BY platform;
```

Запрос к agg:

```sql
SELECT platform, countMerge(cnt) AS cnt, uniqMerge(uniq_users) AS uniq_users,
       avgMerge(avg_latency) AS avg_latency
FROM hw.push_events_agg
WHERE event_date >= '2026-10-02' AND event_date < '2026-10-09'
GROUP BY platform
ORDER BY platform;
```

| variant | query_id | query_duration_ms | read_rows | read_bytes |
| --- | --- | --- | --- | --- |
| raw_distributed | e15d3a41-f4b1-4f69-ab77-d1f3ddbec3c8 | 219 | 12 | 364 |
| agg_distributed | 08b1e90e-ab9d-4cf1-911d-03607f2f3596 | 26 | 6 | 888 |


