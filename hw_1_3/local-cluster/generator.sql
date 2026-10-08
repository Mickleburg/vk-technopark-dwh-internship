DROP DATABASE IF EXISTS homework_data;
CREATE DATABASE homework_data;


CREATE TABLE homework_data.push_events_source
(
    event_dt   DateTime,
    event_id   UInt64,
    user_id    UInt64,
    message_id UInt64,
    platform   LowCardinality(String),
    status     LowCardinality(String),
    latency_ms UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_dt)
ORDER BY (event_dt, user_id, event_id)
SETTINGS index_granularity = 8192;


INSERT INTO homework_data.push_events_source
SELECT
    now() - (rand64(1) % (30 * 86400))                  AS event_dt,
    number                                               AS event_id,
    rand64(2) % 10000                                    AS user_id,
    rand64(3) % 1000000                                  AS message_id,
    ['ANDROID', 'IOS', 'WEB', 'DESKTOP']
        [toInt8(rand64(4) % 4) + 1]                      AS platform,
    ['delivered', 'failed', 'opened']
        [toInt8(rand64(5) % 3) + 1]                      AS status,
    toUInt32(1 + (rand64(6) % 5000))                     AS latency_ms
FROM numbers(2000000);


INSERT INTO homework_data.push_events_source
SELECT
    event_dt + if(rand64(1) % 2 = 0,  3600, -3600)      AS event_dt,
    event_id,
    user_id,
    message_id,
    platform,
    status,
    latency_ms
FROM homework_data.push_events_source
WHERE rand64(7) % 100 < 5;