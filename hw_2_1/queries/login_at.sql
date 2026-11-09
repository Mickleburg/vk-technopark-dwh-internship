-- Parameters are bound by SparkSession.sql(args=...), never interpolated.
-- Session timezone: UTC. Intervals are [valid_from, valid_to).
SELECT user_login
FROM users_scd2
WHERE user_id = :user_id
  AND valid_from <= CAST(:as_of AS TIMESTAMP)
  AND (valid_to IS NULL OR CAST(:as_of AS TIMESTAMP) < valid_to);
