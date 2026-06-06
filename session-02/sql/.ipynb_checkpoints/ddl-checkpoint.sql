CREATE TABLE IF NOT EXISTS glue_catalog.ad_lakehouse.events (
    event_id    BIGINT,
    event_time  TIMESTAMP,
    user_id     STRING,
    amount      DOUBLE
)
USING iceberg
PARTITIONED BY (days(event_time))
LOCATION 's3://iceberg-lab-smj/warehouse/events'
TBLPROPERTIES (
    'format-version' = '2'
);