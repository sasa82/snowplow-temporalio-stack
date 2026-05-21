CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.snowplow_kafka
ON CLUSTER ${CLICKHOUSE_CLUSTER}
(
    `raw_string` String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list  = 'kafka1:9092,kafka2:9095',
    kafka_topic_list   = '${KAFKA_TOPIC_ENRICHED_GOOD}',
    kafka_group_name   = '${KAFKA_GROUP_NAME}',
    kafka_format       = 'LineAsString';
