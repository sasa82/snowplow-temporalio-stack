CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.snowplow_kafka
(
    `raw_string` String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list  = '${KAFKA_BROKERS}',
    kafka_topic_list   = '${KAFKA_TOPIC_ENRICHED_GOOD}',
    kafka_group_name   = '${KAFKA_GROUP_NAME}',
    kafka_format       = 'LineAsString';
