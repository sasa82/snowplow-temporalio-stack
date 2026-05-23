CREATE MATERIALIZED VIEW IF NOT EXISTS
${DATABASE_NAME}.snowplow_kafka_to_events_mv
TO ${DATABASE_NAME}.snowplow_events
(
    `app_id`           String,
    `app_namespace`    String,
    `event`            String,
    `page_url`         String,
    `country_code`     String,
    `city`             String,
    `region`           String,
    `platform`         String,
    `user_agent`       String,
    `page_title`       String,
    `latitude`         String,
    `longitude`        String,
    `viewport_width`   String,
    `viewport_height`  String,
    `screen_width`     String,
    `screen_height`    String,
    `referrer_url`     String,
    `clid`             String,
    `os_name`          String,
    `os_version`       String,
    `browser_name`     String,
    `browser_version`  String
) AS SELECT
    splitByChar('\t', raw_string)[1]    AS app_id,
    splitByChar('\t', raw_string)[9]    AS app_namespace,
    splitByChar('\t', raw_string)[6]    AS event,
    splitByChar('\t', raw_string)[30]   AS page_url,
    splitByChar('\t', raw_string)[19]   AS country_code,
    splitByChar('\t', raw_string)[21]   AS city,
    splitByChar('\t', raw_string)[25]   AS region,
    splitByChar('\t', raw_string)[2]    AS platform,
    splitByChar('\t', raw_string)[78]   AS user_agent,
    splitByChar('\t', raw_string)[31]   AS page_title,
    splitByChar('\t', raw_string)[23]   AS latitude,
    splitByChar('\t', raw_string)[24]   AS longitude,
    splitByChar('\t', raw_string)[96]   AS viewport_width,
    splitByChar('\t', raw_string)[97]   AS viewport_height,
    splitByChar('\t', raw_string)[104]  AS screen_width,
    splitByChar('\t', raw_string)[105]  AS screen_height,
    splitByChar('\t', raw_string)[32]   AS referrer_url,
    replaceRegexpAll(
        visitParamExtractRaw(
            JSONExtractRaw(splitByChar('\t', raw_string)[53], 'data'),
            'clid'
        ),
        concat('^[', regexpQuoteMeta('"'), ']+|[',
        regexpQuoteMeta('"'), ']+$'), ''
    ) AS clid,
    replaceRegexpAll(
        visitParamExtractRaw(
            splitByChar('\t', raw_string)[123],
            'operatingSystemName'
        ),
        concat('^[', regexpQuoteMeta('"'), ']+|[',
        regexpQuoteMeta('"'), ']+$'), ''
    ) AS os_name,
    replaceRegexpAll(
        visitParamExtractRaw(
            splitByChar('\t', raw_string)[123],
            'operatingSystemVersion'
        ),
        concat('^[', regexpQuoteMeta('"'), ']+|[',
        regexpQuoteMeta('"'), ']+$'), ''
    ) AS os_version,
    replaceRegexpAll(
        visitParamExtractRaw(
            splitByChar('\t', raw_string)[123],
            'agentName'
        ),
        concat('^[', regexpQuoteMeta('"'), ']+|[',
        regexpQuoteMeta('"'), ']+$'), ''
    ) AS browser_name,
    replaceRegexpAll(
        visitParamExtractRaw(
            splitByChar('\t', raw_string)[123],
            'agentVersion'
        ),
        concat('^[', regexpQuoteMeta('"'), ']+|[',
        regexpQuoteMeta('"'), ']+$'), ''
    ) AS browser_version
FROM ${DATABASE_NAME}.snowplow_kafka;
