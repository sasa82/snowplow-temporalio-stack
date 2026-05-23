CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.snowplow_events
(
    `app_id`           String,
    `event`            String,
    `page_url`         String,
    `timestamp`        DateTime DEFAULT now(),
    `clid`             String,
    `country_code`     String,
    `city`             String,
    `app_namespace`    String,
    `latitude`         String,
    `longitude`        String,
    `region`           String,
    `page_title`       String,
    `viewport_width`   String,
    `viewport_height`  String,
    `screen_width`     String,
    `referrer_url`     String,
    `user_agent`       String,
    `platform`         String,
    `screen_height`    String,
    `browser_name`     String,
    `browser_version`  String,
    `os_name`          String,
    `os_version`       String
)
ENGINE = MergeTree()
ORDER BY (`timestamp`)
SETTINGS index_granularity = 8192;
