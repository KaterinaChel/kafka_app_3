CREATE STREAM messages_stream (
    user_id STRING,
    recipient_id STRING,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
); 

CREATE TABLE message_cnt AS
SELECT '1' as dump,COUNT(*) as count
FROM messages_stream
GROUP BY '1'
EMIT CHANGES;

CREATE TABLE recipient_id_cnt AS
SELECT '1' as dump,COUNT_DISTINCT(recipient_id) as count
FROM messages_stream
GROUP BY '1'
EMIT CHANGES;

CREATE TABLE user_statistics AS
SELECT user_id,count(message) as cnt_message,count_distinct(recipient_id) as cnt_recipient
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;
