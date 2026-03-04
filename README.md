# kafka_app_3
В данной работе представлены примеры запросов, создания таблиц и топиков в KSQLdb
# 1 Создание топиков

docker exec -it kafka1 kafka-topics --create --topic messages --bootstrap-server kafka1:9092 --partitions 2 --replication-factor 3

# 2 SQL
1 CREATE STREAM

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

2 Реализуйте таблицы, подсчитывающие:
общее количество отправленных сообщений;
количество уникальных получателей сообщений;

так как без GROUP BY таблица не создавалась, то фиктивная колонка

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

3 Создайте таблицу user_statistics для агрегирования данных по каждому пользователю:
сообщения, отправленные каждым пользователем;
количество уникальных получателей для каждого пользователя;

CREATE TABLE user_statistics AS
SELECT user_id,count(message) as cnt_message,count_distinct(recipient_id) as cnt_recipient
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;

# 3 Тесты

тестовые данные 
docker exec -it kafka1 /usr/bin/kafka-console-producer --bootstrap-server localhost:9092 --topic messages --property parse.key=true --property "key.separator=|"


user1|{"user_id":"user1","recipient_id":"user2","message":"hello","timestamp":"41567875"}
user2|{"user_id":"user2","recipient_id":"user4","message":"hello","timestamp":"485962"}
user1|{"user_id":"user2","recipient_id":"user5","message":"hello hi","timestamp":"4854962"}
user1|{"user_id":"user2","recipient_id":"user5","message":"hello hi friend","timestamp":"48534962"}
user1|{"user_id":"user1","recipient_id":"user3","message":"how are you ","timestamp":"431567875"}
user1|{"user_id":"user1","recipient_id":"user3","message":"heeyyyy ","timestamp":"4315678475"}


тестовые запросы в ksqlDB для проверки:

select * from message_cnt EMIT CHANGES;
select * from recipient_id_cnt EMIT CHANGES;
