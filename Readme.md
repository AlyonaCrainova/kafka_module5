Инструкция по запуску и проверке работоспособности:
1. Собираем java-проект, меняем путь к JAR в DockerFile при необходимости.
2. В терминале Docker desktop выполняем переход в папку с проектом: cd <путь к папке>. Запускаем кластер через команду терминала: docker compose up.
   Запускаем команду терминала docker compose ps. Сервисы ui, zookeeper, три брокера kafka, 2 потребителя, schema-registry, prometheus, postgres, kafka-connect, grafana в статусе Up. 
3. Вызовем psql внутри запущенного postgres контейнера: docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d customers.
4. Создаем таблицы:
CREATE TABLE users (
id SERIAL PRIMARY KEY,
name VARCHAR(100),
email VARCHAR(100),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); 
CREATE TABLE orders (
id SERIAL PRIMARY KEY,
user_id INT REFERENCES users(id),
product_name VARCHAR(100),
quantity INT,
order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
5. Сконфигурируем коннектор через коммандную строку.
curl -X POST -H "Content-Type: application/json" --data @<путь к папке проекта>\connector.json http://localhost:8083/connectors
6. Вставим данные в таблицы
-- Добавление пользователей
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');
-- Добавление заказов
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);
7. В терминал приложений в Docker desktop вывелись вставленные данные + сгенерированные поля.

Назначение компонентов:
• zookeeper — управляет кластером и следит за его состоянием.

• kafka-0, kafka-1, kafka-2 — брокеры, обеспечивающие запись и чтение сообщений.

• schema-registry — хранит схемы данных.

• postgres — база данных, откуда через встроенный Debezium забираются изменения.

• kafka-ui — веб-интерфейс для просмотра топиков и сообщений.

• kafka-connect — переносит данные из Postgres в Kafka. На нем также стоит агент для сбора метрик.

• prometheus — собирает метрики работы с kafka-connect.

• grafana — визуализирует собранные prometheus метрики.

• consumer-app-orders и consumer-app-users — приложения для вывода переданных из БД данных в кафка в терминал.

Взаимосвязи:

Все сервисы сидят в одной сети proxynet и общаются друг с другом.
Сначала запускается Zookeeper, а за ним три брокера Kafka, которые к нему подключаются. Schema-registry и Kafka-UI работают напрямую с кластером Kafka.
Kafka-connect связывает всё воедино: он ждет запуска базы Postgres и Kafka, забирает данные из базы, сверяет их со схемами в Schema-registry и отправляет в топики.
Затем Prometheus стучится в Kafka-connect и забирает метрики, а Grafana подключается к Prometheus, чтобы показать их на графиках.
В самом конце, когда брокеры Kafka полностью поднялись и прошли проверку (healthcheck), запускаются приложения consumer-app-orders и consumer-app-users, которые начинают читать свои топики.

Настройки Debezium Connector располагаются в файле connector.json в директории проекта.
• "name": "pg-connector" — имя экземпляра коннектора

• "connector.class": "io.debezium.connector.postgresql.PostgresConnector" — полное имя класса коннектора, который мы хотим запустить

• "database.hostname": "postgres" — адрес PostgreSQL сервера

• "database.port": "5432" — номер порта, который слушает PostgreSQL сервер

• "database.user": "postgres-user" — имя юзера для подключения

• "database.password": "postgres-pw" — пароль юзера для подключения

• "database.dbname": "customers" — имя базы данных для подключения

• "database.server.name": "customers" — имя сервера базы данных

• "table.include.list": "public.users, public.orders" — список таблиц для мониторинга

• "transforms": "unwrap" — исключаем метаданные из сообщения

• "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState" — извлекаем из сообщения только измененные данные

• "transforms.unwrap.drop.tombstones": "false" — tombstone-события не будут передаваться дальше в результирующий топик

• "transforms.unwrap.delete.handling.mode": "rewrite" — событие удаления - обычное сообщение с __deleted: true

• "topic.prefix": "customers" — префикс для имени топика, который будет создан для таблиц

• "topic.creation.enable": "true" — разрешает автоматическое создание топиков под таблицы

• "topic.creation.default.replication.factor": "-1" — установить в качестве фактора репликации под создаваемый топик дефолтный для kafka

• "topic.creation.default.partitions": "-1" — установить в качестве количества партиций под создаваемый топик дефолтный для kafka

• "skipped.operations": "none" — нет фильтрации событий

• "key.converter": "org.apache.kafka.connect.json.JsonConverter" — класс для сериализации/десериализации ключа

• "key.converter.schemas.enable": "false" — без служебных полей kafka connect

• "value.converter": "org.apache.kafka.connect.json.JsonConverter" — класс для сериализации/десериализации значения

• "value.converter.schemas.enable": "false" — без служебных полей kafka connect

   





