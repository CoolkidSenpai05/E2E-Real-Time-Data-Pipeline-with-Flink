-- flink/sql/fraud_sink_to_iceberg.sql
-- Configure Flink Settings for Streaming and State Management
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '1';
-- Load Required Jars
ADD JAR '/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar';
ADD JAR '/opt/flink/lib/flink-json-1.18.1.jar';
ADD JAR '/opt/flink/lib/iceberg-flink-runtime-1.18-1.5.0.jar';
ADD JAR '/opt/flink/lib/hadoop-common-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-hdfs-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-client-2.8.3.jar';
ADD JAR '/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar';
ADD JAR '/opt/flink/lib/bundle-2.20.18.jar';
-- Confirm Jars are Loaded
SHOW JARS;
-- Create Iceberg Catalog
DROP CATALOG IF EXISTS iceberg;
CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password'
);
-- Define Kafka Labeled Stream Source
DROP TABLE IF EXISTS kafka_clickstream_labeled;
CREATE TABLE IF NOT EXISTS kafka_clickstream_labeled (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    url STRING,
    session_id STRING,
    device STRING,
    `timestamp` STRING,
    geo_location STRING,
    purchase_amount DOUBLE,
    is_fraud BOOLEAN,
    fraud_probability DOUBLE,
    fraud_prediction BOOLEAN,
    prediction_timestamp STRING,
    model_version STRING,
    processing_error STRING,
    event_time TIMESTAMP_LTZ(3) METADATA
    FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream_labeled',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);
-- Tạo bảng Iceberg để lưu kết quả fraud detection
CREATE TABLE IF NOT EXISTS iceberg.db.clickstream_fraud (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    url STRING,
    session_id STRING,
    device STRING,
    `timestamp` STRING,
    geo_location STRING,
    purchase_amount DOUBLE,
    is_fraud BOOLEAN,
    fraud_probability DOUBLE,
    fraud_prediction BOOLEAN,
    prediction_timestamp STRING,
    model_version STRING,
    processing_error STRING
) WITH (
    'catalog-name' = 'iceberg_catalog',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3a://warehouse/',
    'format-version' = '2'
);
-- Insert từ Kafka labeled stream vào Iceberg
INSERT INTO iceberg.db.clickstream_fraud
SELECT event_id,
    user_id,
    event_type,
    url,
    session_id,
    device,
    `timestamp`,
    CAST(geo_location AS STRING),
    CAST(purchase_amount AS DOUBLE),
    CAST(is_fraud AS BOOLEAN),
    CAST(fraud_probability AS DOUBLE),
    CAST(fraud_prediction AS BOOLEAN),
    prediction_timestamp,
    model_version,
    processing_error
FROM kafka_clickstream_labeled;