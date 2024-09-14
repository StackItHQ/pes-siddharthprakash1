CREATE DATABASE IF NOT EXISTS superjoin_sync;

USE superjoin_sync;

DROP TABLE IF EXISTS data_table;

CREATE TABLE data_table (
    column1 VARCHAR(255) PRIMARY KEY,
    column2 VARCHAR(255),
    column3 VARCHAR(255)
);
USE superjoin_sync;
SELECT * FROM data_table;