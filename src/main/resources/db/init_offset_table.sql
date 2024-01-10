CREATE TABLE kafka_db_sync (
                               table_name VARCHAR(255) PRIMARY KEY,
                               offset VARCHAR(255),
                               status VARCHAR(255),
                               error_message VARCHAR(255)
);