# Spring Starter Library for Database Table Synchronization
### Description:

This Spring Starter library provides a solution for synchronizing database tables between different services using Apache Kafka for message exchange.

### Features:

Easy to use annotations for marking tables as external or synchronized.
Support for custom converters to map objects between different formats.
Automatic message deduplication to prevent duplicate data from being inserted into the database.
Fault tolerance and retry to ensure reliable data synchronization.

### Usage
- @ExternalTable: This annotation is used to mark a table on the receiving service as an external table. The table must exist in the database before the application starts.

- @SynchronizedTable: This annotation is used to mark a table on the data provider service as a synchronized table. The table will be created automatically if it does not exist.

### Converters:

Each of the annotations can use a custom converter implementation to convert objects between different formats. To implement a converter, simply create a class that implements the Converter interface.