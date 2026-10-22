# hiveDataImport

A small Java utility that connects to HiveServer2 over JDBC, creates a database, and loads a local delimited text file into a Hive table with `LOAD DATA LOCAL INPATH`.

## Tech stack
Java, Maven, Apache Hive JDBC (Cloudera CDH 5.9.1 artifacts, Hive 1.1.0).

## What it does
1. Connects to `jdbc:hive2://localhost:10000/default` as user `hive`.
2. Prompts for a database name and runs `CREATE DATABASE` / `USE`.
3. Prompts for a table name and drops it if present.
4. Loads `/home/sample/Customers.txt` into that table.

A `CREATE TABLE` statement for a flight-delay schema is included but commented out, so the target table must already exist (or be created by uncommenting that block).

## How to run
Requires a running HiveServer2 on `localhost:10000` and the input file at the hard-coded path.

```bash
mvn package
mvn exec:java -Dexec.mainClass=sample.HiveLoadData   # or run HiveLoadData from your IDE
```

## Key concepts
Hive JDBC (HiveServer2), Hive DDL, `LOAD DATA LOCAL INPATH`, delimited text tables.

## Limitations
Connection URL, credentials and file path are hard-coded; database/table names are concatenated into SQL without validation.
