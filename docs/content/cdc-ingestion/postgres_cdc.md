---
title: "Postgres CDC"
weight: 2
type: docs
aliases:
- /cdc-ingestion/postgres-cdc.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Postgres CDC

## Prepare CDC Bundled Jar

```
flink-sql-connector-postgres-cdc-*.jar
```

## Synchronizing Tables

By using [PostgreSqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/postgresql/PostgreSqlSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from PostgreSQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgresql-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--postgresql-conf <postgresql-cdc-source-conf> [--postgresql-conf <postgresql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/postgresql_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified PostgreSQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified PostgreSQL tables.

Example 1: synchronize tables into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgresql-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --postgresql-conf hostname=127.0.0.1 \
    --postgresql-conf username=postgres \
    --postgresql-conf password=123456 \
    --postgresql-conf database-name=source_db \
    --postgresql-conf schema-name=public \
    --postgresql-conf table-name='source_table1|source_table2' \
    --postgresql-conf slot.name=my_replication_slot \
    --postgresql-conf decoding.plugin.name=pgoutput \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

As example shows, the postgresql-conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

Example 2: synchronize schemas into one Paimon table

You can also set 'schema-name' with a regular expression to capture multiple schemas. A typical scenario is that a
table 'source_table' is split into schema 'source_schema1', 'source_schema2' ..., then you can synchronize data of all the
'source_table's into one Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgresql-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --postgresql-conf hostname=127.0.0.1 \
    --postgresql-conf username=postgres \
    --postgresql-conf password=123456 \
    --postgresql-conf database-name=source_db \
    --postgresql-conf schema-name='source_schema.+' \
    --postgresql-conf table-name=source_table \
    --postgresql-conf slot.name=my_replication_slot \
    --postgresql-conf decoding.plugin.name=pgoutput \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

## Synchronizing Databases

By using [PostgreSqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/postgresql/PostgreSqlSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, Users can synchronize the whole schema of a specific PostgreSQL database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgresql-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore-incompatible <true/false>] \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <postgresql-table-name|name-regular-expr>] \
    [--excluding-tables <postgresql-table-name|name-regular-expr>] \
    [--postgresql-conf <postgresql-cdc-source-conf> [--postgresql-conf <postgresql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/postgresql_sync_database >}}

Only tables with primary keys will be synchronized.

For each PostgreSQL table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified PostgreSQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified PostgreSQL tables.

Example 1: synchronize entire database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgresql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --postgresql-conf hostname=127.0.0.1 \
    --postgresql-conf username=postgres \
    --postgresql-conf password=123456 \
    --postgresql-conf database-name=source_db \
    --postgresql-conf schema-name=public \
    --postgresql-conf slot.name=my_replication_slot \
    --postgresql-conf decoding.plugin.name=pgoutput \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Example 2: Synchronize the specified table.

```bash
<FLINK_HOME>/bin/flink run \
-D execution.checkpointing.interval=5s \
/path/to/paimon-flink-action-{{< version >}}.jar \
mongodb-sync-database \
--warehouse hdfs:///path/to/warehouse \
--database test_db \
--postgresql-conf hosts=127.0.0.1 \
--postgresql-conf username=postgres \
--postgresql-conf password=123456 \
--postgresql-conf database=source_db \
--postgresql-conf schema-name=public \
--postgresql-conf slot.name=my_replication_slot \
--postgresql-conf decoding.plugin.name=pgoutput \
--including-tables 'source_table1|source_table2' \
--catalog-conf metastore=hive \
--catalog-conf uri=thrift://hive-metastore:9083 \
--table-conf bucket=4 \
--table-conf changelog-producer=input \
--table-conf sink.parallelism=4
```

The above example will only synchronize the source_table1 and source_table2 tables under the public schema of source_db.

Example 3: Exclude the specified table.

```bash
<FLINK_HOME>/bin/flink run \
-D execution.checkpointing.interval=5s \
/path/to/paimon-flink-action-{{< version >}}.jar \
mongodb-sync-database \
--warehouse hdfs:///path/to/warehouse \
--database test_db \
--postgresql-conf hosts=127.0.0.1 \
--postgresql-conf username=postgres \
--postgresql-conf password=123456 \
--postgresql-conf database=source_db \
--postgresql-conf schema-name=public \
--excluding-tables 'source_table1|source_table2' \
--postgresql-conf slot.name=my_replication_slot \
--postgresql-conf decoding.plugin.name=pgoutput \
--catalog-conf metastore=hive \
--catalog-conf uri=thrift://hive-metastore:9083 \
--table-conf bucket=4 \
--table-conf changelog-producer=input \
--table-conf sink.parallelism=4
```

The above example will not synchronize the source_table1 and source_table2 tables under the public schema of source_db.
