---
title: "SQLServer CDC"
weight: 2
type: docs
aliases:
- /cdc-ingestion/sqlserver-cdc.html
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

# SQLServer CDC

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://ververica.github.io/flink-cdc-connectors/).

## Prepare CDC Bundled Jar

```
flink-sql-connector-sqlserver-cdc-*.jar
```

## Synchronizing Tables

By using [SqlServerSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/sqlserver/SqlServerSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from SQLServer into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    sqlserver-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--type-mapping <option1,option2...>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--metadata-column <metadata-column>] \
    [--sqlserver-conf <sqlserver-cdc-source-conf> [--sqlserver-conf <sqlserver-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/sqlserver_sync_table >}}

Currently, only one database is supported for synchronization. Regular matching of 'database name' is not supported.

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified SQLServer tables. If the Paimon table already exists, its schema will be compared against the schema of all specified SQLServer tables.

Example 1: synchronize tables into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    sqlserver-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --sqlserver-conf hostname=127.0.0.1 \
    --sqlserver-conf username=root \
    --sqlserver-conf password=123456 \
    --sqlserver-conf database-name='source_db' \
    --sqlserver-conf schema-name='dbo' \
    --sqlserver-conf table-name='dbo.source_table1|dbo.source_table2' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

As example shows, the sqlserver-conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

Example 2: synchronize shards into one Paimon table

You can also use regular expressions to set the "schema_name" to capture multiple schemas. A typical scenario is to split the table "source_table" into databases "source_dbo1" and "source_dbo2"..., Then all the data of "source_table" can be synchronized to a Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --sqlserver-conf hostname=127.0.0.1 \
    --sqlserver-conf username=root \
    --sqlserver-conf password=123456 \
    --sqlserver-conf database-name='source_db' \
    --sqlserver-conf schema-name='source_dbo.+' \
    --sqlserver-conf table-name='source_table' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

## Synchronizing Databases

By using [SqlServerSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/SqlServerSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole SQLServer database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    sqlserver-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore-incompatible <true/false>] \
    [--merge-shards <true/false>] \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <sqlserver-table-name|name-regular-expr>] \
    [--excluding-tables <sqlserver-table-name|name-regular-expr>] \
    [--mode <sync-mode>] \
    [--metadata-column <metadata-column>] \
    [--type-mapping <option1,option2...>] \
    [--sqlserver-conf <sqlserver-cdc-source-conf> [--sqlserver-conf <sqlserver-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/sqlserver_sync_database >}}

Currently, only one database is supported for synchronization. Regular matching of 'database_name' is not supported

Only tables with primary keys will be synchronized.

For each SQLServer table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified SQLServer tables. If the Paimon table already exists, its schema will be compared against the schema of all specified SQLServer tables.

Example 1: synchronize entire database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    sqlserver-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --sqlserver-conf hostname=127.0.0.1 \
    --sqlserver-conf username=root \
    --sqlserver-conf password=123456 \
    --sqlserver-conf database-name=source_db \
    --sqlserver-conf schema-name=dbo \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Example 2: synchronize and merge multiple shards

Let's say you have multiple schema shards `schema1`, `schema2`, ... and each schema has tables `tbl1`, `tbl2`, .... You can
synchronize all the `schema.+.tbl.+` into tables `test_db.tbl1`, `test_db.tbl2` ... by following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    sqlserver-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --sqlserver-conf hostname=127.0.0.1 \
    --sqlserver-conf username=root \
    --sqlserver-conf password=123456 \
    --sqlserver-conf database-name='source_db' \
    --sqlserver-conf schema-name='db.+' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4 \
    --including-tables 'tbl.+'
```

By setting schema-name to a regular expression, the synchronization job will capture all tables under matched schemas 
and merge tables of the same name into one table.

{{< hint info >}}
You can set `--merge-shards false` to prevent merging shards. The synchronized tables will be named to 'databaseName_tableName' 
to avoid potential name conflict.
{{< /hint >}}

## FAQ

1. Chinese characters in records ingested from MySQL are garbled.
* Try to set `env.java.opts: -Dfile.encoding=UTF-8` in `flink-conf.yaml`
(the option is changed to `env.java.opts.all` since Flink-1.17).