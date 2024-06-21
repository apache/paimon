---
title: "Mysql CDC"
weight: 2
type: docs
aliases:
- /cdc-ingestion/mysql-cdc.html
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

# MySQL CDC

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://ververica.github.io/flink-cdc-connectors/).

## Prepare CDC Bundled Jar

```
flink-sql-connector-mysql-cdc-*.jar
```

## Synchronizing Tables

By using [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from MySQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping <option1,option2...>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata-column>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

Example 1: synchronize tables into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db' \
    --mysql_conf table-name='source_table1|source_table2' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

As example shows, the mysql_conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

Example 2: synchronize shards into one Paimon table

You can also set 'database-name' with a regular expression to capture multiple databases. A typical scenario is that a 
table 'source_table' is split into database 'source_db1', 'source_db2' ..., then you can synchronize data of all the 
'source_table's into one Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db.+' \
    --mysql_conf table-name='source_table' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## Synchronizing Databases

By using [MySqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MySQL database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore_incompatible <true/false>] \
    [--merge_shards <true/false>] \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <mysql-table-name|name-regular-expr>] \
    [--excluding_tables <mysql-table-name|name-regular-expr>] \
    [--mode <sync-mode>] \
    [--metadata_column <metadata-column>] \
    [--type_mapping <option1,option2...>] \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_database >}}

Only tables with primary keys will be synchronized.

For each MySQL table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

Example 1: synchronize entire database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

Example 2: synchronize newly added tables under database

Let's say at first a Flink job is synchronizing tables [product, user, address] 
under database `source_db`. The command to submit the job looks like:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'product|user|address'
```

At a later point we would like the job to also synchronize tables [order, custom], 
which contains history data. We can achieve this by recovering from the previous
snapshot of the job and thus reusing existing state of the job. The recovered job will 
first snapshot newly added tables, and then continue reading changelog from previous 
position automatically.

The command to recover from previous snapshot and add new tables to synchronize looks like:


```bash
<FLINK_HOME>/bin/flink run \
    --fromSavepoint savepointPath \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --including_tables 'product|user|address|order|custom'
```

{{< hint info >}}
You can set `--mode combined` to enable synchronizing newly added tables without restarting job.
{{< /hint >}}

Example 3: synchronize and merge multiple shards

Let's say you have multiple database shards `db1`, `db2`, ... and each database has tables `tbl1`, `tbl2`, .... You can 
synchronize all the `db.+.tbl.+` into tables `test_db.tbl1`, `test_db.tbl2` ... by following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='db.+' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'tbl.+'
```

By setting database-name to a regular expression, the synchronization job will capture all tables under matched databases 
and merge tables of the same name into one table.

{{< hint info >}}
You can set `--merge_shards false` to prevent merging shards. The synchronized tables will be named to 'databaseName_tableName' 
to avoid potential name conflict.
{{< /hint >}}

## FAQ

1. Chinese characters in records ingested from MySQL are garbled.
* Try to set `env.java.opts: -Dfile.encoding=UTF-8` in `flink-conf.yaml`
(the option is changed to `env.java.opts.all` since Flink-1.17).