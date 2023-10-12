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
    mysql-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--type-mapping <option1,option2...>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--metadata-column <metadata-column>] \
    [--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

Example 1: synchronize tables into one Paimon table

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
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name='source_db' \
    --mysql-conf table-name='source_table1|source_table2' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

As example shows, the mysql-conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

Example 2: synchronize shards into one Paimon table

You can also set 'database-name' with a regular expression to capture multiple databases. A typical scenario is that a 
table 'source_table' is split into database 'source_db1', 'source_db2' ..., then you can synchronize data of all the 
'source_table's into one Paimon table.

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
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name='source_db.+' \
    --mysql-conf table-name='source_table' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

## Synchronizing Databases

By using [MySqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MySQL database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore-incompatible <true/false>] \
    [--merge-shards <true/false>] \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <mysql-table-name|name-regular-expr>] \
    [--excluding-tables <mysql-table-name|name-regular-expr>] \
    [--mode <sync-mode>] \
    [--metadata-column <metadata-column>] \
    [--type-mapping <option1,option2...>] \
    [--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_database >}}

Only tables with primary keys will be synchronized.

For each MySQL table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

Example 1: synchronize entire database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=source_db \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Example 2: synchronize newly added tables under database

Let's say at first a Flink job is synchronizing tables [product, user, address] 
under database `source_db`. The command to submit the job looks like:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=source_db \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4 \
    --including-tables 'product|user|address'
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
    mysql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=source_db \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --including-tables 'product|user|address|order|custom'
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
    mysql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name='db.+' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4 \
    --including-tables 'tbl.+'
```

By setting database-name to a regular expression, the synchronization job will capture all tables under matched databases 
and merge tables of the same name into one table.

{{< hint info >}}
You can set `--merge-shards false` to prevent merging shards. The synchronized tables will be named to 'databaseName_tableName' 
to avoid potential name conflict.
{{< /hint >}}

## FAQ

1. Chinese characters in records ingested from MySQL are garbled.
* Try to set `env.java.opts: -Dfile.encoding=UTF-8` in `flink-conf.yaml`
(the option is changed to `env.java.opts.all` since Flink-1.17).