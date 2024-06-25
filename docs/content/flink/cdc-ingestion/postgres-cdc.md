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

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://nightlies.apache.org/flink/flink-cdc-docs-stable/).

## Prepare CDC Bundled Jar

```
flink-connector-postgres-cdc-*.jar
```

## Synchronizing Tables

By using [PostgresSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/postgres/PostgresSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from PostgreSQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgres_sync_table
    --warehouse <warehouse_path> \
    --database <database_name> \
    --table <table_name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary_keys>] \
    [--type_mapping <option1,option2...>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata_column>] \
    [--postgres_conf <postgres_cdc_source_conf> [--postgres_conf <postgres_cdc_source_conf> ...]] \
    [--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] \
    [--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]
```

{{< generated/postgres_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified PostgreSQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified PostgreSQL tables.

Example 1: synchronize tables into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgres_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --postgres_conf hostname=127.0.0.1 \
    --postgres_conf username=root \
    --postgres_conf password=123456 \
    --postgres_conf database-name='source_db' \
    --postgres_conf schema-name='public' \
    --postgres_conf table-name='source_table1|source_table2' \
    --postgres_conf slot.name='paimon_cdc' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

As example shows, the postgres_conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

Example 2: synchronize shards into one Paimon table

You can also set 'schema-name' with a regular expression to capture multiple schemas. A typical scenario is that a 
table 'source_table' is split into schema 'source_schema1', 'source_schema2' ..., then you can synchronize data of all the 
'source_table's into one Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    postgres_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --postgres_conf hostname=127.0.0.1 \
    --postgres_conf username=root \
    --postgres_conf password=123456 \
    --postgres_conf database-name='source_db' \
    --postgres_conf schema-name='source_schema.+' \
    --postgres_conf table-name='source_table' \
    --postgres_conf slot.name='paimon_cdc' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```
