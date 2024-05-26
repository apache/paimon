---
title: "Kafka CDC"
weight: 3
type: docs
aliases:
- /cdc-ingestion/kafka-cdc.html
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

# Kafka CDC

## Prepare Kafka Bundled Jar

```
flink-sql-connector-kafka-*.jar
```

## Supported Formats
Flink provides several Kafka CDC formats: Canal, Debezium, Ogg, Maxwell and Normal JSON.
If a message in a Kafka topic is a change event captured from another database using the Change Data Capture (CDC) tool, then you can use the Paimon Kafka CDC. Write the INSERT, UPDATE, DELETE messages parsed into the paimon table.
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Formats</th>
        <th class="text-left">Supported</th>
      </tr>
    </thead>
    <tbody>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/canal/">Canal CDC</a></td>
          <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/debezium/">Debezium CDC</a></td>
         <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/maxwell/ >}}">Maxwell CDC</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/ogg/">OGG CDC</a></td>
        <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/json/">JSON</a></td>
        <td>True</td>
        </tr>
    </tbody>
</table>

{{< hint info >}}
The JSON sources possibly missing some information. For example, Ogg and Maxwell format standards don't contain field 
types; When you write JSON sources into Flink Kafka sink, it will only reserve data and row type and drop other information. 
The synchronization job will try best to handle the problem as follows:
1. Usually, debezium-json contains 'schema' field, from which Paimon will retrieve data types. Make sure your debezium 
json has this field, or Paimon will use 'STRING' type.
2. If missing field types, Paimon will use 'STRING' type as default. 
3. If missing database name or table name, you cannot do database synchronization, but you can still do table synchronization.
4. If missing primary keys, the job might create non primary key table. You can set primary keys when submit job in table 
synchronization.
{{< /hint >}}

## Synchronizing Tables

By using [KafkaSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from Kafka's one topic into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping to-string] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--kafka_conf <kafka-source-conf> [--kafka_conf <kafka-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/kafka_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Kafka topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Kafka topic's tables.

Example 1:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

If the kafka topic doesn't contain message when you start the synchronization job, you must manually create the table
before submitting the job. You can define the partition keys and primary keys only, and the left columns will be added
by the synchronization job.

NOTE: In this case you shouldn't use --partition_keys or --primary_keys, because those keys are defined when creating
the table and can not be modified. Additionally, if you specified computed columns, you should also define all the argument
columns used for computed columns.

Example 2:
If you want to synchronize a table which has primary key 'id INT', and you want to compute a partition key 'part=date_format(create_time,yyyy-MM-dd)',
you can create a such table first (the other columns can be omitted):

```sql
CREATE TABLE test_db.test_table (
    id INT,                 -- primary key
    create_time TIMESTAMP,  -- the argument of computed column part
    part STRING,            -- partition key
    PRIMARY KEY (id, part) NOT ENFORCED
) PARTITIONED BY (part);
```

Then you can submit synchronization job:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --computed_column 'part=date_format(create_time,yyyy-MM-dd)' \
    ... (other conf)
```

Example 3: 
For some append data (such as log data), it can be treated as special CDC data with only INSERT operation type, so you can use 'format=json' to synchronize such data to the Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --computed_column 'pt=date_format(event_tm, yyyyMMdd)' \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=test_log \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf sink.parallelism=4
```

## Synchronizing Databases

By using [KafkaSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the multi topic or one topic into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <table-name|name-regular-expr>] \
    [--excluding_tables <table-name|name-regular-expr>] \
    [--type_mapping to-string] \
    [--kafka_conf <kafka-source-conf> [--kafka_conf <kafka-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/kafka_sync_database >}}

Only tables with primary keys will be synchronized.

This action will build a single combined sink for all tables. For each Kafka topic's table to be synchronized, if the
corresponding Paimon table does not exist, this action will automatically create the table, and its schema will be derived
from all specified Kafka topic's tables. If the Paimon table already exists and its schema is different from that parsed
from Kafka record, this action will try to preform schema evolution.

Example

Synchronization from one Kafka topic to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

Synchronization from multiple Kafka topics to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka_conf topic=order\;logistic_order\;user \
    --kafka_conf properties.group.id=123456 \
    --kafka_conf value.format=canal-json \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```
