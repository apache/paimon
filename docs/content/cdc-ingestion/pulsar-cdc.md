---
title: "Pulsar CDC"
weight: 5
type: docs
aliases:
- /cdc-ingestion/pulsar-cdc.html
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

# Pulsar CDC

## Prepare Pulsar Bundled Jar

```
flink-connector-pulsar-*.jar
```

## Supported Formats
Flink provides several Pulsar CDC formats: Canal, Debezium, Ogg and Maxwell JSON.
If a message in a pulsar topic is a change event captured from another database using the Change Data Capture (CDC) tool, then you can use the Paimon Pulsar CDC. Write the INSERT, UPDATE, DELETE messages parsed into the paimon table.
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
    </tbody>
</table>

{{< hint info >}}
The JSON sources possibly missing some information. For example, Ogg and Maxwell format standards don't contain field 
types; When you write JSON sources into Flink Pulsar sink, it will only reserve data and row type and drop other information. 
The synchronization job will try best to handle the problem as follows:
1. If missing field types, Paimon will use 'STRING' type as default. 
2. If missing database name or table name, you cannot do database synchronization, but you can still do table synchronization.
3. If missing primary keys, the job might create non primary key table. You can set primary keys when submit job in table 
synchronization.
{{< /hint >}}

## Synchronizing Tables

By using [PulsarSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/pulsar/PulsarSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from Pulsar's one topic into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--type-mapping to-string] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--pulsar-conf <pulsar-source-conf> [--pulsar-conf <pulsar-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/pulsar_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Pulsar topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Pulsar topic's tables.

Example 1:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --pulsar-conf topic=order \
    --pulsar-conf value.format=canal-json \
    --pulsar-conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar-conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar-conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

If the Pulsar topic doesn't contain message when you start the synchronization job, you must manually create the table
before submitting the job. You can define the partition keys and primary keys only, and the left columns will be added
by the synchronization job.

NOTE: In this case you shouldn't use --partition-keys or --primary-keys, because those keys are defined when creating
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
    pulsar-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --computed-column 'part=date_format(create_time,yyyy-MM-dd)' \
    ... (other conf)
```

## Synchronizing Databases

By using [PulsarSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/pulsar/PulsarSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the multi topic or one topic into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <table-name|name-regular-expr>] \
    [--excluding-tables <table-name|name-regular-expr>] \
    [--type-mapping to-string] \
    [--pulsar-conf <pulsar-source-conf> [--pulsar-conf <pulsar-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/pulsar_sync_database >}}

Only tables with primary keys will be synchronized.

This action will build a single combined sink for all tables. For each Pulsar topic's table to be synchronized, if the
corresponding Paimon table does not exist, this action will automatically create the table, and its schema will be derived
from all specified Pulsar topic's tables. If the Paimon table already exists and its schema is different from that parsed
from Pulsar record, this action will try to preform schema evolution.

Example

Synchronization from one Pulsar topic to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --pulsar-conf topic=order \
    --pulsar-conf value.format=canal-json \
    --pulsar-conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar-conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar-conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Synchronization from multiple Pulsar topics to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --pulsar-conf topic=order,logistic_order,user \
    --pulsar-conf value.format=canal-json \
    --pulsar-conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar-conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar-conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```
