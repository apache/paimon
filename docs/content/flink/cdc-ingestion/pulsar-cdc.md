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
Flink provides several Pulsar CDC formats: Canal, Debezium, Ogg, Maxwell and Normal JSON.
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
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/json/">JSON</a></td>
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
    pulsar_sync_table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping to-string] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--pulsar_conf <pulsar-source-conf> [--pulsar_conf <pulsar-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/pulsar_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Pulsar topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Pulsar topic's tables.

Example 1:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --pulsar_conf topic=order \
    --pulsar_conf value.format=canal-json \
    --pulsar_conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar_conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar_conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

If the Pulsar topic doesn't contain message when you start the synchronization job, you must manually create the table
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
    pulsar_sync_table \
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
    --table_conf sink.parallelism=4
```

## Synchronizing Databases

By using [PulsarSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/pulsar/PulsarSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the multi topic or one topic into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar_sync_database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <table-name|name-regular-expr>] \
    [--excluding_tables <table-name|name-regular-expr>] \
    [--type_mapping to-string] \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--pulsar_conf <pulsar-source-conf> [--pulsar_conf <pulsar-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
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
    pulsar_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --pulsar_conf topic=order \
    --pulsar_conf value.format=canal-json \
    --pulsar_conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar_conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar_conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

Synchronization from multiple Pulsar topics to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    pulsar_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --pulsar_conf topic=order,logistic_order,user \
    --pulsar_conf value.format=canal-json \
    --pulsar_conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \
    --pulsar_conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \
    --pulsar_conf pulsar.consumer.subscriptionName=paimon-tests \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## Additional pulsar_config

There are some useful options to build Flink Pulsar Source, but they are not provided by flink-pulsar-connector document. They are:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Key</th>
        <th class="text-left">Default</th>
        <th class="text-left">Type</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
        <tr>
         <td>value.format</td>
          <td>(none)</td>
          <td>String</td>
          <td>Defines the format identifier for encoding value data.</td>
        </tr>
        <tr>
          <td>topic</td>
          <td>(none)</td>
          <td>String</td>
          <td>Topic name(s) from which the data is read. It also supports topic list by separating topic by semicolon 
              like 'topic-1;topic-2'. Note, only one of "topic-pattern" and "topic" can be specified.
          </td>
        </tr>
        <tr>
          <td>topic-pattern</td>
          <td>(none)</td>
          <td>String</td>
          <td>The regular expression for a pattern of topic names to read from. All topics with names that match the 
              specified regular expression will be subscribed by the consumer when the job starts running. Note, only 
              one of "topic-pattern" and "topic" can be specified.
          </td>
        </tr>
        <tr>
          <td>pulsar.startCursor.fromMessageId</td>
          <td>EARLIEST</td>
          <td>Sting</td>
          <td>Using a unique identifier of a single message to seek the start position. The common format is a triple 
              '&ltlong&gtledgerId,&ltlong&gtentryId,&ltint&gtpartitionIndex'. Specially, you can set it to 
              EARLIEST (-1, -1, -1) or LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).
          </td>
        </tr>
        <tr>
          <td>pulsar.startCursor.fromPublishTime</td>
          <td>(none)</td>
          <td>Long</td>
          <td>Using the message publish time to seek the start position.</td>
        </tr>
        <tr>
          <td>pulsar.startCursor.fromMessageIdInclusive</td>
          <td>true</td>
          <td>Boolean</td>
          <td>Whether to include the given message id. This option only works when the message id is not EARLIEST or LATEST.</td>
        </tr>
        <tr>
          <td>pulsar.stopCursor.atMessageId</td>
          <td>(none)</td>
          <td>String</td>
          <td>Stop consuming when the message id is equal or greater than the specified message id. Message that is equal 
              to the specified message id will not be consumed. The common format is a triple '&ltlong&gtledgerId,&ltlong&gtentryId,&ltint&gtpartitionIndex'. 
              Specially, you can set it to LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).
        <tr>
          <td>pulsar.stopCursor.afterMessageId</td>
          <td>(none)</td>
          <td>String</td>
          <td>Stop consuming when the message id is greater than the specified message id. Message that is equal to the 
              specified message id will be consumed. The common format is a triple '&ltlong&gtledgerId,&ltlong&gtentryId,&ltint&gtpartitionIndex'. 
              Specially, you can set it to LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).
          </td>
        </tr>
        <tr>
          <td>pulsar.stopCursor.atEventTime</td>
          <td>(none)</td>
          <td>Long</td>
          <td>Stop consuming when message event time is greater than or equals the specified timestamp. 
              Message that even time is equal to the specified timestamp will not be consumed.
          </td>
        </tr>
        <tr>
          <td>pulsar.stopCursor.afterEventTime</td>
          <td>(none)</td>
          <td>Long</td>
          <td>Stop consuming when message event time is greater than the specified timestamp. 
              Message that even time is equal to the specified timestamp will be consumed.
          </td>
        </tr>
        <tr>
          <td>pulsar.source.unbounded</td>
          <td>true</td>
          <td>Boolean</td>
          <td>To specify the boundedness of a stream.</td>
        </tr>
    </tbody>
</table>

