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

## 支持的格式

Flink 提供了多种 Pulsar CDC 格式：Canal、Debezium、Ogg 和 Maxwell JSON。 如果 Pulsar 主题中的消息是使用 Change Data Capture (CDC) 工具从另一个数据库捕获的更改事件，那么您可以使用 Paimon Pulsar CDC。将解析为 INSERT、UPDATE、DELETE 消息的数据写入 paimon 表中。
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
JSON 源可能缺少一些信息。例如，Ogg 和 Maxwell 格式标准不包含字段类型；当您将 JSON 源写入 Flink Pulsar 汇流器时，它只会保留数据和行类型，并删除其他信息。同步作业将尽力处理以下问题：

1. 如果缺少字段类型，Paimon 将使用 'STRING' 类型作为默认类型。
2. 如果缺少数据库名称或表名称，您无法进行数据库同步，但仍可以进行表同步。
3. 如果缺少主键，作业可能会创建非主键表。在表同步中提交作业时，可以设置主键。
{{< /hint >}}

## 同步表

通过在 Flink DataStream 作业中使用 [PulsarSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/pulsar/PulsarSyncTableAction) 或直接通过 `flink run`，用户可以将一个或多个表从 Pulsar 的一个主题同步到一个 Paimon 表中。

要通过 `flink run` 使用此功能，请运行以下 shell 命令。

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

如果您指定的 Paimon 表不存在，此操作将自动创建该表。其模式将从所有指定的 Pulsar 主题的表中派生，它获取主题的最早的非 DDL 数据解析模式。如果 Paimon 表已经存在，则其模式将与所有指定的 Pulsar 主题的表的模式进行比较。

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

如果在启动同步作业时 Pulsar 主题不包含消息，您必须在提交作业之前手动创建表。您只能定义分区键和主键，其余列将由同步作业添加。

注意：在这种情况下，您不应使用 --partition_keys 或 --primary_keys，因为这些键在创建表时定义，并且不能修改。另外，如果您指定了计算列，还应定义用于计算列的所有参数列。

示例 2： 如果您想要同步一个具有主键 'id INT' 的表，并且想要计算一个分区键 'part=date_format(create_time,yyyy-MM-dd)'，您可以首先创建这样一个表（其他列可以省略）：

```sql
CREATE TABLE test_db.test_table (
    id INT,                 -- primary key
    create_time TIMESTAMP,  -- the argument of computed column part
    part STRING,            -- partition key
    PRIMARY KEY (id, part) NOT ENFORCED
) PARTITIONED BY (part);
```

然后，您可以提交同步作业：

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

## 同步数据库

通过在 Flink DataStream 作业中使用 [PulsarSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/pulsar/PulsarSyncDatabaseAction) 或直接通过 `flink run`，用户可以将多个主题或一个主题同步到一个 Paimon 数据库中。

要通过 `flink run` 使用此功能，请运行以下 shell 命令。

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
    [--pulsar_conf <pulsar-source-conf> [--pulsar_conf <pulsar-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/pulsar_sync_database >}}

只有带有主键的表才会被同步。

此操作将为所有表构建一个单一的组合接收器。对于要同步的每个 Pulsar 主题的表，如果相应的 Paimon 表不存在，此操作将自动创建该表，并且其模式将从所有指定的 Pulsar 主题的表中派生。如果 Paimon 表已经存在并且其模式与从 Pulsar 记录解析的模式不同，此操作将尝试执行模式演变。

示例

从一个 Pulsar 主题同步到 Paimon 数据库。

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

从多个 Pulsar 主题同步到 Paimon 数据库。

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

有一些有用的选项可用于构建 Flink Pulsar 源，但它们未在 flink-pulsar-connector 文档中提供。它们包括：

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

