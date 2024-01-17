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

## 支持的格式
Flink 提供了多种 Kafka CDC 格式：Canal、Debezium、Ogg 和 Maxwell JSON。
如果 Kafka 主题中的消息是使用 Change Data Capture (CDC) 工具从另一个数据库捕获的更改事件，那么您可以使用 Paimon Kafka CDC。将 INSERT、UPDATE、DELETE 消息解析为 paimon 表中的数据。
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
JSON 源可能缺少一些信息。例如，Ogg 和 Maxwell 格式标准不包含字段类型；当您将 JSON 源写入 Flink Kafka 汇流，它只会保留数据和行类型，并删除其他信息。
同步作业将尝试尽力处理以下问题：

如果缺少字段类型，Paimon 将使用 'STRING' 类型作为默认类型。
如果缺少数据库名称或表名称，您无法进行数据库同步，但仍然可以进行表同步。
如果缺少主键，作业可能会创建非主键表。在提交表同步作业时，您可以设置主键。
{{< /hint >}}

## 同步表

通过在 Flink DataStream 作业中使用 [KafkaSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction) 或直接通过 flink run，用户可以将一个或多个表从 Kafka 的一个主题同步到一个 Paimon 表中。

要通过 `flink run` 使用此功能，请运行以下 shell 命令。

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

如果您指定的 Paimon 表不存在，此操作将自动创建该表。其架构将从所有指定的 Kafka 主题表中派生，它从主题获取最早的非 DDL 数据解析模式。如果 Paimon 表已经存在，其模式将与所有指定的 Kafka 主题表的模式进行比较。

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

如果在启动同步作业时 Kafka 主题不包含消息，您必须在提交作业之前手动创建表。您只能定义分区键和主键，剩下的列将由同步作业添加。
注意：在这种情况下，您不应该使用 --partition_keys 或 --primary_keys，因为这些键是在创建表时定义的，不能修改。此外，如果您指定了计算列，您还应该定义用于计算列的所有参数列。

Example 2:
如果要同步一个具有主键 'id INT' 的表，同时要计算一个分区键 'part=date_format(create_time,yyyy-MM-dd)'，您可以首先创建这样的表（其他列可以省略）：

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
    kafka_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --computed_column 'part=date_format(create_time,yyyy-MM-dd)' \
    ... (other conf)
```

## 同步数据库

通过在 Flink DataStream 作业中使用 [KafkaSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncDatabaseAction) 或直接通过 flink run，用户可以将多个主题或一个主题同步到一个 Paimon 数据库中。

要通过 flink run 使用此功能，请运行以下 shell 命令。

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

只会同步带有主键的表。

此操作将为所有表构建一个单一的组合汇流。对于要同步的每个 Kafka 主题的表，如果相应的 Paimon 表不存在，此操作将自动创建该表，并且其模式将从所有指定的 Kafka 主题表中派生。如果 Paimon 表已经存在并且其模式与从 Kafka 记录解析的模式不同，此操作将尝试执行模式演化。

Example

从一个 Kafka 主题同步到 Paimon 数据库。

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

从多个 Kafka 主题同步到 Paimon 数据库。

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
