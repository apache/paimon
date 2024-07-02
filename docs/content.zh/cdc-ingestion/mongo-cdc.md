---
title: "Mongo CDC"
weight: 4
type: docs
aliases:
- /cdc-ingestion/mongo-cdc.html
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

# Mongo CDC

## Prepare MongoDB Bundled Jar

```
flink-sql-connector-mongodb-cdc-*.jar
```
仅支持 cdc 2.4+

## 同步表

通过在 Flink DataStream 作业中使用 [MongoDBSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncTableAction) 或直接通过 flink run，用户可以将 MongoDB 中的一个集合同步到 Paimon 表格中。

要通过 flink run 使用此功能，请运行以下 shell 命令。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb_sync_table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--mongodb_conf <mongodb-cdc-source-conf> [--mongodb_conf <mongodb-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mongodb_sync_table >}}

以下是一些需要注意的要点：

1. `mongodb_conf` 引入了 `schema.start.mode` 参数，除了 MongoDB CDC 源配置之外，还在其基础上提供了两种模式：`dynamic`（默认）和 `specified`。 在 `dynamic` 模式下，MongoDB 模式信息在一个级别上进行解析，这形成了模式更改演变的基础。 在 `specified` 模式下，根据指定的条件进行同步。 可以通过配置 `field.name` 来指定同步字段，通过 `parser.path` 来指定这些字段的 JSON 解析路径。 两者之间的区别在于 `specify` 模式要求用户明确标识要使用的字段并创建一个基于这些字段的映射表。 另一方面，动态模式可以确保 Paimon 和 MongoDB 始终保持顶级字段一致，消除了专注于特定字段的需求。 当使用来自嵌套字段的值时，需要进一步处理数据表。
2. `mongodb_conf` 引入了 `default.id.generation` 参数，以增强 MongoDB CDC 源配置。 `default.id.generation` 设置提供了两种不同的行为：设置为 true 和设置为 false 时。 当 `default.id.generation` 设置为 true 时，MongoDB CDC 源遵循默认的 `_id` 生成策略，该策略涉及去除外部的 $oid 嵌套，以提供更直接的标识符。 这种模式简化了 `_id` 表示，使其更直接和用户友好。 相反，当 `default.id.generation` 设置为 false 时，MongoDB CDC 源保留了原始的 `_id` 结构，不进行任何额外的处理。 这种模式为用户提供了使用 MongoDB 提供的原始 `_id` 格式的灵活性，包括保留像 `$oid` 这样的嵌套元素。 选择两者之间的方式取决于用户的偏好：前者用于更干净、更简化的 `_id`，后者用于直接表示 MongoDB 的 `_id` 结构。

{{< generated/mongodb_operator >}}


函数可以在路径的末尾调用 - 函数的输入由函数本身决定。

{{< generated/mongodb_functions >}}

Path Examples
```json
{
    "store": {
        "book": [
            {
                "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95
            },
            {
                "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99
            },
            {
                "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 8.99
            },
            {
                "category": "fiction",
                "author": "J. R. R. Tolkien",
                "title": "The Lord of the Rings",
                "isbn": "0-395-19395-8",
                "price": 22.99
            }
        ],
        "bicycle": {
            "color": "red",
            "price": 19.95
        }
    },
    "expensive": 10
}
```

{{< generated/mongodb_path_example >}}

2. 同步表格需要将其主键设置为 `_id`。 这是因为 MongoDB 的更改事件记录在消息之前。 因此，我们只能将它们转换为 Flink 的 UPSERT 更改日志流。 Upsert 流需要一个唯一的键，这就是为什么我们必须声明 `_id` 为主键。 声明其他列为主键是不可行的，因为删除操作只涵盖了 _id 和分片键，不包括其他键和值。
    
3. MongoDB Change Streams 旨在返回简单的 JSON 文档，不包含任何数据类型定义。 这是因为 MongoDB 是一种面向文档的数据库，其核心特性之一是动态模式，文档可以包含不同的字段，并且字段的数据类型可以灵活。 因此，Change Streams 中缺少数据类型定义是为了保持此灵活性和可扩展性。 出于这个原因，我们将同步 MongoDB 到 Paimon 的所有字段数据类型都设置为 String，以解决无法获取数据类型的问题。
    

如果您指定的 Paimon 表格不存在，此操作将自动创建该表格。 其模式将从 MongoDB 集合派生而来。

Example 1: 将集合同步到一个 Paimon 表格中

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --computed_column '_year=year(age)' \
    --mongodb_conf hosts=127.0.0.1:27017 \
    --mongodb_conf username=root \
    --mongodb_conf password=123456 \
    --mongodb_conf database=source_db \
    --mongodb_conf collection=source_table1 \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

Example 2: 根据指定的字段映射将集合同步到 Paimon 表格中

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --mongodb_conf hosts=127.0.0.1:27017 \
    --mongodb_conf username=root \
    --mongodb_conf password=123456 \
    --mongodb_conf database=source_db \
    --mongodb_conf collection=source_table1 \
    --mongodb_conf schema.start.mode=specified \
    --mongodb_conf field.name=_id,name,description \
    --mongodb_conf parser.path=$._id,$.name,$.description \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## 同步数据库

通过在 Flink DataStream 作业中使用 [MongoDBSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncDatabaseAction) 或直接通过 `flink run`，用户可以将整个 MongoDB 数据库同步到一个 Paimon 数据库中。

要通过 `flink run` 使用此功能，请运行以下 shell 命令。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb_sync_database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <mongodb-table-name|name-regular-expr>] \
    [--excluding_tables <mongodb-table-name|name-regular-expr>] \
    [--mongodb_conf <mongodb-cdc-source-conf> [--mongodb_conf <mongodb-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mongodb_sync_database >}}

要同步的所有集合都需要将 _id 设置为主键。 要同步的每个 MongoDB 集合，如果相应的 Paimon 表格不存在，此操作将自动创建该表格。 其模式将从所有指定的 MongoDB 集合派生而来。 如果 Paimon 表格已经存在，其模式将与所有指定的 MongoDB 集合的模式进行比较。 在任务开始后创建的任何 MongoDB 表格将自动包含在内。

Example 1: 同步整个数据库

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mongodb_conf hosts=127.0.0.1:27017 \
    --mongodb_conf username=root \
    --mongodb_conf password=123456 \
    --mongodb_conf database=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

Example 2: 根据指定的表格同步

```bash
<FLINK_HOME>/bin/flink run \
--fromSavepoint savepointPath \
/path/to/paimon-flink-action-{{< version >}}.jar \
mongodb_sync_database \
--warehouse hdfs:///path/to/warehouse \
--database test_db \
--mongodb_conf hosts=127.0.0.1:27017 \
--mongodb_conf username=root \
--mongodb_conf password=123456 \
--mongodb_conf database=source_db \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://hive-metastore:9083 \
--table_conf bucket=4 \
--including_tables 'product|user|address|order|custom'
```
