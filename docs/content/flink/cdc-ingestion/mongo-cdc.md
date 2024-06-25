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
only cdc 2.4+ is supported

## Synchronizing Tables

By using [MongoDBSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one collection from MongoDB into one Paimon table.

To use this feature through `flink run`, run the following shell command.

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

Here are a few points to take note of:

1. The `mongodb_conf` introduces the `schema.start.mode` parameter on top of the MongoDB CDC source configuration.`schema.start.mode` provides two modes: `dynamic` (default) and `specified`.
   In `dynamic` mode, MongoDB schema information is parsed at one level, which forms the basis for schema change evolution.
   In `specified` mode, synchronization takes place according to specified criteria.
   This can be done by configuring `field.name` to specify the synchronization fields and `parser.path` to specify the JSON parsing path for those fields.
   The difference between the two is that the `specify` mode requires the user to explicitly identify the fields to be used and create a mapping table based on those fields.
   Dynamic mode, on the other hand, ensures that Paimon and MongoDB always keep the top-level fields consistent, eliminating the need to focus on specific fields.
   Further processing of the data table is required when using values from nested fields.
2. The `mongodb_conf` introduces the `default.id.generation` parameter as an enhancement to the MongoDB CDC source configuration. The `default.id.generation` setting offers two distinct behaviors: when set to true and when set to false.
   When `default.id.generation` is set to true, the MongoDB CDC source adheres to the default `_id` generation strategy, which involves stripping the outer $oid nesting to provide a more straightforward identifier. This mode simplifies the `_id` representation, making it more direct and user-friendly.
   On the contrary, when `default.id.generation` is set to false, the MongoDB CDC source retains the original `_id` structure, without any additional processing. This mode offers users the flexibility to work with the raw `_id` format as provided by MongoDB, preserving any nested elements like `$oid`.
   The choice between the two hinges on the user's preference: the former for a cleaner, simplified `_id` and the latter for a direct representation of MongoDB's `_id` structure.

{{< generated/mongodb_operator >}}


Functions can be invoked at the tail end of a path - the input to a function is the output of the path expression. The function output is dictated by the function itself.

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

1. The synchronized table is required to have its primary key set as `_id`.
   This is because MongoDB's change events are recorded before updates in messages.
   Consequently, we can only convert them into Flink's UPSERT change log stream.
   The upstart stream demands a unique key, which is why we must declare `_id` as the primary key.
   Declaring other columns as primary keys is not feasible, as delete operations only encompass the _id and sharding key, excluding other keys and values.

2. MongoDB Change Streams are designed to return simple JSON documents without any data type definitions. This is because MongoDB is a document-oriented database, and one of its core features is the dynamic schema, where documents can contain different fields, and the data types of fields can be flexible. Therefore, the absence of data type definitions in Change Streams is to maintain this flexibility and extensibility.
   For this reason, we have set all field data types for synchronizing MongoDB to Paimon as String to address the issue of not being able to obtain data types.

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from MongoDB collection.

Example 1: synchronize collection into one Paimon table

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

Example 2: Synchronize collection into a Paimon table according to the specified field mapping.

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

## Synchronizing Databases

By using [MongoDBSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MongoDB database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

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
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--mongodb_conf <mongodb-cdc-source-conf> [--mongodb_conf <mongodb-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mongodb_sync_database >}}

All collections to be synchronized need to set _id as the primary key.
For each MongoDB collection to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table.
Its schema will be derived from all specified MongoDB collection. If the Paimon table already exists, its schema will be compared against the schema of all specified MongoDB collection.
Any MongoDB tables created after the commencement of the task will automatically be included.

Example 1: synchronize entire database

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

Example 2: Synchronize the specified table.

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
