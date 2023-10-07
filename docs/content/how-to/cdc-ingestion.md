---
title: "CDC Ingestion"
weight: 8
type: docs
aliases:
- /how-to/cdc-ingestion.html
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

# CDC Ingestion

Paimon supports a variety of ways to ingest data into Paimon tables with schema evolution. This means that the added
columns are synchronized to the Paimon table in real time and the synchronization job will not be restarted for this purpose.

We currently support the following sync ways:

1. MySQL Synchronizing Table: synchronize one or multiple tables from MySQL into one Paimon table.
2. MySQL Synchronizing Database: synchronize the whole MySQL database into one Paimon database.
3. [API Synchronizing Table]({{< ref "/api/flink-api#cdc-ingestion-table" >}}): synchronize your custom DataStream input into one Paimon table.
4. Kafka Synchronizing Table: synchronize one Kafka topic's table into one Paimon table. 
5. Kafka Synchronizing Database: synchronize one Kafka topic containing multiple tables or multiple topics containing one table each into one Paimon database.
6. MongoDB Synchronizing Collection: synchronize one Collection from MongoDB into one Paimon table. 
7. MongoDB Synchronizing Database: synchronize the whole MongoDB database into one Paimon database.


## What is Schema Evolution

Suppose we have a MySQL table named `tableA`, it has three fields: `field_1`, `field_2`, `field_3`. When we want to load
this MySQL table to Paimon, we can do this in Flink SQL, or use [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction).

**Flink SQL:**

In Flink SQL, if we change the table schema of the MySQL table after the ingestion, the table schema change will not be synchronized to Paimon.

{{< img src="/img/cdc-ingestion-flinksql.png">}}

**MySqlSyncTableAction:**

In [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction),
if we change the table schema of the MySQL table after the ingestion, the table schema change will be synchronized to Paimon,
and the data of `field_4` which is newly added will be synchronized to Paimon too.

{{< img src="/img/cdc-ingestion-schema-evolution.png">}}

## MySQL

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://ververica.github.io/flink-cdc-connectors/).

### Prepare CDC Bundled Jar

```
flink-sql-connector-mysql-cdc-*.jar
```

### Synchronizing Tables

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

### Synchronizing Databases

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


## Kafka

### Prepare Kafka Bundled Jar

```
flink-sql-connector-kafka-*.jar
```

### Supported Formats
Flink provides several Kafka CDC formats: Canal, Debezium, Ogg and Maxwell JSON.
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
         <td>False</td>
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
In Oracle GoldenGate and Maxwell, the data format synchronized to Kafka does not include field data type information. As a result, Paimon sets the data type for all fields to "String" by default.
{{< /hint >}}

### Synchronizing Tables

By using [KafkaSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from Kafka's one topic into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--type-mapping to-string] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--kafka-conf <kafka-source-conf> [--kafka-conf <kafka-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/kafka_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Kafka topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Kafka topic's tables.

Example

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-column '_year=year(age)' \
    --kafka-conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka-conf topic=order \
    --kafka-conf properties.group.id=123456 \
    --kafka-conf value.format=canal-json \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```
### Synchronizing Databases

By using [KafkaSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the multi topic or one topic into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <table-name|name-regular-expr>] \
    [--excluding-tables <table-name|name-regular-expr>] \
    [--type-mapping to-string] \
    [--kafka-conf <kafka-source-conf> [--kafka-conf <kafka-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
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
    kafka-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka-conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka-conf topic=order \
    --kafka-conf properties.group.id=123456 \
    --kafka-conf value.format=canal-json \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Synchronization from multiple Kafka topics to Paimon database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    kafka-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --kafka-conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka-conf topic=order\;logistic_order\;user \
    --kafka-conf properties.group.id=123456 \
    --kafka-conf value.format=canal-json \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```
## MongoDB

### Prepare MongoDB Bundled Jar

```
flink-sql-connector-mongodb-cdc-*.jar
```
only cdc 2.4+ is supported

### Synchronizing Tables

By using [MongoDBSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one collection from MongoDB into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--mongodb-conf <mongodb-cdc-source-conf> [--mongodb-conf <mongodb-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

{{< generated/mongodb_sync_table >}}

Here are a few points to take note of:

1. The `mongodb-conf` introduces the `schema.start.mode` parameter on top of the MongoDB CDC source configuration.`schema.start.mode` provides two modes: `dynamic` (default) and `specified`.
In `dynamic` mode, MongoDB schema information is parsed at one level, which forms the basis for schema change evolution.
In `specified` mode, synchronization takes place according to specified criteria.
This can be done by configuring `field.name` to specify the synchronization fields and `parser.path` to specify the JSON parsing path for those fields.
The difference between the two is that the `specify` mode requires the user to explicitly identify the fields to be used and create a mapping table based on those fields.
Dynamic mode, on the other hand, ensures that Paimon and MongoDB always keep the top-level fields consistent, eliminating the need to focus on specific fields.
Further processing of the data table is required when using values from nested fields.
2. The `mongodb-conf` introduces the `default.id.generation` parameter as an enhancement to the MongoDB CDC source configuration. The `default.id.generation` setting offers two distinct behaviors: when set to true and when set to false.
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

2. The synchronized table is required to have its primary key set as `_id`. 
This is because MongoDB's change events are recorded before updates in messages. 
Consequently, we can only convert them into Flink's UPSERT change log stream. 
The upstart stream demands a unique key, which is why we must declare `_id` as the primary key. 
Declaring other columns as primary keys is not feasible, as delete operations only encompass the _id and sharding key, excluding other keys and values.

3. MongoDB Change Streams are designed to return simple JSON documents without any data type definitions. This is because MongoDB is a document-oriented database, and one of its core features is the dynamic schema, where documents can contain different fields, and the data types of fields can be flexible. Therefore, the absence of data type definitions in Change Streams is to maintain this flexibility and extensibility.
For this reason, we have set all field data types for synchronizing MongoDB to Paimon as String to address the issue of not being able to obtain data types.

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from MongoDB collection. 

Example 1: synchronize collection into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --computed-column '_year=year(age)' \
    --mongodb-conf hosts=127.0.0.1:27017 \
    --mongodb-conf username=root \
    --mongodb-conf password=123456 \
    --mongodb-conf database=source_db \
    --mongodb-conf collection=source_table1 \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

Example 2: Synchronize collection into a Paimon table according to the specified field mapping.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --mongodb-conf hosts=127.0.0.1:27017 \
    --mongodb-conf username=root \
    --mongodb-conf password=123456 \
    --mongodb-conf database=source_db \
    --mongodb-conf collection=source_table1 \
    --mongodb-conf schema.start.mode=specified \
    --mongodb-conf field.name=_id,name,description \
    --mongodb-conf parser.path=$._id,$.name,$.description \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

### Synchronizing Databases

By using [MongoDBSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mongodb/MongoDBSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MongoDB database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mongodb-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <mongodb-table-name|name-regular-expr>] \
    [--excluding-tables <mongodb-table-name|name-regular-expr>] \
    [--mongodb-conf <mongodb-cdc-source-conf> [--mongodb-conf <mongodb-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
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
    mongodb-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mongodb-conf hosts=127.0.0.1:27017 \
    --mongodb-conf username=root \
    --mongodb-conf password=123456 \
    --mongodb-conf database=source_db \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```
Example 2: Synchronize the specified table.

```bash
<FLINK_HOME>/bin/flink run \
--fromSavepoint savepointPath \
/path/to/paimon-flink-action-{{< version >}}.jar \
mongodb-sync-database \
--warehouse hdfs:///path/to/warehouse \
--database test_db \
--mongodb-conf hosts=127.0.0.1:27017 \
--mongodb-conf username=root \
--mongodb-conf password=123456 \
--mongodb-conf database=source_db \
--catalog-conf metastore=hive \
--catalog-conf uri=thrift://hive-metastore:9083 \
--table-conf bucket=4 \
--including-tables 'product|user|address|order|custom'
```

## Schema Change Evolution

Cdc Ingestion supports a limited number of schema changes. Currently, the framework can not rename table, drop columns, so the
behaviors of `RENAME TABLE` and `DROP COLUMN` will be ignored, `RENAME COLUMN` will add a new column. Currently supported schema changes includes:

* Adding columns.

* Altering column types. More specifically,

  * altering from a string type (char, varchar, text) to another string type with longer length,
  * altering from a binary type (binary, varbinary, blob) to another binary type with longer length,
  * altering from an integer type (tinyint, smallint, int, bigint) to another integer type with wider range,
  * altering from a floating-point type (float, double) to another floating-point type with wider range,

  are supported.

## Computed Functions

`--computed-column` are the definitions of computed columns. The argument field is from Kafka topic's table field name. Supported expressions are:

{{< generated/compute_column >}}

## Special Data Type Mapping

1. MySQL TINYINT(1) type will be mapped to Boolean by default. If you want to store number (-128~127) in it like MySQL, 
you can specify type mapping option `tinyint1-not-bool` (Use `--type-mapping`), then the column will be mapped to TINYINT in Paimon table.
2. You can use type mapping option `to-nullable` (Use `--type-mapping`) to ignore all NOT NULL constraints (except primary keys).
3. You can use type mapping option `to-string` (Use `--type-mapping`) to map all MySQL data type to STRING.
4. You can use type mapping option `char-to-string` (Use `--type-mapping`) to map MySQL CHAR(length)/VARCHAR(length) types to STRING.
5. MySQL BIT(1) type will be mapped to Boolean.
6. When using Hive catalog, MySQL TIME type will be mapped to STRING.
7. MySQL BINARY will be mapped to Paimon VARBINARY. This is because the binary value is passed as bytes in binlog, so it 
should be mapped to byte type (BYTES or VARBINARY). We choose VARBINARY because it can retain the length information.

## FAQ
1. Chinese characters in records ingested from MySQL are garbled.
* Try to set `env.java.opts: -Dfile.encoding=UTF-8` in `flink-conf.yaml`
(the option is changed to `env.java.opts.all` since Flink-1.17).