---
title: "Overview"
weight: 1
type: docs
aliases:
- /cdc-ingestion/overview.html
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

# Overview

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
8. Pulsar Synchronizing Table: synchronize one Pulsar topic's table into one Paimon table.
9. Pulsar Synchronizing Database: synchronize one Pulsar topic containing multiple tables or multiple topics containing one table each into one Paimon database.

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

`--computed-column` are the definitions of computed columns. The argument field is from source table field name. Supported expressions are:

{{< generated/compute_column >}}

## Special Data Type Mapping

1. MySQL TINYINT(1) type will be mapped to Boolean by default. If you want to store number (-128~127) in it like MySQL,
   you can specify type mapping option `tinyint1-not-bool` (Use `--type-mapping`), then the column will be mapped to TINYINT in Paimon table.
2. You can use type mapping option `to-nullable` (Use `--type-mapping`) to ignore all NOT NULL constraints (except primary keys).
3. You can use type mapping option `to-string` (Use `--type-mapping`) to map all MySQL data type to STRING.
4. You can use type mapping option `char-to-string` (Use `--type-mapping`) to map MySQL CHAR(length)/VARCHAR(length) types to STRING.
5. You can use type mapping option `longtext-to-bytes` (Use `--type-mapping`) to map MySQL LONGTEXT types to BYTES.
6. MySQL BIT(1) type will be mapped to Boolean.
7. When using Hive catalog, MySQL TIME type will be mapped to STRING.
8. MySQL BINARY will be mapped to Paimon VARBINARY. This is because the binary value is passed as bytes in binlog, so it
   should be mapped to byte type (BYTES or VARBINARY). We choose VARBINARY because it can retain the length information.

## Setting Custom Job Name

Use `-Dpipeline.name=<job-name>` to set custom synchronization job name.