---
title: "Overview"
weight: 1
type: docs
aliases:
- /flink/cdc-ingestion/overview.html
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
3. [Program API Sync]({{< ref "/program-api/flink-api#cdc-ingestion-table" >}}): synchronize your custom DataStream input into one Paimon table.
4. Kafka Synchronizing Table: synchronize one Kafka topic's table into one Paimon table.
5. Kafka Synchronizing Database: synchronize one Kafka topic containing multiple tables or multiple topics containing one table each into one Paimon database.
6. MongoDB Synchronizing Collection: synchronize one Collection from MongoDB into one Paimon table.
7. MongoDB Synchronizing Database: synchronize the whole MongoDB database into one Paimon database.
8. Pulsar Synchronizing Table: synchronize one Pulsar topic's table into one Paimon table.
9. Pulsar Synchronizing Database: synchronize one Pulsar topic containing multiple tables or multiple topics containing one table each into one Paimon database.
10. SQLServer Synchronizing Table: synchronize one or multiple tables from SQLServer into one Paimon table.
11. SQLServer Synchronizing Database: synchronize the whole SQLServer database into one Paimon database.

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

`--computed_column` are the definitions of computed columns. The argument field is from source table field name. 

### Temporal Functions

Temporal functions can convert date and epoch time to another form. A common use case is to generate partition values.

{{< generated/temporal_functions >}}

The data type of the temporal-column can be one of the following cases:
1. DATE, DATETIME or TIMESTAMP.
2. Any integer numeric type (such as INT and BIGINT). In this case, the data will be considered as epoch time of `1970-01-01 00:00:00`. 
You should set precision of the value (default is 0).
3. STRING. In this case, if you didn't set the time unit, the data will be considered as formatted string of DATE, 
DATETIME or TIMESTAMP value. Otherwise, the data will be considered as string value of epoch time. So you must set time 
unit in the latter case.

The precision represents the unit of the epoch time. Currently, There are four valid precisions: `0` (for epoch seconds),
`3` (for epoch milliseconds), `6`(for epoch microseconds) and `9` (for epoch nanoseconds). Take the time point 
`1970-01-01 00:00:00.123456789` as an example, the epoch seconds are 0, the epoch milliseconds are 123, the epoch microseconds 
are 123456, and the epoch nanoseconds are 123456789. The precision should match the input values. You can set precision 
in this way: `date_format(epoch_col, yyyy-MM-dd, 0)`.

`date_format` is a flexible function which is able to convert the temporal value to various formats with different format 
strings. A most common format string is `yyyy-MM-dd HH:mm:ss.SSS`. Another example is `yyyy-ww` which can extract the year 
and the week-of-the-year from the input. Note that the output is affected by the locale. For example, in some regions the 
first day of a week is Monday while in others is Sunday, so if you use `date_format(date_col, yyyy-ww)` and the input of 
date_col is 2024-01-07 (Sunday), the output maybe `2024-01` (if the first day of a week is Monday) or `2024-02` (if the 
first day of a week is Sunday).

### Other Functions

{{< generated/other_functions >}}

## Special Data Type Mapping

1. MySQL TINYINT(1) type will be mapped to Boolean by default. If you want to store number (-128~127) in it like MySQL,
   you can specify type mapping option `tinyint1-not-bool` (Use `--type_mapping`), then the column will be mapped to TINYINT in Paimon table.
2. You can use type mapping option `to-nullable` (Use `--type_mapping`) to ignore all NOT NULL constraints (except primary keys).
3. You can use type mapping option `to-string` (Use `--type_mapping`) to map all MySQL data type to STRING.
4. You can use type mapping option `char-to-string` (Use `--type_mapping`) to map MySQL CHAR(length)/VARCHAR(length) types to STRING.
5. You can use type mapping option `longtext-to-bytes` (Use `--type_mapping`) to map MySQL LONGTEXT types to BYTES.
6. MySQL `BIGINT UNSIGNED`, `BIGINT UNSIGNED ZEROFILL`, `SERIAL` will be mapped to `DECIMAL(20, 0)` by default. You can 
use type mapping option `bigint-unsigned-to-bigint` (Use `--type_mapping`) to map these types to Paimon `BIGINT`, but there 
is potential data overflow because `BIGINT UNSIGNED` can store up to 20 digits integer value but Paimon `BIGINT` can only 
store up to 19 digits integer value. So you should ensure the overflow won't occur when using this option.
7. MySQL BIT(1) type will be mapped to Boolean.
8. When using Hive catalog, MySQL TIME type will be mapped to STRING.
9. MySQL BINARY will be mapped to Paimon VARBINARY. This is because the binary value is passed as bytes in binlog, so it
   should be mapped to byte type (BYTES or VARBINARY). We choose VARBINARY because it can retain the length information.

## Custom Job Settings

### Checkpointing

Use `-Dexecution.checkpointing.interval=<interval>` to enable checkpointing and set interval. For 0.7 and later versions,
if you haven't enabled checkpointing, Paimon will enable checkpointing by default and set checkpoint interval to 180 seconds.

### Job Name

Use `-Dpipeline.name=<job-name>` to set custom synchronization job name.

### table configuration

You can use `--table_conf` to set table properties and some flink job properties (like `sink.parallelism`). If the table is
created by the cdc job, the table's properties will be equal to the given properties. Otherwise, the job will use the given
properties to alter table's properties. But note that immutable options (like `merge-engine`) and bucket number won't be altered.