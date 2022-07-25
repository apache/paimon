---
title: "Hive"
weight: 2
type: docs
aliases:
- /engines/hive.html
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

# Hive

Table Store currently supports the following features related with Hive:
* Create, drop and insert into table store tables in Flink SQL through table store Hive catalog. Tables created in this way can also be read directly from Hive.
* Register existing table store tables as external tables in Hive SQL.

## Version

Table Store currently supports Hive 2.x.

## Install

{{< stable >}}
Download [flink-table-store-hive-connector-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-hive-connector-{{< version >}}.jar).
{{< /stable >}}
{{< unstable >}}

You are using an unreleased version of Table Store. See [Build From Source]({{< ref "docs/engines/build" >}}) for how to build and find Hive connector jar file.

{{< /unstable >}}

Copy table store Hive connector bundle jar to a path accessible by Hive, then use `add jar /path/to/flink-table-store-hive-connector-{{< version >}}.jar` to enable table store support in Hive.

## Using Table Store Hive Catalog

By using table store Hive catalog, you can create, drop and insert into table store tables from Flink. These operations directly affect the corresponding Hive metastore. Tables created in this way can also be accessed directly from Hive.

Execute the following Flink SQL script in Flink SQL client to define a table store Hive catalog and create a table store table.

```sql
-- Flink SQL CLI
-- Define table store Hive catalog

CREATE CATALOG my_hive WITH (
  'type' = 'table-store',
  'metastore' = 'hive',
  'uri' = 'thrift://<hive-metastore-host-name>:<port>',
  'warehouse' = '/path/to/table/store/warehouse'
);

-- Use table store Hive catalog

USE CATALOG my_hive;

-- Create a table in table store Hive catalog (use "default" database by default)

CREATE TABLE test_table (
  a int,
  b string
);

-- Insert records into test table

INSERT INTO test_table VALUES (1, 'Table'), (2, 'Store');

-- Read records from test table

SELECT * FROM test_table;

/*
+---+-------+
| a |     b |
+---+-------+
| 1 | Table |
| 2 | Store |
+---+-------+
*/
```

Run the following Hive SQL in Hive CLI to access the created table.

```sql
-- Enable table store support in Hive

ADD JAR /path/to/flink-table-store-hive-connector-{{< version >}}.jar;

-- List tables in Hive
-- (you might need to switch to "default" database if you're not there by default)

SHOW TABLES;

/*
OK
test_table
*/

-- Read records from test_table

SELECT a, b FROM test_table ORDER BY a;

/*
OK
1	Table
2	Store
*/
```

## Using External Table

To access existing table store table, you can also register them as external tables in Hive. Run the following Hive SQL in Hive CLI.

```sql
-- Enable table store support in Hive

ADD JAR /path/to/flink-table-store-hive-connector-{{< version >}}.jar;

-- Let's use the test_table created in the above section.
-- To create an external table, you don't need to specify any column or table properties.
-- Pointing the location to the path of table is enough.

CREATE EXTERNAL TABLE external_test_table
STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'
LOCATION '/path/to/table/store/warehouse/default.db/test_table';

-- Read records from external_test_table

SELECT a, b FROM test_table ORDER BY a;

/*
OK
1	Table
2	Store
*/
```