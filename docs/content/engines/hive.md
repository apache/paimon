---
title: "Hive"
weight: 5
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

This documentation is a guide for using Paimon in Hive.

## Version

Paimon currently supports Hive 3.1, 2.3, 2.2, 2.1 and 2.1-cdh-6.3.

## Execution Engine

Paimon currently supports MR and Tez execution engine for Hive Read, and MR execution engine for Hive Write. 
Note If you use beeline, please restart the hive cluster.

## Installation

Download the jar file with corresponding version.

{{< stable >}}

|                  | Jar                                                                                                                                                                                                                     |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hive 3.1         | [paimon-hive-connector-3.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-3.1/{{< version >}}/paimon-hive-connector-3.1-{{< version >}}.jar)                         |
| Hive 2.3         | [paimon-hive-connector-2.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.3/{{< version >}}/paimon-hive-connector-2.3-{{< version >}}.jar)                         |
| Hive 2.2         | [paimon-hive-connector-2.2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.2/{{< version >}}/paimon-hive-connector-2.2-{{< version >}}.jar)                         |
| Hive 2.1         | [paimon-hive-connector-2.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.1/{{< version >}}/paimon-hive-connector-2.1-{{< version >}}.jar)                         |
| Hive 2.1-cdh-6.3 | [paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.1-cdh-6.3/{{< version >}}/paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

|                  | Jar                                                                                                                                                                   |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hive 3.1         | [paimon-hive-connector-3.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-3.1/{{< version >}}/)                 |
| Hive 2.3         | [paimon-hive-connector-2.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.3/{{< version >}}/)                 |
| Hive 2.2         | [paimon-hive-connector-2.2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.2/{{< version >}}/)                 |
| Hive 2.1         | [paimon-hive-connector-2.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.1/{{< version >}}/)                 |
| Hive 2.1-cdh-6.3 | [paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.1-cdh-6.3/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.
`mvn clean install -DskipTests`

You can find Hive connector jar in `./paimon-hive/paimon-hive-connector-<hive-version>/target/paimon-hive-connector-<hive-version>-{{< version >}}.jar`.

There are several ways to add this jar to Hive.

* You can create an `auxlib` folder under the root directory of Hive, and copy `paimon-hive-connector-{{< version >}}.jar` into `auxlib`.
* You can also copy this jar to a path accessible by Hive, then use `add jar /path/to/paimon-hive-connector-{{< version >}}.jar` to enable paimon support in Hive. Note that this method is not recommended. If you're using the MR execution engine and running a join statement, you may be faced with the exception `org.apache.hive.com.esotericsoftware.kryo.kryoexception: unable to find class`.

NOTE: 

* If you are using HDFS, make sure that the environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR` is set.
* With hive cbo, it may lead to some incorrect query results, such as to query `struct` type with `not null` predicate, you can disable the cbo by `set hive.cbo.enable=false;` command.

## Hive SQL: access Paimon Tables already in Hive metastore

Run the following Hive SQL in Hive CLI to access the created table.

```sql
-- Assume that paimon-hive-connector-<hive-version>-{{< version >}}.jar is already in auxlib directory.
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

-- Insert records into test table
-- Limitations:
-- Only support INSERT INTO, not support INSERT OVERWRITE
-- It is recommended to write to a non primary key table
-- Writing to a primary key table may result in a large number of small files

INSERT INTO test_table VALUES (3, 'Paimon');

SELECT a, b FROM test_table ORDER BY a;

/*
OK
1	Table
2	Store
3	Paimon
*/

-- time travel

SET paimon.scan.snapshot-id=1;
SELECT a, b FROM test_table ORDER BY a;
/*
OK
1	Table
2	Store
3	Paimon
*/
SET paimon.scan.snapshot-id=null;

```

## Hive SQL: create new Paimon Tables

You can create new paimon tables in Hive. Run the following Hive SQL in Hive CLI.

```sql
-- Assume that paimon-hive-connector-{{< version >}}.jar is already in auxlib directory.
-- Let's create a new paimon table.

SET hive.metastore.warehouse.dir=warehouse_path;

CREATE TABLE hive_test_table(
    a INT COMMENT 'The a field',
    b STRING COMMENT 'The b field'
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler';
```

## Hive SQL: access Paimon Tables by External Table

To access existing paimon table, you can also register them as external tables in Hive. Run the following Hive SQL in Hive CLI.

```sql
-- Assume that paimon-hive-connector-{{< version >}}.jar is already in auxlib directory.
-- Let's use the test_table created in the above section.
-- To create an external table, you don't need to specify any column or table properties.
-- Pointing the location to the path of table is enough.

CREATE EXTERNAL TABLE external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION '/path/to/table/store/warehouse/default.db/test_table';
    
-- In addition to the way setting location above, you can also place the location setting in TBProperties
-- to avoid Hive accessing Paimon's location through its own file system when creating tables.
-- This method is effective in scenarios using Object storage,such as s3.

CREATE EXTERNAL TABLE external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
TBLPROPERTIES (
 'paimon_location' ='s3://xxxxx/path/to/table/store/warehouse/default.db/test_table'
);

-- Read records from external_test_table

SELECT a, b FROM external_test_table ORDER BY a;

/*
OK
1	Table
2	Store
*/

-- Insert records into test table

INSERT INTO external_test_table VALUES (3, 'Paimon');

SELECT a, b FROM external_test_table ORDER BY a;

/*
OK
1	Table
2	Store
3	Paimon
*/

```

## Hive Type Conversion

This section lists all supported type conversion between Hive and Paimon.
All Hive's data types are available in package `org.apache.hadoop.hive.serde2.typeinfo`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Hive Data Type</th>
      <th class="text-left" style="width: 10%">Paimon Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>StructTypeInfo</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>MapTypeInfo</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>ListTypeInfo</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("boolean")</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("tinyint")</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("smallint")</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("int")</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("bigint")</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("float")</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("double")</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>CharTypeInfo(length)</code></td>
      <td><code>CharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("string")</code></td>
      <td><code>VarCharType(VarCharType.MAX_LENGTH)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VarcharTypeInfo(length)</code></td>
      <td><code>VarCharType(length), length is less than VarCharType.MAX_LENGTH</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("date")</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("timestamp")</code></td>
      <td><code>TimestampType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalTypeInfo(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("binary")</code></td>
      <td><code>VarBinaryType</code>, <code>BinaryType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>
