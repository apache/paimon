---
title: "SQL Functions"
sidebar_position: 2
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

# SQL Functions

Use built-in functions for partitions and blob descriptors, or define functions in a REST
catalog. Configure the [catalog and extensions](./quick-start#setup) first.

| Task | Function or guide |
| --- | --- |
| Find the latest nonempty top-level partition | [`max_pt`](#max_pt) |
| Reference an external blob | [`path_to_descriptor`](#path_to_descriptor) |
| Inspect blob metadata | [`descriptor_to_string`](#descriptor_to_string) |
| Create a temporary OSS download URL | [`descriptor_to_presigned_url`](#descriptor_to_presigned_url) |
| Define reusable logic | [User-defined functions](#user-defined-function) |

## Built-in Function

### max_pt

`sys.max_pt($table_name)`

It accepts a string type literal to specify the table name and return a max-valid-toplevel partition value.
- **valid**: the partition which contains data files
- **toplevel**: only return the first partition value if the table has multi-partition columns

It would throw exception when:
- the table is not a partitioned table
- the partitioned table does not have partition
- all of the partitions do not contains data files

**Example**

```sql
SELECT sys.max_pt('t');
-- 20250101

SELECT * FROM t where pt = sys.max_pt('t');
-- a, 20250101
```

### path_to_descriptor

`sys.path_to_descriptor($file_path)`

Converts a file path (STRING) to a blob descriptor (BINARY). This function is useful when working with blob data stored in external files. It creates a blob descriptor that references the file at the specified path.

**Arguments:**
- `file_path` (STRING): The path to the external file containing the blob data.

**Returns:**
- A BINARY value representing the serialized blob descriptor.

**Example**

```sql
-- Insert blob data using path_to_descriptor function
INSERT INTO t VALUES ('1', 'paimon', sys.path_to_descriptor('file:///path/to/blob_file'));

-- Insert with partition
INSERT OVERWRITE TABLE t PARTITION(ds='1017', batch='test')
VALUES ('1', 'paimon', '1024', '12345678', '20241017', sys.path_to_descriptor('file:///path/to/blob_file'));
```

### descriptor_to_string

`sys.descriptor_to_string($descriptor)`

Converts a blob descriptor (BINARY) to its string representation (STRING). This function is useful for debugging or displaying the contents of a blob descriptor in a human-readable format.

**Arguments:**
- `descriptor` (BINARY): The blob descriptor bytes to convert.

**Returns:**
- A STRING representation of the blob descriptor.

**Example**

```sql
-- Convert a blob descriptor to string for inspection
SELECT sys.descriptor_to_string(content) FROM t WHERE id = '1';
-- [BlobDescriptor{version=1', uri='/path/to/data-2c103f6f-3857-4062-abc3-2e260374a68e-1.blob', offset=4, length=1048576}]
```

### descriptor_to_presigned_url

`sys.descriptor_to_presigned_url($source_table, $descriptor, $validity)`

Creates a temporary HTTPS GET URL for an OSS-backed blob descriptor. Use
`sys.try_descriptor_to_presigned_url` to return `NULL` instead of failing on row-level errors.

- `source_table` must be a non-null STRING literal in `database.table` form, or
  `catalog.database.table` with the same catalog as the function.
- `descriptor` is the serialized BINARY descriptor. Set `blob-as-descriptor=true` when reading a
  normal BLOB column.
- `validity` is a positive day-time interval containing whole seconds.

The materialized object has no file extension and is served as
`application/octet-stream`. Its bytes are unchanged.

The catalog's `fs.oss.endpoint` must be a standard public HTTPS endpoint, for example
`https://oss-cn-hangzhou.aliyuncs.com`.

```sql
ALTER TABLE image_table SET TBLPROPERTIES ('blob-as-descriptor' = 'true');

SELECT sys.descriptor_to_presigned_url(
    'default.image_table',
    image,
    INTERVAL '5' MINUTE)
FROM image_table;
```

Repeated short-term calls for the same descriptor reuse the materialized object and issue a fresh
URL. Treat the URL as a bearer credential: send it immediately to the consumer and never log or
persist it. Direct model `image_url` use is supported only for image formats verified with that
model; PDF is not covered. See [Blob Storage](../multimodal-table/blob-references#presigned-urls-for-oss-blobs)
for Java and Flink examples, caching behavior, and current limitations.

## User-defined Function

Paimon-managed function definitions require a REST catalog. Choose a definition format:

| Definition | Use it for | Requirement |
| --- | --- | --- |
| [Java lambda](#lambda-function) | A small Java expression stored in the catalog | Register metadata and a Spark definition with procedures. |
| [JAR implementation](#file-function) | Existing Spark or Hive UDF/UDAF classes | Spark 3.4+ and the implementation JAR. |
| [SQL body](#sql-function) | Reusable scalar SQL logic | Spark 4.0+. |

The examples assume the REST catalog and its `default` database are selected.

### Lambda Function

Define the function signature, then attach a Java lambda as its Spark implementation.

**Example**

```sql
-- Create Function
CALL sys.create_function(`function` => 'default.area_func',
  `inputParams` => '[{"id": 0, "name":"length", "type":"INT"}, {"id": 1, "name":"width", "type":"INT"}]',
  `returnParams` => '[{"id": 0, "name":"area", "type":"BIGINT"}]',
  `deterministic` => true,
  `comment` => 'comment',
  `options` => 'k1=v1,k2=v2'
);

-- Alter Function
CALL sys.alter_function(`function` => 'default.area_func',
  `change` => '{"action" : "addDefinition", "name" : "spark", "definition" : {"type" : "lambda", "definition" : "(Integer length, Integer width) -> { return (long) length * width; }", "language": "JAVA" } }'
);

-- Drop Function
CALL sys.drop_function(`function` => 'default.area_func');
```

### File Function

Register a Spark or Hive UDF/UDAF implementation packaged in a JAR.

Currently, supports Spark or Hive implementations of UDFs and UDAFs, see [Spark UDFs](https://spark.apache.org/docs/latest/sql-ref-functions.html#udfs-user-defined-functions)

This feature requires Spark 3.4 or higher.

Replace the class name and JAR path with your implementation. Permanent and temporary
functions use different names here so that the examples can coexist:

```sql
-- Persistent function in the current REST catalog.
CREATE FUNCTION default.simple_udf
AS 'com.example.SimpleUdf'
USING JAR '/path/to/SimpleUdf.jar';

-- Session-scoped function: no database prefix on its name.
CREATE TEMPORARY FUNCTION temporary_udf
AS 'com.example.SimpleUdf'
USING JAR '/path/to/SimpleUdf.jar';

CREATE OR REPLACE TEMPORARY FUNCTION temporary_udf
AS 'com.example.SimpleUdf'
USING JAR '/path/to/SimpleUdf.jar';

DESCRIBE FUNCTION EXTENDED default.simple_udf;
DROP FUNCTION default.simple_udf;
DROP TEMPORARY FUNCTION temporary_udf;
```

To attach more than one JAR, separate resources with commas:
`USING JAR '/path/to/udf.jar', JAR '/path/to/dependency.jar'`.

### SQL Function

Define reusable scalar functions with a pure SQL body. The definition is persisted in the Paimon catalog.

This feature requires Spark 4.0 or higher. Table functions (`RETURNS TABLE(...)`) are not supported yet.

**Example**

```sql
-- Create Function (expression body)
CREATE FUNCTION area(width DOUBLE, height DOUBLE)
RETURNS DOUBLE
RETURN width * height;

SELECT area(3.0, 4.0);
-- 12.0

-- Query body: assumes emp has columns salary and dept_id.
CREATE FUNCTION dept_total(d INT) RETURNS INT
RETURN SELECT SUM(salary) FROM emp WHERE dept_id = d;

-- Parameter with DEFAULT value
CREATE FUNCTION addd(x INT, y INT DEFAULT 10) RETURNS INT RETURN x + y;

-- Create or Replace / If Not Exists
CREATE OR REPLACE FUNCTION inc(x INT) RETURNS INT RETURN x + 100;
CREATE FUNCTION IF NOT EXISTS inc(x INT) RETURNS INT RETURN x + 1;

-- Describe / Show / Drop Function
DESCRIBE FUNCTION EXTENDED area;
SHOW USER FUNCTIONS;
DROP FUNCTION IF EXISTS area;
```
