---
title: "SQL Functions"
weight: 2
type: docs
aliases:
- /spark/sql-functions.html
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

This section introduce all available Paimon Spark functions.

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
```

## User-defined Function

Paimon Spark supports two types of user-defined functions: lambda functions and file-based functions.

This feature currently only supports the REST catalog.

### Lambda Function

Empowering users to define functions using Java lambda expressions, enabling inline, concise, and functional-style operations.

**Example**

```sql
-- Create Function
CALL sys.create_function(`function` => 'my_db.area_func',
  `inputParams` => '[{"id": 0, "name":"length", "type":"INT"}, {"id": 1, "name":"width", "type":"INT"}]',
  `returnParams` => '[{"id": 0, "name":"area", "type":"BIGINT"}]',
  `deterministic` => true,
  `comment` => 'comment',
  `options` => 'k1=v1,k2=v2'
);

-- Alter Function
CALL sys.alter_function(`function` => 'my_db.area_func',
  `change` => '{"action" : "addDefinition", "name" : "spark", "definition" : {"type" : "lambda", "definition" : "(Integer length, Integer width) -> { return (long) length * width; }", "language": "JAVA" } }'
);

-- Drop Function
CALL sys.drop_function(`function` => 'my_db.area_func');
```

### File Function

Users can define functions within a file, providing flexibility and modular support for function definition, only supports jar files now.

Currently, supports Spark or Hive implementations of UDFs and UDAFs, see [Spark UDFs](https://spark.apache.org/docs/latest/sql-ref-functions.html#udfs-user-defined-functions)

This feature requires Spark 3.4 or higher.

**Example**

```sql
-- Create Function or Temporary Function (Temporary function should not specify database name)
CREATE [TEMPORARY] FUNCTION <mydb>.simple_udf
AS 'com.example.SimpleUdf' 
USING JAR '/tmp/SimpleUdf.jar' [, JAR '/tmp/SimpleUdfR.jar'];

-- Create or Replace Temporary Function (Temporary function should not specify database name)
CREATE OR REPLACE [TEMPORARY] FUNCTION <mydb>.simple_udf 
AS 'com.example.SimpleUdf'
USING JAR '/tmp/SimpleUdf.jar';
       
-- Describe Function
DESCRIBE FUNCTION [EXTENDED] <mydb>.simple_udf;

-- Drop Function
DROP [TEMPORARY] FUNCTION <mydb>.simple_udf;
```
