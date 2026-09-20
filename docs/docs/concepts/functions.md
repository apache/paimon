---
title: "Functions"
sidebar_position: 9
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

# Functions

A catalog function stores a reusable function definition and its metadata. The compute engine
loads and executes the definition, so supported languages and function operations depend on the
engine and catalog implementation.

## Catalog and Engine Support

The REST Catalog implements persistent function operations. The built-in Hive, JDBC, and
Filesystem catalogs do not implement persistent function creation, alteration, or deletion.

The engine adapter determines which stored definitions it can load:

| Integration | Definitions loaded by the adapter |
| --- | --- |
| Flink catalog | File functions with implementation class and resource metadata. |
| Spark V1 function interface | Java file functions. |
| Spark V2 function interface | Lambda functions with a single return value. |

## Types of Functions Supported

Paimon's function metadata can represent three definition types:

| Type | Definition |
| --- | --- |
| File function | References implementation resources, such as JAR files, with language and entry-point metadata. |
| Lambda function | Stores a lambda definition and its language. |
| SQL function | Stores a SQL definition. |

The metadata model does not imply that every engine can execute every definition type. The
following examples show Java file functions in Flink; see [Functions in Spark](#functions-in-spark)
for Spark usage.

## File Function Usage in Flink

Select a Paimon [REST Catalog](./rest/) and make the implementation JAR accessible to the Flink
job, then register the function in an existing database.

### Create Function

```sql
CREATE FUNCTION mydb.parse_str
    AS 'com.streaming.flink.udf.StrUdf'
    LANGUAGE JAVA
    USING JAR 'oss://my_bucket/my_location/udf.jar';
```

Add further `JAR` resources to the `USING` clause when the function requires additional dependencies.

### Alter Function

Change the registered implementation class:

```sql
ALTER FUNCTION mydb.parse_str
    AS 'com.streaming.flink.udf.StrUdf2'
    LANGUAGE JAVA;
```

### Drop Function

```sql
DROP FUNCTION mydb.parse_str;
```

## Functions in Spark

See [Spark SQL Functions](../spark/sql-functions#user-defined-function) for supported definitions
and examples.
