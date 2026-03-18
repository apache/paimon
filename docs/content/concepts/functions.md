---
title: "Functions"
weight: 9
type: docs
aliases:
- /concepts/functions.html
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

Paimon introduces a Function abstraction designed to support functions in a standard format for compute engine, addressing:

- **Unified Column-Level Filtering and Processing:** Facilitates operations at the column level, including tasks such as encryption and decryption of data.

- **Parameterized View Capabilities:** Supports parameterized operations within views, enhancing the dynamism and usability of data retrieval processes.

## Types of Functions Supported

Currently, Paimon supports three types of functions:

1. **File Function:** Users can define functions within a file, providing flexibility and modular support for function definition.

2. **Lambda Function:** Empowering users to define functions using Java lambda expressions, enabling inline, concise, and functional-style operations.

3. **SQL Function:** Users can define functions directly within SQL, which integrates seamlessly with SQL-based data processing.

## File Function Usage in Flink

Paimon functions can be utilized within Apache Flink to execute complex data operations. Below are the SQL commands for creating, altering, and dropping functions in Flink environments.

### Create Function

To create a new function in Flink SQL:

```sql
-- Flink SQL
CREATE FUNCTION mydb.parse_str
    AS 'com.streaming.flink.udf.StrUdf' 
    LANGUAGE JAVA
    USING JAR 'oss://my_bucket/my_location/udf.jar' [, JAR 'oss://my_bucket/my_location/a.jar'];
```

This statement creates a Java-based user-defined function named `parse_str` within the `mydb` database, utilizing specified JAR files from an object storage location.

### Alter Function

To modify an existing function in Flink SQL:

```sql
-- Flink SQL
ALTER FUNCTION mydb.parse_str
    AS 'com.streaming.flink.udf.StrUdf2' 
    LANGUAGE JAVA;
```

This command changes the implementation of the `parse_str` function to use a new Java class definition.

### Drop Function

To remove a function from Flink SQL:

```sql
-- Flink SQL
DROP FUNCTION mydb.parse_str;
```

This statement deletes the existing `parse_str` function from the `mydb` database, relinquishing its functionality.

## Functions in Spark

see [SQL Functions]({{< ref "spark/sql-functions#user-defined-function" >}})
