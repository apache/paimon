---
title: "Data Types"
weight: 7
type: docs
aliases:
- /development/data-type.html
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

# Data Types


### Supported Flink Type
This section lists all supported Flink types, which are available in package `org.apache.flink.table.types.logical`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">SQL Type</th>
      <th class="text-left" style="width: 20%">Java Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>ROW</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    <tr>
      <td><code>MAP</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    <tr>
      <td><code>CHAR(%d)</code></td>
      <td><code>CharType</code></td>
      <td>true</td>
    <tr>
      <td><code>VARCHAR(%d), STRING</code></td>
      <td><code>VarCharType</code></td>
      <td>true</td>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    <tr>
      <td><code>BINARY(%d)</code></td>
      <td><code>BinaryType</code></td>
      <td>true</td>
    <tr>
      <td><code>VARBINARY(%d)</code></td>
      <td><code>VarBinaryType</code></td>
      <td>true</td>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    <tr>
      <td><code>INT</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    <tr>
      <td><code>DECIMAL(%d, %d)</code></td>
      <td><code>DecimalType</code></td>
      <td>true</td>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    <tr>
      <td><code>TIME(%d)</code></td>
      <td><code>TimeType</code></td>
      <td>true</td>
    <tr>
      <td><code>TIMESTAMP(%d)</code></td>
      <td><code>TimestampType</code></td>
      <td>true</td>
    </tbody>
</table>

{{< hint info >}}
__Note:__
- Currently, `MULTISET` is **not supported** for all `write-mode`, 
  and `MAP` is **only supported** as a non-primary key field in a primary-keyed table.
{{< /hint >}}

### Spark Type Conversion

This section lists all supported type conversion between Spark and Flink.
All Spark's data types are available in package `org.apache.spark.sql.types`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Spark Data Type</th>
      <th class="text-left" style="width: 10%">Flink Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>StructType</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>MapType</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>ArrayType</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>BooleanType</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>ByteType</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>ShortType</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>IntegerType</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>LongType</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>FloatType</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DoubleType</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>StringType</code></td>
      <td><code>VarCharType</code>, <code>CharType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DateType</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampType</code></td>
      <td><code>TimestampType</code>, <code>LocalZonedTimestamp</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalType(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BinaryType</code></td>
      <td><code>VarBinaryType</code>, <code>BinaryType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>

{{< hint info >}}
__Note:__
- Currently, Spark's field comment cannot be described under Flink CLI.
- Conversion between Spark's `UserDefinedType` and Flink's `UserDefinedType` is not supported.
{{< /hint >}}

### Hive Type Conversion

This section lists all supported type conversion between Hive and Flink.
All Hive's data types are available in package `org.apache.hadoop.hive.serde2.typeinfo`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Hive Data Type</th>
      <th class="text-left" style="width: 10%">Flink Data Type</th>
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
      <td><code>BaseCharTypeInfo("char(%d)")</code></td>
      <td><code>CharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("string")</code></td>
      <td><code>VarCharType(VarCharType.MAX_LENGTH)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BaseCharTypeInfo("varchar(%d)")</code></td>
      <td><code>VarCharType(length), length is less than VarCharType.MAX_LENGTH</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>PrimitiveTypeInfo("date")</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampType</code></td>
      <td><code>TimestampType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalTypeInfo("decimal(%d, %d)")</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalTypeInfo("binary")</code></td>
      <td><code>VarBinaryType</code>, <code>BinaryType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>
