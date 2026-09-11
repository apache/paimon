---
title: "Types and Predicates"
sidebar_position: 5
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

# Types and Predicates

The Java table API exchanges `InternalRow` values. Use Paimon's internal value representations
when constructing rows or predicate literals; SQL type names do not imply that arbitrary Java
objects can be placed in a row.

## Data Types

| Paimon logical type | Internal Java value |
| --- | --- |
| `BOOLEAN` | `boolean` / `Boolean` |
| `TINYINT` | `byte` / `Byte` |
| `SMALLINT` | `short` / `Short` |
| `INT` | `int` / `Integer` |
| `BIGINT` | `long` / `Long` |
| `FLOAT` | `float` / `Float` |
| `DOUBLE` | `double` / `Double` |
| `CHAR`, `VARCHAR`, `STRING` | `org.apache.paimon.data.BinaryString` |
| `DECIMAL` | `org.apache.paimon.data.Decimal` |
| `DATE` | `int`, days since the Unix epoch |
| `TIME` | `int`, milliseconds since midnight |
| `TIMESTAMP`, `TIMESTAMP_LTZ` | `org.apache.paimon.data.Timestamp` |
| `BINARY`, `VARBINARY`, `BYTES` | `byte[]` |
| `ARRAY` | `org.apache.paimon.data.InternalArray` |
| `MAP` | `org.apache.paimon.data.InternalMap` |
| `ROW` | `org.apache.paimon.data.InternalRow` |

For example, the sample schema `(f0 STRING, f1 INT)` accepts:

```java
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;

GenericRow row = GenericRow.of(BinaryString.fromString("Alice"), 12);
```

`GenericRow` uses insert row kind by default. For changelog writes, use Paimon's
`org.apache.paimon.types.RowKind` and the table's supported change semantics. Flink's external
`Row` and `RowKind` are separate types; the [Flink API](flink-api) converts them.
See [Data Types](../concepts/data-types) for logical type definitions and additional types.

## Predicate Types

Construct a `PredicateBuilder` from `table.rowType()`. Field indexes refer to the original table
schema, even when the read uses projection.

| SQL predicate | `PredicateBuilder` method |
| --- | --- |
| `AND`, `OR` | `and`, `or` |
| `IS NULL`, `IS NOT NULL` | `isNull`, `isNotNull` |
| `IN`, `NOT IN` | `in`, `notIn` |
| `=`, `<>` | `equal`, `notEqual` |
| `<`, `<=` | `lessThan`, `lessOrEqual` |
| `>`, `>=` | `greaterThan`, `greaterOrEqual` |
| `BETWEEN` | `between` |
| `LIKE` | `like` |
| Array membership | `arrayContains` |

```java
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;

PredicateBuilder builder = new PredicateBuilder(table.rowType());
Predicate filter = PredicateBuilder.and(
        builder.equal(0, BinaryString.fromString("Alice")),
        builder.greaterOrEqual(1, 12));
```

Pass predicates to `ReadBuilder.withFilter`. Enable `TableRead.executeFilter()` when the reader
must also evaluate them per row; pruning alone can return candidate rows that do not match.
See [Java Reads](java-reading#batch-read) for a complete example.
