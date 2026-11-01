---
title: "Schema"
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

# Schema

A schema file defines the fields, keys, and options of a table at a particular schema ID. Readers
use schema IDs in [snapshots](./snapshot) and [data file metadata](./manifest#data-file-metadata)
to interpret records written before or after a schema change.

## Schema ID and Format Version

These two numbers serve different purposes:

- **`id`** identifies a table schema. IDs start at `0`; an update creates a new schema ID.
- **`version`** identifies the schema JSON format. The current format version is `3`.

In the default layout, schema ID `0` is stored in `schema/schema-0`. A file named `schema-3`
does not imply format version `3`.

## JSON Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `version` | Integer | Schema JSON format version. See [Compatibility](#compatibility). |
| `id` | Long | Schema ID, used in the file name and metadata references. |
| `fields` | Array of DataField | Ordered table fields, including stable field IDs. |
| `highestFieldId` | Integer | Highest field ID allocated, including nested fields; used when allocating IDs for new fields. |
| `partitionKeys` | Array of strings | Names of the partition fields. |
| `primaryKeys` | Array of strings | Names of the primary-key fields; empty for a table without a primary key. |
| `options` | Map of strings to strings | Table options. Map entry order has no meaning. |
| `comment` | String, optional | Table comment. |
| `timeMillis` | Long | Schema creation time in milliseconds since the Unix epoch. |

## Example

This example is schema ID `0`, serialized with format version `3`:

```json
{
  "version" : 3,
  "id" : 0,
  "fields" : [ {
    "id" : 0,
    "name" : "order_id",
    "type" : "BIGINT NOT NULL"
  }, {
    "id" : 1,
    "name" : "order_name",
    "type" : "STRING"
  }, {
    "id" : 2,
    "name" : "order_user_id",
    "type" : "BIGINT"
  }, {
    "id" : 3,
    "name" : "order_shop_id",
    "type" : "BIGINT"
  } ],
  "highestFieldId" : 3,
  "partitionKeys" : [ ],
  "primaryKeys" : [ "order_id" ],
  "options" : {
    "bucket" : "5"
  },
  "comment" : "",
  "timeMillis" : 1720496663041
}
```


## Compatibility

Older schema files can omit options whose defaults have since changed. Readers preserve their
original behavior when decoding those schemas:

| Schema format | Missing field or option | Reader behavior |
| --- | --- | --- |
| No `version` field | `version` | Treat as format version `1`. |
| Version `1` | `bucket` | Supply `bucket = 1`. |
| Versions `1` and `2` | `file.format` | Supply `file.format = orc`. |
| Older files without a timestamp | `timeMillis` | Use `0`. |

These compatibility defaults do not replace options explicitly stored in the schema. New tables
use current defaults, including Parquet as the default data file format.

## DataField

A DataField describes one column, including fields nested inside a row type.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | Integer | Stable field identifier used for schema evolution. |
| `name` | String | Field name. |
| `type` | String or JSON type object | Logical type, including nullability and nested type information. |
| `description` | String, optional | Field comment. |
| `defaultValue` | String, optional | Stored default value definition. Engine support determines how it is used. |
| `generationExpression` | String, optional | SQL scalar expression generating a physical top-level column. Mutually exclusive with `defaultValue`. |

Primitive types commonly use strings such as `BIGINT NOT NULL`. Nested types need their
structured type representation. See [Data Types](../data-types) for the logical type reference.

### Generated Columns

A top-level field, including a partition field, may define `generationExpression`, for example:

```json
{
  "id": 2,
  "name": "dt",
  "type": "DATE",
  "generationExpression": "CAST(event_time AS DATE)"
}
```

Writers compile expressions for omitted columns into Paimon `Transform` operations once at
initialization. These operations define the automatically generated value, including CAST
formatting and invalid-input behavior; the write engine's ANSI mode or implicit CAST rules do
not change that definition. Spark fills omitted columns through the same evaluator, including
source-column defaults, before writing.
Expressions may reference ordinary top-level fields and compose supported scalar calls, such as
`YEAR(event_time)`, `UPPER(name)`, `CONCAT(UPPER(name), '-', name)`, and
`DATE_ADD(CAST(event_time AS DATE), days)`. The expression result type must match the declared
column type. Nested field references, references to other generated columns, arbitrary SQL,
UDFs, and nondeterministic functions are not supported. `TIMESTAMP WITH LOCAL TIME ZONE` inputs
and cast targets are rejected because their results can depend on a writer's default time zone.

Generated columns are physical fields; readers use the stored values without evaluating the
expression. A cast from `TIMESTAMP WITHOUT TIME ZONE` to `DATE` uses the timestamp's calendar
date, independently of the writer's default time zone.

This is a write-time column-filling feature, not a strict computed-column constraint. Adapted
writers evaluate an expression only when the input schema omits its column. If the input schema
includes the column, its supplied value is preserved without evaluating or checking the expression,
including an explicit NULL. Normal type, NOT NULL, primary-key, and partition constraints still
apply. Stored values may therefore differ from the expression result; readers and optimizers must
not assume that the expression is an invariant or infer partition filters from it. Updating a source
column does not recompute a generated column that is already supplied in the write row.

Definitions are supported at table creation. Changing a definition or renaming, dropping, or
changing the type of any source is not supported. A generated column cannot be used as
`rowkind.field`.

All primary-key merge engines are supported. Column filling happens before merging; the merge
engine applies its normal rules to generated columns without recomputing expressions after a merge.
For example, a partial-update merge can retain an existing generated value when its source changes.
Data Evolution partial-column updates write only the selected columns. Unselected generated
columns keep their existing values, and selected generated columns accept the supplied values
without requiring their expression's source columns.

Use clients supporting this attribute for writes; older clients may ignore it. Its presence does
not establish compatibility with older writers. See [Spark DDL](../../spark/sql-ddl#generated-columns)
for SQL examples.

## Update Schema

A schema update writes a new schema file rather than replacing the old definition:

```text
my_table/
└── schema/
    ├── schema-0
    ├── schema-1
    └── schema-2
```

The latest schema defines the current table, while existing snapshots and data files can still
reference earlier schema IDs. Retain schema files that are needed to interpret existing data.
Use the supported [Flink schema changes](../../flink/sql-alter) or
[Spark schema changes](../../spark/sql-alter) instead of editing schema JSON directly.
