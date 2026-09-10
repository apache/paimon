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

Primitive types commonly use strings such as `BIGINT NOT NULL`. Nested types need their
structured type representation. See [Data Types](../data-types) for the logical type reference.

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
