---
title: "Schema Evolution and Type Mapping"
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

# Schema Evolution and Type Mapping

This page describes the Paimon CDC **actions** and CDC ingestion sinks. The schema changes that
reach Paimon depend on the source connector and event format. Flink CDC YAML pipelines have their
own schema-change handling; see the relevant pipeline connector documentation.

## Schema Change Evolution

The action compares incoming field names and types with the target schema and applies supported
changes. It does not replicate every source DDL statement.

| Source change | Effect on the Paimon target |
| --- | --- |
| Add a column | Add the new field when its schema reaches the sink. |
| Widen a string or binary type | Increase the target length. |
| Widen an integer or floating-point type | Widen within the corresponding type family, such as `INT` to `BIGINT` or `FLOAT` to `DOUBLE`. |
| Change a non-string type to a string type | Requires `--type_mapping allow-non-string-to-string`; disabled by default. |
| Drop a column | Keep the existing target column. |
| Rename a column | Treat the new name as a new column; keep the old column. |
| Rename a table | Do not rename the existing Paimon table. Source selection and routing determine whether events for the new name are consumed. |

The sink also handles decimal precision/scale changes, temporal precision changes, and compatible
nested types. These changes must pass Paimon's schema validation. A source type narrowing does
not narrow an existing target type within the string, binary, integer, or floating-point family.
Incompatible conversions can fail the job; do not assume an unsupported change will be ignored.

Column evolution does not migrate primary keys or partition keys. When reusing a target table,
make sure its schema and keys are compatible with the source and the action arguments.

## Special Data Type Mapping

The following mappings apply where the source parser exposes the corresponding type information:

| Source type or condition | Paimon type |
| --- | --- |
| MySQL `TINYINT(1)` or `BIT(1)` | `BOOLEAN` |
| MySQL `BIGINT UNSIGNED`, `BIGINT UNSIGNED ZEROFILL`, or `SERIAL` | `DECIMAL(20, 0)` |
| MySQL `BINARY(n)` | `VARBINARY(n)`; preserves the length of the binlog byte value |
| PostgreSQL `NUMERIC` without declared precision and scale | `DECIMAL(38, 18)` |
| MySQL `TIME` | `TIME`; exposed as `STRING` to Hive |

For MongoDB fields and message formats that do not carry field types, see the source guide.
Type mapping cannot recover metadata that is absent from the event.

### Mapping Options

Pass a comma-separated list using `--type_mapping`. Option names use **hyphens**:

```bash
--type_mapping tinyint1-not-bool,to-nullable
```

| Option | Effect |
| --- | --- |
| `tinyint1-not-bool` | Map MySQL `TINYINT(1)` to `TINYINT` instead of `BOOLEAN`. |
| `to-nullable` | Ignore source `NOT NULL` constraints, except for primary keys. |
| `to-string` | Map source types to `STRING` where supported by the source parser. |
| `char-to-string` | Map MySQL `CHAR(n)` and `VARCHAR(n)` to `STRING`. |
| `longtext-to-bytes` | Map MySQL `LONGTEXT` to `BYTES`. |
| `decimal-no-change` | Keep an existing target `DECIMAL` column's type unchanged during schema evolution. |
| `bigint-unsigned-to-bigint` | Map MySQL unsigned `BIGINT` variants to `BIGINT`. Use only when all values fit in a signed 64-bit integer. |
| `allow-non-string-to-string` | Allow evolution of an existing non-string column to a string type. |

MySQL-specific options do not change the behavior of unrelated source parsers. Initial type
mapping and later schema evolution are separate steps: `to-string` determines the mapped schema,
while `allow-non-string-to-string` allows an existing target column to evolve.

## Next Steps

- Add [computed columns](./action-configuration#computed-functions) when deriving partition values.
- Check the [source guide](./#choose-an-ingestion-path) for schema discovery, key requirements,
  and source-specific limits.
