---
title: "Schema Evolution on Write"
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

# Schema Evolution on Write

When `write.merge-schema` is enabled, Paimon automatically evolves the table schema during write to accommodate new columns in the incoming data, while preserving data integrity.

:::info

Since the table schema may be updated during writing, catalog caching needs to be disabled to use this feature. Configure `spark.sql.catalog.<catalogName>.cache-enabled` to `false`.

:::

## How It Evolves the Schema

Three options control how aggressively the schema evolves; each only takes effect when the previous one is enabled:

| Option | Description |
| --- | --- |
| `write.merge-schema` | If true, evolve the table schema to accept new columns from the incoming data. Existing column types are preserved and incoming values are cast to them; to also widen existing types, enable `write.merge-schema.type-widening`. |
| `write.merge-schema.type-widening` | Only effective when `write.merge-schema` is true. If true, widen an existing column type when the incoming data has a wider compatible type (e.g. INT -> BIGINT, DECIMAL precision increase). Lossy changes are still rejected unless `write.merge-schema.explicit-cast` is also true. |
| `write.merge-schema.explicit-cast` | Only effective when `write.merge-schema.type-widening` is true. If true, also allow lossy type changes between compatible types (e.g. BIGINT -> INT, STRING -> DATE). |

## Examples

DataFrame batch write:

```scala
data.write
  .format("paimon")
  .mode("append")
  .option("write.merge-schema", "true")
  .saveAsTable("t")
```

Spark SQL (requires Spark 3.5+ for `BY NAME`):

```sql
SET `spark.paimon.write.merge-schema` = true;

CREATE TABLE t (a INT, b STRING);
INSERT INTO t VALUES (1, '1'), (2, '2');

INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, 3 AS c;
```

Streaming write (use the existing table's actual location):

```scala
// input is a streaming DataFrame whose columns match or extend table t.
input
  .writeStream
  .format("paimon")
  .option("checkpointLocation", "/path/to/checkpoint")
  .option("write.merge-schema", "true")
  .start("/path/to/warehouse/default.db/t")
```

## Column Alignment by Write Path

When the source schema doesn't match the target schema exactly, the behavior depends on both `write.merge-schema` and the write path. For nested struct fields, all byName paths behave the same; at the top level, `MERGE INTO *` differs from regular byName `INSERT` because `*` expansion only references target columns.

| Write path | Scenario | `merge-schema=false` (default) | `merge-schema=true` |
|------------|----------|-------------------------------|---------------------|
| **byName `INSERT`** (`INSERT INTO ... BY NAME` / `saveAsTable` / `writeTo`) | Top-level source-extra columns | Throws | Evolved into the target schema |
| | Top-level target columns missing from source | NULL-filled | NULL-filled |
| | Nested struct source-extra fields | Throws | Evolved into the target schema |
| | Nested struct target-missing fields | Throws | NULL-filled |
| **`MERGE INTO *`** (`UPDATE *` / `INSERT *`) | Top-level source-extra columns | Silently dropped (`*` only covers target columns) | Evolved into the target schema |
| | Top-level target columns missing from source | Throws | `UPDATE *` preserves current value; `INSERT *` fills `CURRENT_DEFAULT` (or NULL when no default) |
| | Nested struct source-extra fields | Throws | Evolved into the target schema |
| | Nested struct target-missing fields | Throws | `UPDATE *` preserves current value; `INSERT *` fills `CURRENT_DEFAULT` (or NULL when no default) |

Notes:
- Position-based writes (e.g. `INSERT INTO t VALUES (...)` without `BY NAME`) require an exact column count match and don't engage schema evolution; only byName writes are covered above.
- Top-level target-missing under `merge-schema=false` for byName `INSERT` mirrors Spark's `INSERT FILL` semantics — only nested missing fields throw.
- Under strict mode (`merge-schema=false`), nested source-extra fields throw to avoid silent data loss; for `MERGE INTO *` at the top level, source-extras are silently dropped because `*` never references them.

For explicit DDL changes, see [Alter Tables](./sql-alter). For default expressions, see
[Default Values](./default-value).
