---
title: "Migration and Copy"
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

# Migration and Copy

Migrate Hive tables or copy existing Paimon table files. See
[Migration from Hive](../../migration/migration-from-hive) for migration prerequisites.
Import CSV, JSON, or Parquet files, use [COPY INTO](../copy-into).

For catalog selection and invocation syntax, see [Procedures](../procedures).

## migrate_database

Migrate all hive tables in database to paimon tables.

**Arguments**

- `source_type` (`STRING`, required): the origin database's type to be migrated, such as hive.
- `database` (`STRING`, required): name of the origin database to be migrated.
- `options` (`STRING`, optional): the table options of the paimon table to migrate.
- `options_map` (`MAP<STRING, STRING>`, optional): Options map for adding key-value options which is a map.
- `parallelism` (`INT`, optional): the parallelism for migrate process, default is core numbers of machine.

```sql
CALL sys.migrate_database(
  source_type => 'hive',
  database => 'db01',
  options => 'file.format=parquet',
  options_map => map('k1','v1'),
  parallelism => 6
);
```

## migrate_table

Migrate hive table to a paimon table.

**Arguments**

- `source_type` (`STRING`, required): the origin table's type to be migrated, such as hive.
- `table` (`STRING`, required): name of the origin table to be migrated.
- `options` (`STRING`, optional): the table options of the paimon table to migrate.
- `target_table` (`STRING`, optional): name of the target paimon table to migrate. If not set would keep the same name with origin table
- `delete_origin` (`BOOLEAN`, optional): If had set target_table, can set delete_origin to decide whether delete the origin table metadata from hms after migrate. Default is true
- `options_map` (`MAP<STRING, STRING>`, optional): Options map for adding key-value options which is a map.
- `parallelism` (`INT`, optional): the parallelism for migrate process, default is core numbers of machine.

```sql
CALL sys.migrate_table(
  source_type => 'hive',
  table => 'default.T',
  options => 'file.format=parquet',
  options_map => map('k1','v1'),
  parallelism => 6
);
```

## copy

Copy Paimon table files.

**Arguments**

- `source_table` (`STRING`, required): the source table identifier.
- `target_table` (`STRING`, required): the target table identifier.
- `where` (`STRING`, optional): partition predicate. Omit for all partitions.

```sql
CALL sys.copy(source_table => "t1", target_table => "t1_copy");

CALL sys.copy(source_table => "t1", target_table => "t1_copy", where => "day = '2025-08-17'");
```
