---
title: "Action Jars"
sidebar_position: 98
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

# Action Jars

Submit the action jar to a running Flink cluster. See [Installation](./installation) for the
artifact and dependencies. For SQL `CALL` equivalents on Flink 1.18+, see [Procedures](./procedures).

## Submit an Action

Replace the placeholders with an action name and its arguments:

```bash
<FLINK_HOME>/bin/flink run \
 /path/to/paimon-flink-action-@@VERSION@@.jar \
 <action> \
 <args>
```

The following command is used to compact a table.

```bash
<FLINK_HOME>/bin/flink run \
 /path/to/paimon-flink-action-@@VERSION@@.jar \
 compact \
 --path <TABLE_PATH>
```

Use `<action> --help` to inspect its arguments. Partition specifications use comma-separated
`key=value` pairs; quote shell arguments that contain spaces or SQL expressions.

## Find an Action

| Task | Guide |
| --- | --- |
| Compact tables | [Dedicated Compaction](../maintenance/dedicated-compaction) |
| Merge records | [Merge into a table](#merging-into-table) |
| Delete matching rows | [Primary-key tables](#deleting-from-table), [Data Evolution tables](#deleting-from-a-data-evolution-table) |
| Drop partitions | [Drop Partition](#drop-partition) |
| Maintain indexes or row IDs | [Rewrite File Index](#rewrite-file-index), [Reassign Row ID](#reassign-row-id) |
| Manage readers | [Consumer ID](./consumer-id), [Query Service](./sql-lookup#query-service) |
| Ingest external changes | [CDC Ingestion](../cdc-ingestion/) |
| Recover a failed job submission | [Force Start Flink Job](#force-start-flink-job) |

## Merging into table

Paimon supports "MERGE INTO" via submitting the 'merge_into' job through `flink run`.

:::info

Important table properties setting:
1. Only [primary key table](../primary-key-table/) supports this feature.
2. The action won't produce UPDATE_BEFORE, so it's not recommended to set 'changelog-producer' = 'input'.

:::

The design referenced such syntax:
```sql
MERGE INTO target-table
  USING source_table | source-expr AS source-alias
  ON merge-condition
  WHEN MATCHED [AND matched-condition]
    THEN UPDATE SET xxx
  WHEN MATCHED [AND matched-condition]
    THEN DELETE
  WHEN NOT MATCHED [AND not_matched_condition]
    THEN INSERT VALUES (xxx)
  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
    THEN UPDATE SET xxx
  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
    THEN DELETE
```
The action's upsert clauses insert a row when its resulting primary key does not exist and
replace or merge the row when it does. Assigning a different primary key can therefore create
another row; it does not implicitly delete the row under the old key.

Run the following command to submit a 'merge_into' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <target-table> \
    [--target_as <target-table-alias>] \
    --source_table <source_table-name> \
    [--source_sql <sql> ...]\
    --on <merge-condition> \
    --merge_actions <matched-upsert,matched-delete,not-matched-insert,not-matched-by-source-upsert,not-matched-by-source-delete> \
    --matched_upsert_condition <matched-condition> \
    --matched_upsert_set <upsert-changes> \
    --matched_delete_condition <matched-condition> \
    --not_matched_insert_condition <not-matched-condition> \
    --not_matched_insert_values <insert-values> \
    --not_matched_by_source_upsert_condition <not-matched-by-source-condition> \
    --not_matched_by_source_upsert_set <not-matched-upsert-changes> \
    --not_matched_by_source_delete_condition <not-matched-by-source-condition> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

```

Pass `--source_sql` repeatedly to configure the SQL environment or create source tables before
the merge runs. Supply only the action-specific arguments for the actions you select.

### Merge Examples

The examples below assume target `T(id, price, mark)` and source `S(order_id, price, mark)`
already exist, with `id` as the target primary key. Adapt the expressions to your schemas.
Each example is an independent operation.

```bash
# Examples:
# Find all orders mentioned in the source table, then mark as important if the price is above 100
# or delete if the price is under 10.
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    matched-upsert,matched-delete \
    --matched_upsert_condition "T.price > 100" \
    --matched_upsert_set "mark = 'important'" \
    --matched_delete_condition "T.price < 10"

# For matched order rows, increase the price, and if there is no match, insert the order from the
# source table:
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    matched-upsert,not-matched-insert \
    --matched_upsert_set "price = T.price + 20" \
    --not_matched_insert_values "S.order_id, S.price, S.mark"

# For not matched by source order rows (which are in the target table and does not match any row in the
# source table based on the merge-condition), decrease the price or if the mark is 'trivial', delete them:
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    not-matched-by-source-upsert,not-matched-by-source-delete \
    --not_matched_by_source_upsert_condition "T.mark <> 'trivial'" \
    --not_matched_by_source_upsert_set "price = T.price - 20" \
    --not_matched_by_source_delete_condition "T.mark = 'trivial'"

# A --source_sql example:
# Create a temporary view S in new catalog and use it as source table
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_sql "CREATE CATALOG test_cat WITH (...)" \
    --source_sql "CREATE TEMPORARY VIEW test_cat.\`default\`.S AS SELECT order_id, price, 'important' AS mark FROM important_order" \
    --source_table test_cat.default.S \
    --on "T.id = S.order_id" \
    --merge_actions not-matched-insert\
    --not_matched_insert_values "S.order_id, S.price, S.mark"
```

### Match Categories and Expressions

| Match category | Rows affected | Expression constraints |
| --- | --- | --- |
| `matched` | Target rows that join to a source row and satisfy the optional action condition. | Conditions and assigned values can reference both tables. |
| `not-matched` | Source rows without a target match. | Conditions and inserted values cannot reference target columns. |
| `not-matched-by-source` | Target rows without a source match. | Conditions and assigned values cannot reference source columns. |

For `--matched_upsert_set`, use assignments such as `price = S.price, mark = 'important'`.
Do not qualify the column on the left side of `=`. For `--not_matched_by_source_upsert_set`,
use the same assignment form with target columns or expressions only.

For `--not_matched_insert_values`, specify values for every target column in schema order,
such as `S.order_id, S.price, S.mark`. The special value `"*"` is supported for matched upserts
and inserts only when source and target schemas match. Quote it so the shell does not expand it
to filenames.

### Source Tables and Shell Quoting

The `--source_sql` statements run in the order supplied. If they change the current catalog or
database with `USE`, the source name is resolved in that context. Otherwise qualify a source in
another namespace, for example `--source_table "my_db.S"` or
`--source_table "my_catalog.my_db.S"`. Expressions can refer to the table's short name, such as
`S.order_id`.

Quote each SQL argument so the shell passes it as one value. Within double-quoted shell strings,
escape SQL identifier backticks and dollar signs to prevent shell substitution; the temporary
view example above shows escaped backticks. Pass a literal `*` as `"*"`.

:::warning

- Choose at least one merge action.
- If both `matched-upsert` and `matched-delete` are selected, provide a condition for each.
  The same requirement applies when combining `not-matched-by-source-upsert` and
  `not-matched-by-source-delete`. Otherwise, action conditions are optional.
- A target alias must not conflict with an existing table name.

:::

For more information of 'merge_into', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    merge_into --help
```

## Deleting from table

In Flink 1.16 and previous versions, Paimon only supports deleting records via submitting the 'delete' job through `flink run`.

Run the following command to submit a 'delete' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    delete \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    --where <filter_spec> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

```

`filter_spec` is the SQL `WHERE` expression. For example:

```sql
age >= 18 AND age <= 60
animal <> 'cat'
id > (SELECT count(*) FROM employee)
```

For more information of 'delete', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    delete --help
```

## Deleting from a Data Evolution table

For a non-primary-key append table in
[Data Evolution](../multimodal-table/data-evolution) mode, Paimon supports
logically deleting matching rows through the same `delete` action used by
primary-key tables. The action dispatches to the appropriate implementation
based on the target table type. Regular append-only tables are not supported.

The action evaluates the filter against a fixed snapshot of the
`$row_tracking` system table and records matched rows in deletion vectors. It
does not rewrite existing data files or dedicated BLOB files.

:::info

The target table must:

1. have no primary key;
2. use bucket-unaware mode (`bucket = -1`);
3. enable `row-tracking.enabled`;
4. enable `data-evolution.enabled`;
5. enable `deletion-vectors.enabled`.

:::

Run the following command to submit a `delete` job:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    delete \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--source_sql <sql> [--source_sql <sql> ...]] \
    --where "<filter_spec>" \
    [--sink_parallelism <sink-parallelism>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

`filter_spec` uses Flink SQL expression syntax and is equivalent to the
predicate in a SQL `WHERE` clause. For example:

```text
last_access_time < TIMESTAMP '2026-07-01 00:00:00'
status = 'expired'
id >= 100 AND id < 200
```

`source_sql` is repeatable. Each statement is executed in order before the
target query, so any bounded table supported by Flink SQL and optional views
can be registered and referenced from a subquery in `filter_spec`. This is not
limited to a particular external system: JDBC databases, data warehouses, and
other bounded connectors can all be used as long as the corresponding
connector is available in the action job's classpath.

The subquery is also where source-side filtering belongs. In the example below,
the action reads an access-state table, selects only cold URLs, and deletes the
matching rows from the target Paimon table:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    delete \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    --source_sql "CREATE TEMPORARY TABLE access_state (
        url STRING,
        last_ingest_time TIMESTAMP(3),
        last_request_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = '<jdbc-url>',
        'table-name' = '<access-state-table>'
    )" \
    --where "url IN (
        SELECT url
        FROM access_state
        WHERE last_ingest_time < TIMESTAMP '2026-07-01 00:00:00'
          AND (last_request_time IS NULL
               OR last_request_time < TIMESTAMP '2026-07-01 00:00:00')
    )" \
    --sink_parallelism 8
```

Using a subquery avoids copying a large candidate set into a temporary Paimon
table. The external source must be bounded so the action can finish and commit
one delete snapshot.

:::warning

- This action performs a logical delete. Data Evolution compaction preserves
  those logical deletions instead of materializing them, and the legacy
  `data-evolution.compaction.rewrite-row-ids` option is no longer supported.
  Run `CALL sys.materialize_deletion_vectors(...)` separately to apply the
  resulting deletion vectors to the latest table state and assign new row IDs.
  Replaced files remain while referenced by historical snapshots or tags;
  storage is reclaimed only after those references and snapshots expire.
- Do not run multiple delete actions, or concurrent `APPEND`, `COMPACT`, or
  `OVERWRITE` operations, against the same table. A conflicting
  commit causes the action to fail instead of silently overwriting deletion
  vectors.
- Row positions are aggregated in parallel per anchor file. Deletion vectors
  are then written in parallel across independent rewrite groups. One existing
  deletion-vector index file is an atomic rewrite group and always has a single
  writer owner, because it can contain deletion vectors for several anchor
  files.
- Split large deletes into bounded batches to limit row-tracking planning,
  deletion-vector memory usage, and external-source scan size.
- Snapshot retention must preserve the action's fixed base snapshot until the
  job finishes.

:::

For more information, run:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    delete --help
```

## Drop Partition

Run the following command to submit a 'drop_partition' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    drop_partition \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition <partition_spec> [--partition <partition_spec> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

```

Use comma-separated partition fields, for example `dt=2026-09-01,hh=10`.

For more information of 'drop_partition', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    drop_partition --help
```

## Rewrite File Index

Run the following command to submit a 'rewrite_file_index' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    rewrite_file_index \
    --warehouse <warehouse-path> \
    --identifier <database.table> \
    [--partitions <partition_spec>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

For more information of 'rewrite_file_index', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    rewrite_file_index --help
```

## Reassign Row ID

Run the following command to submit a 'reassign_row_id' job for a data evolution table.
This action rewrites metadata to make partition row-id ranges non-overlapping.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    reassign_row_id \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--table_conf <paimon-table-conf> [--table_conf <paimon-table-conf> ...]] \
    [--partition <partition_spec> [--partition <partition_spec> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

```

Use comma-separated partition fields, for example `dt=2026-09-01,hh=10`.

For more information of 'reassign_row_id', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    reassign_row_id --help
```

## Force Start Flink Job

Some actions, like `create_tag`, are lightweight and by default will not be submitted as a job to Flink cluster. If you
have the need to unify the experience regardless of actions, you can use the `--force_start_flink_job` flag to make sure
submitting them as jobs. For example,

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    drop_partition \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition <partition_spec> [--partition <partition_spec> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
    --force_start_flink_job true
```
