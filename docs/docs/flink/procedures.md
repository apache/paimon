---
title: "Procedures"
sidebar_position: 1
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

# Procedures

Paimon procedures expose maintenance and metadata operations through Flink SQL `CALL`.
They are available in **Flink 1.18 and later**. Some procedures submit Flink jobs; configure the
runtime mode and parallelism before calling them. For command-line submission, see [Action Jars](./action-jars).

## Calling a Procedure

Select your Paimon catalog with `USE CATALOG`, or qualify the procedure as `catalog_name.sys.procedure_name`.
Table identifiers in the arguments are normally `database_name.table_name`.

```sql
USE CATALOG my_catalog;
SET 'execution.runtime-mode' = 'batch';

-- Flink 1.19+: use named arguments and omit optional arguments.
CALL sys.compact(`table` => 'default.t', options => 'sink.parallelism=4');

-- Flink 1.18: select a positional signature and keep its argument order.
-- Empty strings stand in for unused string arguments in this signature.
CALL sys.compact('default.t', '', '', '', 'sink.parallelism=4');
```

Named arguments require Flink 1.19+. On Flink 1.18, use a supported positional signature from
the reference; do not replace numeric or Boolean arguments with empty strings. See
[Flink CALL Statements](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/call/)
for SQL syntax.

## Argument Conventions

| Argument form | Meaning | Example |
| --- | --- | --- |
| Partition filter | Commas combine fields with AND; semicolons separate partitions with OR. | `dt=2026-09-01,hh=10;dt=2026-09-02,hh=11` |
| Options string | Comma-separated `key=value` pairs. | `sink.parallelism=4,write-buffer-size=256mb` |
| SQL predicate | An expression accepted by procedures that expose a `where` argument. | `` `where` => 'dt > 10 AND h < 20' `` |
| Signature notation | `[catalog.]` and square-bracketed arguments describe optional syntax. Replace placeholders before execution. | `[catalog.]sys.compact('table')` |

Each procedure page lists its arguments, supported signatures, and examples. Requirements and
defaults are specific to the procedure.

## Procedure Reference

| Task | Procedures |
| --- | --- |
| [Compaction and Layout](./procedures/compaction) | [`compact`](./procedures/compaction#compact), [`compact_database`](./procedures/compaction#compact_database), [`compact_chain_table`](./procedures/compaction#compact_chain_table), [`compact_manifest`](./procedures/compaction#compact_manifest), [`rescale`](./procedures/compaction#rescale), [`materialize_deletion_vectors`](./procedures/compaction#materialize_deletion_vectors), [`reassign_row_id`](./procedures/compaction#reassign_row_id) |
| [Snapshots and Retention](./procedures/snapshots) | [`expire_snapshots`](./procedures/snapshots#expire_snapshots), [`expire_changelogs`](./procedures/snapshots#expire_changelogs), [`expire_partitions`](./procedures/snapshots#expire_partitions), [`rollback_to`](./procedures/snapshots#rollback_to), [`rollback_to_as_latest`](./procedures/snapshots#rollback_to_as_latest), [`rollback_to_timestamp`](./procedures/snapshots#rollback_to_timestamp), [`rollback_to_watermark`](./procedures/snapshots#rollback_to_watermark), [`purge_files`](./procedures/snapshots#purge_files) |
| [Tags and Branches](./procedures/tags-and-branches) | [`create_tag`](./procedures/tags-and-branches#create_tag), [`create_tag_from_timestamp`](./procedures/tags-and-branches#create_tag_from_timestamp), [`create_tag_from_watermark`](./procedures/tags-and-branches#create_tag_from_watermark), [`replace_tag`](./procedures/tags-and-branches#replace_tag), [`rename_tag`](./procedures/tags-and-branches#rename_tag), [`delete_tag`](./procedures/tags-and-branches#delete_tag), [`expire_tags`](./procedures/tags-and-branches#expire_tags), [`trigger_tag_automatic_creation`](./procedures/tags-and-branches#trigger_tag_automatic_creation), [`create_branch`](./procedures/tags-and-branches#create_branch), [`delete_branch`](./procedures/tags-and-branches#delete_branch), [`rename_branch`](./procedures/tags-and-branches#rename_branch), [`fast_forward`](./procedures/tags-and-branches#fast_forward), [`merge_branch`](./procedures/tags-and-branches#merge_branch) |
| [Table Operations](./procedures/table-operations) | [`merge_into`](./procedures/table-operations#merge_into), [`data_evolution_merge_into`](./procedures/table-operations#data_evolution_merge_into), [`migrate_database`](./procedures/table-operations#migrate_database), [`migrate_table`](./procedures/table-operations#migrate_table), [`clone`](./procedures/table-operations#clone), [`copy_files`](./procedures/table-operations#copy_files), [`alter_column_default_value`](./procedures/table-operations#alter_column_default_value), [`drop_partition`](./procedures/table-operations#drop_partition), [`mark_partition_done`](./procedures/table-operations#mark_partition_done) |
| [Indexes and Search](./procedures/indexes) | [`create_global_index`](./procedures/indexes#create_global_index), [`drop_global_index`](./procedures/indexes#drop_global_index), [`full_text_search`](./procedures/indexes#full_text_search), [`vector_search`](./procedures/indexes#vector_search), [`rewrite_file_index`](./procedures/indexes#rewrite_file_index) |
| [Consumers and Query Service](./procedures/consumers) | [`reset_consumer`](./procedures/consumers#reset_consumer), [`clear_consumers`](./procedures/consumers#clear_consumers), [`query_service`](./procedures/consumers#query_service) |
| [Cleanup and Repair](./procedures/repair) | [`remove_orphan_files`](./procedures/repair#remove_orphan_files), [`remove_unexisting_files`](./procedures/repair#remove_unexisting_files), [`remove_unexisting_manifests`](./procedures/repair#remove_unexisting_manifests), [`repair`](./procedures/repair#repair), [`repair_earliest_snapshot`](./procedures/repair#repair_earliest_snapshot) |
| [Views and Functions](./procedures/catalog) | [`alter_view_dialect`](./procedures/catalog#alter_view_dialect), [`create_function`](./procedures/catalog#create_function), [`alter_function`](./procedures/catalog#alter_function), [`drop_function`](./procedures/catalog#drop_function) |
