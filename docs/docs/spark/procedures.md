---
title: "Procedures"
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

Run procedures through the `sys` namespace of a Paimon catalog, with
`PaimonSparkSessionExtensions` enabled as in [Quick Start](./quick-start#setup).

```sql
-- Select a Paimon catalog, then use its sys namespace.
USE paimon.default;
CALL sys.compact(table => 'default.T');

-- Or qualify the catalog explicitly.
CALL paimon.sys.compact(table => 'default.T');
```

Examples below assume the named tables already exist. Named arguments (`name => value`) make
optional parameters explicit; identifiers passed as arguments are SQL string literals.
Parameter types and required/optional labels below follow the procedure signatures. An optional
argument can still be required by a particular mode or combination; those constraints are
listed in its description. Examples under one procedure are alternative calls, not a script to
run in sequence.

## Find a Procedure

Choose a group, then use the page contents to jump to a procedure:

| Group | Operations |
| --- | --- |
| [Compaction and cleanup](./procedures/maintenance) | Compact files and manifests, rescale buckets, expire history, and repair metadata. |
| [Tags, branches, and rollback](./procedures/versions) | Create and manage versions, merge branches, or restore table state. |
| [Migration and copy](./procedures/migration) | Migrate Hive tables or copy Paimon files. |
| [Indexes and row IDs](./procedures/indexes) | Build or drop indexes and reassign row IDs. |
| [Consumers, views, and functions](./procedures/metadata) | Manage reader progress, partition completion, and catalog objects. |

## Procedure Index

### Compaction and Cleanup

[`compact`](./procedures/maintenance#compact),
[`compact_database`](./procedures/maintenance#compact_database),
[`compact_chain_table`](./procedures/maintenance#compact_chain_table),
[`compact_manifest`](./procedures/maintenance#compact_manifest),
[`materialize_deletion_vectors`](./procedures/maintenance#materialize_deletion_vectors),
[`rescale`](./procedures/maintenance#rescale),
[`expire_snapshots`](./procedures/maintenance#expire_snapshots),
[`expire_partitions`](./procedures/maintenance#expire_partitions),
[`remove_orphan_files`](./procedures/maintenance#remove_orphan_files),
[`remove_unexisting_files`](./procedures/maintenance#remove_unexisting_files),
[`purge_files`](./procedures/maintenance#purge_files),
[`repair`](./procedures/maintenance#repair),
[`repair_earliest_snapshot`](./procedures/maintenance#repair_earliest_snapshot)

### Tags, Branches, and Rollback

[`create_tag`](./procedures/versions#create_tag),
[`create_tag_from_timestamp`](./procedures/versions#create_tag_from_timestamp),
[`replace_tag`](./procedures/versions#replace_tag),
[`rename_tag`](./procedures/versions#rename_tag),
[`delete_tag`](./procedures/versions#delete_tag),
[`expire_tags`](./procedures/versions#expire_tags),
[`trigger_tag_automatic_creation`](./procedures/versions#trigger_tag_automatic_creation),
[`create_branch`](./procedures/versions#create_branch),
[`delete_branch`](./procedures/versions#delete_branch),
[`rename_branch`](./procedures/versions#rename_branch),
[`fast_forward`](./procedures/versions#fast_forward),
[`merge_branch`](./procedures/versions#merge_branch),
[`rollback`](./procedures/versions#rollback),
[`rollback_to_timestamp`](./procedures/versions#rollback_to_timestamp),
[`rollback_to_watermark`](./procedures/versions#rollback_to_watermark)

### Migration and Copy

[`migrate_database`](./procedures/migration#migrate_database),
[`migrate_table`](./procedures/migration#migrate_table),
[`copy`](./procedures/migration#copy)

### Indexes and Row IDs

[`rewrite_file_index`](./procedures/indexes#rewrite_file_index),
[`create_global_index`](./procedures/indexes#create_global_index),
[`drop_global_index`](./procedures/indexes#drop_global_index),
[`reassign_row_id`](./procedures/indexes#reassign_row_id)

### Consumers, Views, and Functions

[`reset_consumer`](./procedures/metadata#reset_consumer),
[`clear_consumers`](./procedures/metadata#clear_consumers),
[`mark_partition_done`](./procedures/metadata#mark_partition_done),
[`alter_view_dialect`](./procedures/metadata#alter_view_dialect),
[`create_function`](./procedures/metadata#create_function),
[`alter_function`](./procedures/metadata#alter_function),
[`drop_function`](./procedures/metadata#drop_function)

## Permissions and Policies

REST catalog access management is documented with its resource scopes and parameter contracts:

| Procedure | Reference |
| --- | --- |
| `grant_permission` | [Grant permissions](../concepts/rest/management-api#grant-permissions) |
| `revoke_permission` | [Revoke permissions](../concepts/rest/management-api#revoke-permissions) |
| `list_permissions` | [List permissions](../concepts/rest/management-api#list-permissions) |
| `create_policy` | [Row filters](../concepts/rest/management-api#create-row-filter-policies) and [column masks](../concepts/rest/management-api#create-column-masking-policies) |
| `drop_policy` | [Drop policies](../concepts/rest/management-api#drop-policies) |
| `list_policies` | [List policies](../concepts/rest/management-api#list-policies) |
