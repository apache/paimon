---
title: "Tags, Branches, and Rollback"
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

# Tags, Branches, and Rollback

Manage retained versions and branches. For SQL tag DDL, see
[Create Tables, Views, and Tags](../sql-ddl#tag).
For retention and branch behavior, see [Manage Tags](../../maintenance/manage-tags) and
[Manage Branches](../../maintenance/manage-branches).

For catalog selection and invocation syntax, see [Procedures](../procedures).

## create_tag

Create a tag based on given snapshot.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `tag` (`STRING`, required): name of the new tag.
- `snapshot` (`BIGINT`, optional): id of the snapshot which the new tag is based on.
- `time_retained` (`STRING`, optional): The maximum time retained for newly created tags.

```sql
-- based on snapshot 10 with 1d
CALL sys.create_tag(table => 'default.T', tag => 'my_tag', snapshot => 10, time_retained => '1 d');

-- based on the latest snapshot
CALL sys.create_tag(table => 'default.T', tag => 'my_tag');
```

## create_tag_from_timestamp

Create a tag based on given timestamp.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `tag` (`STRING`, required): name of the new tag.
- `timestamp` (`BIGINT`, optional): Find the first retained snapshot (including tagged snapshots) whose commit time is at or after this Unix timestamp in milliseconds.
- `time_retained` (`STRING`, optional): The maximum time retained for newly created tags.

```sql
CALL sys.create_tag_from_timestamp(
  `table` => 'default.T',
  `tag` => 'my_tag',
  `timestamp` => 1724404318750,
  time_retained => '1 d'
);
```

## replace_tag

Replace an existing tag with new tag info.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `tag` (`STRING`, required): name of the existed tag.
- `snapshot` (`BIGINT`, optional): id of the snapshot which the tag is based on, it is optional.
- `time_retained` (`STRING`, optional): The maximum time retained for the existing tag, it is optional.

```sql
CALL sys.replace_tag(table => 'default.T', tag => 'tag1', snapshot => 10, time_retained => '1 d');
```

## rename_tag

Rename a tag with a new tag name.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `tag` (`STRING`, required): name of the tag.
- `target_tag` (`STRING`, required): the new tag name to rename.

```sql
CALL sys.rename_tag(table => 'default.T', tag => 'tag1', target_tag => 'tag2');
```

## delete_tag

Delete a tag.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `tag` (`STRING`, required): name of the tag to be deleted. If you specify multiple tags, delimiter is ','.

```sql
CALL sys.delete_tag(table => 'default.T', tag => 'my_tag');
```

## expire_tags

Expire tags by time.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `older_than` (`STRING`, optional): tagCreateTime before which tags will be removed.

```sql
CALL sys.expire_tags(table => 'default.T', older_than => '2024-09-06 11:00:00');
```

## trigger_tag_automatic_creation

Trigger the tag automatic creation.

**Arguments**

- `table` (`STRING`, required): the target table identifier.

```sql
CALL sys.trigger_tag_automatic_creation(table => 'default.T');
```

## create_branch

Create an empty branch, or create a branch from an existing tag. Without `tag`, the new branch
contains the table schema and no data.

**Arguments**

- `table` (`STRING`, required): the source table or branch identifier.
- `branch` (`STRING`, required): the name of the new branch.
- `tag` (`STRING`, optional): an existing tag in the source table or branch to start from.
- `ignoreIfExists` (`STRING`, optional): STRING parsed as a boolean. Use 'true' to ignore an existing branch. Default is 'false'.

```sql
CALL sys.create_branch(table => 'test_db.T', branch => 'test_branch');

CALL sys.create_branch(table => 'test_db.T', branch => 'test_branch', tag => 'my_tag');

CALL sys.create_branch(
  table => 'test_db.T$branch_existBranchName',
  branch => 'test_branch',
  tag => 'my_tag'
);
```

## delete_branch

Delete one or more branches.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `branch` (`STRING`, required): names of the branches to delete, separated by commas.

```sql
CALL sys.delete_branch(table => 'test_db.T', branch => 'test_branch');
```

## rename_branch

Rename a branch.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `from_branch` (`STRING`, required): name of the branch to be renamed.
- `to_branch` (`STRING`, required): new name of the branch.

```sql
CALL sys.rename_branch(table => 'test_db.T', from_branch => 'test_branch', to_branch => 'new_branch');
```

## fast_forward

Fast_forward a branch to main branch.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `branch` (`STRING`, required): name of the branch to be merged.

```sql
CALL sys.fast_forward(table => 'test_db.T', branch => 'test_branch');
```

## merge_branch

Merge data files from source branch into target branch for append-only tables. The table must be
created with `'branch-merge.enabled' = 'true'`. This option enforces a pure-append table history by
rejecting compaction and INSERT OVERWRITE, and it is incompatible with deletion vectors. Requires
compatible schema history and consistent row-tracking settings between source and target.

**Arguments**

- `table` (`STRING`, required): the table identifier.
- `source_branch` (`STRING`, required): name of the source branch to merge from.
- `target_branch` (`STRING`, optional): name of the target branch to merge into. Default is 'main'.

```sql
CALL sys.merge_branch(table => 'test_db.T', source_branch => 'branch1');

CALL sys.merge_branch(table => 'test_db.T', source_branch => 'branch1', target_branch => 'branch2');
```

## rollback

Roll back to a retained snapshot or tag. Specify exactly one of `snapshot`, `tag`, or the legacy
`version` argument.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `version` (`STRING`, optional): Legacy snapshot ID or tag name. Prefer `snapshot` or `tag` for new calls.
- `snapshot` (`BIGINT`, optional): snapshot that will roll back to.
- `tag` (`STRING`, optional): tag that will roll back to.

```sql
CALL sys.rollback(table => 'default.T', version => 'my_tag');

CALL sys.rollback(table => 'default.T', version => 10);

CALL sys.rollback(table => 'default.T', tag => 'tag1');
CALL sys.rollback(table => 'default.T', snapshot => 2);
```

## rollback_to_timestamp

Rollback to the snapshot which earlier or equal than timestamp.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `timestamp` (`BIGINT`, required): roll back to the snapshot which earlier or equal than timestamp.

```sql
CALL sys.rollback_to_timestamp(table => 'default.T', timestamp => 1730292023000);
```

## rollback_to_watermark

Rollback to the snapshot which earlier or equal than watermark.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `watermark` (`BIGINT`, required): roll back to the snapshot which earlier or equal than watermark.

```sql
CALL sys.rollback_to_watermark(table => 'default.T', watermark => 1730292023000);
```
