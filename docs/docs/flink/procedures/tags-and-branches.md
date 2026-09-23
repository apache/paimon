---
title: "Tags and Branches"
sidebar_position: 3
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

# Tags and Branches

Create and manage named versions and branches.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## create_tag

To create a tag based on given snapshot. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `tagName`: name of the new tag.

- snapshotId (Long): id of the snapshot which the new tag is based on.

- `time_retained`: The maximum time retained for newly created tags.

**Syntax**

```sql
-- Use named argument
-- based on the specified snapshot
CALL [catalog.]sys.create_tag(`table` => 'identifier', tag => 'tagName', snapshot_id => snapshotId);

-- based on the latest snapshot
CALL [catalog.]sys.create_tag(`table` => 'identifier', tag => 'tagName');

-- Use indexed argument
-- based on the specified snapshot
CALL [catalog.]sys.create_tag('identifier', 'tagName', snapshotId);

-- based on the latest snapshot
CALL [catalog.]sys.create_tag('identifier', 'tagName');
```

**Example**

```sql
CALL sys.create_tag(
    `table` => 'default.T',
    tag => 'my_tag',
    snapshot_id => cast(10 as bigint),
    time_retained => '1 d'
);
```

## create_tag_from_timestamp

To create a tag based on given timestamp. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `tag`: name of the new tag.

- timestamp (Long): Find the first snapshot whose commit-time greater than this timestamp.

- `time_retained`: The maximum time retained for newly created tags.

**Syntax**

```sql
-- Create a tag from the first snapshot whose commit-time greater than the specified timestamp.
-- Use named argument
CALL [catalog.]sys.create_tag_from_timestamp(
    `table` => 'identifier',
    tag => 'tagName',
    timestamp => timestamp,
    time_retained => time_retained
);

-- Use indexed argument
CALL [catalog.]sys.create_tag_from_timestamp('identifier', 'tagName', timestamp, time_retained);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.create_tag_from_timestamp('default.T', 'my_tag', 1724404318750, '1 d');

-- for Flink 1.19 and later
CALL sys.create_tag_from_timestamp(
    `table` => 'default.T',
    `tag` => 'my_tag',
    `timestamp` => 1724404318750,
    time_retained => '1 d'
);
```

## create_tag_from_watermark

To create a tag based on given watermark timestamp. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `tag`: name of the new tag.

- watermark (Long): Find the first snapshot whose watermark greater than the specified watermark.

- `time_retained`: The maximum time retained for newly created tags.

**Syntax**

```sql
-- Create a tag from the first snapshot whose watermark greater than the specified timestamp.
-- Use named argument
CALL [catalog.]sys.create_tag_from_watermark(
    `table` => 'identifier',
    tag => 'tagName',
    watermark => watermark,
    time_retained => time_retained
);

-- Use indexed argument
CALL [catalog.]sys.create_tag_from_watermark('identifier', 'tagName', watermark, time_retained);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.create_tag_from_watermark('default.T', 'my_tag', 1724404318750, '1 d');

-- for Flink 1.19 and later
CALL sys.create_tag_from_watermark(
    `table` => 'default.T',
    `tag` => 'my_tag',
    `watermark` => 1724404318750,
    time_retained => '1 d'
);
```

## replace_tag

To replace an existing tag with new tag info. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `tag`: name of the existed tag. Cannot be empty.

- snapshot(Long):  id of the snapshot which the tag is based on, it is optional.

- `time_retained`: The maximum time retained for the existing tag, it is optional.

**Syntax**

```sql
-- Use named argument
-- replace tag with new time retained
CALL [catalog.]sys.replace_tag(`table` => 'identifier', tag => 'tagName', time_retained => 'timeRetained');

-- replace tag with new snapshot id and time retained
CALL [catalog.]sys.replace_tag(`table` => 'identifier', snapshot_id => 'snapshotId');

-- Use indexed argument
-- replace tag with new snapshot id and time retained
CALL [catalog.]sys.replace_tag('identifier', 'tagName', 'snapshotId', 'timeRetained');
```

**Example**

```sql
-- for Flink 1.18
CALL sys.replace_tag('default.T', 'my_tag', 5, '1 d');

-- for Flink 1.19 and later
CALL sys.replace_tag(`table` => 'default.T', tag => 'my_tag', snapshot_id => 5, time_retained => '1 d');
```

## rename_tag

Rename a tag. Arguments:

- `table`: the target table identifier.

- `tagName`: the existing tag name.

- `targetTagName`: the new tag name.

**Syntax**

```sql
CALL [catalog.]sys.rename_tag(`table` => 'identifier', tagName => 'tagName', targetTagName => 'newTagName');
```

**Example**

```sql
CALL sys.rename_tag(`table` => 'default.T', tagName => 'tag1', targetTagName => 'tag2');
```

## delete_tag

To delete a tag. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `tagName`: name of the tag to be deleted. If you specify multiple tags, delimiter is ','.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.delete_tag(`table` => 'identifier', tag => 'tagName');

-- Use indexed argument
CALL [catalog.]sys.delete_tag('identifier', 'tagName');
```

**Example**

```sql
CALL sys.delete_tag(`table` => 'default.T', tag => 'my_tag');
```

## expire_tags

To expire tags by time. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `older_than`: tagCreateTime before which tags will be removed.

**Syntax**

```sql
CALL [catalog.]sys.expire_tags('identifier', 'older_than');
```

**Example**

```sql
CALL sys.expire_tags(table => 'default.T', older_than => '2024-09-06 11:00:00');
```

## trigger_tag_automatic_creation

Trigger the tag automatic creation. Arguments:

- `table`: the target table identifier. Cannot be empty.

**Syntax**

```sql
CALL [catalog.]sys.trigger_tag_automatic_creation('identifier');
```

**Example**

```sql
CALL sys.trigger_tag_automatic_creation(table => 'default.T');
```

## create_branch

To create a branch based on given tag, or just create empty branch. Arguments:

- `table`: the target table identifier or branch identifier. Cannot be empty.

- `branch`: name of the new branch.

- `tag`: name of the tag which the new branch is based on.

- `ignoreIfExists`: ignore if branch exists, default is false.

**Syntax**

```sql
CALL [catalog.]sys.create_branch(`table` => 'identifier', branch => 'branchName', tag => 'tagName');
```

**Example**

```sql
CALL sys.create_branch(`table` => 'default.T', branch => 'branch1', tag => 'tag1');

-- based on the specified branch's tag
CALL sys.create_branch(`table` => 'default.T$branch_existBranchName', branch => 'branch1', tag => 'tag1');

CALL sys.create_branch(`table` => 'default.T', branch => 'branch1');
```

## delete_branch

To delete a branch. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `branchName`: name of the branch to be deleted. If you specify multiple branches, delimiter is ','.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.delete_branch(`table` => 'identifier', branch => 'branchName');

-- Use indexed argument
CALL [catalog.]sys.delete_branch('identifier', 'branchName');
```

**Example**

```sql
CALL sys.delete_branch(`table` => 'default.T', branch => 'branch1');
```

## rename_branch

To rename a branch. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `from_branch`: name of the branch to be renamed.

- `to_branch`: new name of the branch.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.rename_branch(
    `table` => 'identifier',
    from_branch => 'branchName',
    to_branch => 'newBranchName'
);

-- Use indexed argument
CALL [catalog.]sys.rename_branch('identifier', 'branchName', 'newBranchName');
```

**Example**

```sql
CALL sys.rename_branch(`table` => 'default.T', from_branch => 'branch1', to_branch => 'branch2');
```

## fast_forward

To fast_forward a branch to main branch. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `branchName`: name of the branch to be merged.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.fast_forward(`table` => 'identifier', branch => 'branchName');

-- Use indexed argument
CALL [catalog.]sys.fast_forward('identifier', 'branchName');
```

**Example**

```sql
CALL sys.fast_forward(`table` => 'default.T', branch => 'branch1');
```

## merge_branch

Merge data files from source branch into target branch for append-only tables. The table must be created with `'branch-merge.enabled' = 'true'`. This option enforces a pure-append table history by rejecting compaction and INSERT OVERWRITE, and it is incompatible with deletion vectors. Requires compatible schema history and consistent row-tracking settings between source and target. Arguments:

- `table`: the table identifier. Cannot be empty.

- `source_branch`: name of the source branch to merge from. Cannot be empty.

- `target_branch` (optional): name of the target branch to merge into. Default is 'main'.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.merge_branch(`table` => 'identifier', source_branch => 'sourceBranchName');

CALL [catalog.]sys.merge_branch(
    `table` => 'identifier',
    source_branch => 'sourceBranchName',
    target_branch => 'targetBranchName'
);
```

**Example**

```sql
CALL sys.merge_branch(`table` => 'default.T', source_branch => 'branch1');

CALL sys.merge_branch(`table` => 'default.T', source_branch => 'branch1', target_branch => 'branch2');
```
