---
title: "Branches and Rollback"
description: "Use branches to maintain separate table histories and rollback to restore a retained snapshot."
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

# Branches and Rollback

Use branches to maintain separate table histories and rollback to restore a retained snapshot. To pin a dataset for later reads or training, use [tags](./manage-tags). The examples operate on an existing `table`.

## Branch Management

A branch maintains a separate table history. Creating a branch without a tag
copies the current schema into an empty branch. To include existing data, create
the branch from a retained tag.

:::info

PyPaimon provides two implementations of `BranchManager`:
- **FileSystemBranchManager**: For tables accessed directly via filesystem (default for filesystem catalog)
- **CatalogBranchManager**: For tables accessed via catalog (e.g., REST catalog)

The `table.branch_manager()` method automatically returns the appropriate implementation based on the table's catalog environment.

:::

### Create Branch

Create an empty branch with the current table schema:

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({'warehouse': 'file:///path/to/warehouse'})
table = catalog.get_table('database_name.table_name')

# Create an empty branch with the current schema
table.branch_manager().create_branch('feature_branch')
```

Alternatively, create a branch that includes the data retained by an existing tag:

```python
# Create a branch from tag 'v1.0'
table.branch_manager().create_branch('feature_branch', tag_name='v1.0')
```

Create a branch and ignore if it already exists:

```python
# No error if branch already exists
table.branch_manager().create_branch('feature_branch', ignore_if_exists=True)
```

### List Branches

List all branches for a table:

```python
# Get all branch names
branches = table.branch_manager().branches()

for branch in branches:
    print(f"Branch: {branch}")
```

### Check Branch Exists

Check if a specific branch exists:

```python
if table.branch_manager().branch_exists('feature_branch'):
    print("Branch exists")
else:
    print("Branch does not exist")
```

### Drop Branch

Delete an existing branch:

```python
# Drop a branch
table.branch_manager().drop_branch('feature_branch')
```

### Rename Branch

Rename an existing branch to a new name:

```python
# Rename a branch
table.branch_manager().rename_branch('old_branch_name', 'new_branch_name')
```

:::warning

The source branch must exist and cannot be the main branch. The target branch name must be valid and not already exist.

:::

### Fast Forward

Fast forward the main branch to a specific branch:

```python
# Fast forward main to feature branch
# This is useful when you want to merge changes from a feature branch back to main
table.branch_manager().fast_forward('feature_branch')
```

:::warning

Fast forward operation is irreversible and will replace the current state of the main branch with the target branch's state.

:::

### Branch Path Structure

Paimon organizes branches in the file system as follows:

- **Main branch**: Stored directly in the table directory (e.g., `/path/to/table/`)
- **Feature branches**: Stored in a `branch` subdirectory (e.g., `/path/to/table/branch/branch-feature_branch/`)

### Branch Name Validation

Branch names have the following constraints:

- Cannot be "main" (the default branch)
- Cannot be blank or whitespace only
- Cannot be a pure numeric string
- Valid examples: `feature`, `develop`, `feature-123`, `my-branch`

## Rollback

Paimon supports rolling back a table to a previous snapshot or tag. This is useful for undoing unwanted changes or
restoring the table to a known good state.

### Rollback to Snapshot

You can rollback a table to a specific snapshot by its ID:

```python
table = catalog.get_table('database_name.table_name')

# Rollback to snapshot 3
table.rollback_to(3)  # snapshot id
```

### Rollback to Tag

You can also rollback a table to a previously created tag:

```python
table = catalog.get_table('database_name.table_name')

# Rollback to tag 'v3'
table.rollback_to('v3')  # tag name
```

The `rollback_to` method accepts either an `int` (snapshot ID) or a `str` (tag name) and automatically dispatches
to the appropriate rollback logic.
