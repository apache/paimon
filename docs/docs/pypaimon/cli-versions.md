---
title: "CLI: Tags and Branches"
sidebar_label: "Tags and Branches"
description: "Retain a dataset snapshot with a tag or work on a separate table history with a branch."
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

# CLI: Tags and Branches

Retain a dataset snapshot with a tag or work on a separate table history with a branch. Configure the CLI first, and use a table that already has committed data. Commands below illustrate separate operations; choose the ones you need.

## Tag Commands

Manage tags (named snapshots) on a table. Tags are useful for time travel and pinning a snapshot for later access.

```shell
paimon tag <create|list|get|delete> mydb.users ...
```

### Tag Create

```shell
# Tag the latest snapshot
paimon tag create mydb.users v1

# Tag a specific snapshot
paimon tag create mydb.users v1 --snapshot-id 3

# Do not error if the tag already exists
paimon tag create mydb.users v1 --ignore-if-exists
```

Options:
- `--snapshot-id, -s`: Snapshot id to tag (default: the latest snapshot)
- `--ignore-if-exists, -i`: Do not raise an error if the tag already exists

### Tag List

```shell
# List all tags
paimon tag list mydb.users

# Only tags with a name prefix
paimon tag list mydb.users --prefix prod_

# JSON output
paimon tag list mydb.users --format json
```

Options:
- `--prefix, -p`: Only list tags whose name starts with this prefix
- `--format, -f`: Output format, `table` (default) or `json`

### Tag Get

```shell
paimon tag get mydb.users v1

# JSON output
paimon tag get mydb.users v1 --format json
```

Options:
- `--format, -f`: Output format, `table` (default) or `json`

### Tag Delete

```shell
paimon tag delete mydb.users v1
```

## Branch Commands

Manage separate table histories. Create an empty branch with the current schema,
or initialize a branch with data from a retained tag. A branch can later be
fast-forwarded into main. See [Branches and Rollback](./branches) for the Python API.

```shell
paimon branch <create|list|delete|rename|fast-forward> mydb.users ...
```

### Branch Create

```shell
# Create an empty branch with the current schema
paimon branch create mydb.users b1

# Create a branch from an existing tag
paimon branch create mydb.users b1 --tag v1
```

Options:
- `--tag, -t`: Initialize the branch from this tag (default: an empty branch with the current schema)

### Branch List

```shell
# List all branches
paimon branch list mydb.users

# JSON output
paimon branch list mydb.users --format json
```

Options:
- `--format, -f`: Output format, `table` (default) or `json`

### Branch Delete

```shell
paimon branch delete mydb.users b1
```

### Branch Rename

```shell
paimon branch rename mydb.users b1 b2
```

### Branch Fast-Forward

Fast-forward the main branch to the given branch (main adopts the branch's snapshots).

```shell
paimon branch fast-forward mydb.users b1
```
