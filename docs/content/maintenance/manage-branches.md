---
title: "Manage Branches"
weight: 11
type: docs
aliases:
- /maintenance/manage-branches.html
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

# Manage Branches

In streaming data processing, it's difficult to correct data for it may affect the existing data, and users will see the streaming provisional results, which is not expected.

We suppose the branch that the existing workflow is processing on is 'main' branch, by creating custom data branch, it can help to do experimental tests and data validating for the new job on the existing table, which doesn't need to stop the existing reading / writing workflows and no need to copy data from the main branch.

By merge or replace branch operations, users can complete the correcting of data.

## Create Branches

Paimon supports creating branch from a specific tag or snapshot, or just creating an empty branch which means the initial state of the created branch is like an empty table.

{{< tabs "create-branches" >}}

{{< tab "Flink" >}}

Run the following sql:

```sql
-- create branch named 'branch1' from tag 'tag1'
CALL sys.create_branch('default.T', 'branch1', 'tag1');

-- create branch named 'branch1' from snapshot 1
CALL sys.create_branch('default.T', 'branch1', 1);

-- create empty branch named 'branch1'
CALL sys.create_branch('default.T', 'branch1');
```
{{< /tab >}}

{{< /tabs >}}

## Delete Branches

You can delete branch by its name.

{{< tabs "delete-branches" >}}

{{< tab "Flink" >}}

Run the following sql:

```sql
CALL sys.delete_branch('default.T', 'branch1');
```
{{< /tab >}}

{{< /tabs >}}

## Read / Write With Branch

You can read or write with branch as below.

{{< tabs "read-write-branch" >}}

{{< tab "Flink" >}}

```sql
-- read from branch 'branch1'
SELECT * FROM t /*+ OPTIONS('branch' = 'branch1') */;

-- write to branch 'branch1'
INSERT INTO t /*+ OPTIONS('branch' = 'branch1') */ SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Merge Branch

Merging the custom branch to main will delete all the snapshots, tags and schemas in the main branch that are created after the branch's initial tag. And copy snapshots, tags and schemas from the branch to the main branch.

{{< tabs "merge-branch" >}}

{{< tab "Flink" >}}

```sql
CALL sys.merge_branch('default.T', 'branch1');
```

{{< /tab >}}

{{< /tabs >}}

## Replace Branch

Replacing main branch with custom branch will copy the snapshots, tags and schemas which should be copied from the main branch to target branch, and then the custom branch will be the new 'main' branch.

Suppose there is a job read and write the specified branch, by replacing main branch with the created branch, we don't need to do
anything with this job, for the new branch is totally complete now, and the 'main' branch is pointed to the new branch now.

We cannot merge branch to main here because the new job will still read and write the specified branch which will be completely independent of main.

{{< tabs "replace-branch" >}}

{{< tab "Flink" >}}

```sql
CALL sys.replace_branch('default.T', 'branch1');
```

{{< /tab >}}

{{< /tabs >}}