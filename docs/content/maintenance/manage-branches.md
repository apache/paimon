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

{{< tab "Flink Action Jar" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    create_branch \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --branch_name <branch-name> \
    [--tag_name <tag-name>] \
    [--snapshot <snapshot_id>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
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

{{< tab "Flink Action Jar" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    delete_branch \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --branch_name <branch-name> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```
{{< /tab >}}

{{< /tabs >}}

## Read / Write With Branch

You can read or write with branch as below.

{{< tabs "read-write-branch" >}}

{{< tab "Flink" >}}

```sql
-- read from branch 'branch1'
SELECT * FROM `t$branch_branch1`;
SELECT * FROM `t$branch_branch1` /*+ OPTIONS('consumer-id' = 'myid') */;

-- write to branch 'branch1'
INSERT INTO `t$branch_branch1` SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Fast Forward

Fast-Forward the custom branch to main will delete all the snapshots, tags and schemas in the main branch that are created after the branch's initial tag. And copy snapshots, tags and schemas from the branch to the main branch.

{{< tabs "fast_forward" >}}

{{< tab "Flink" >}}

```sql
CALL sys.fast_forward('default.T', 'branch1');
```

{{< /tab >}}

{{< tab "Flink Action Jar" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    fast_forward \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --branch_name <branch-name> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```
{{< /tab >}}

{{< /tabs >}}

### Batch Reading from Fallback Branch

You can set the table option `scan.fallback-branch`
so that when a batch job reads from the current branch, if a partition does not exist,
the reader will try to read this partition from the fallback branch.
For streaming read jobs, this feature is currently not supported, and will only produce results from the current branch.

What's the use case of this feature? Say you have created a Paimon table partitioned by date.
You have a long-running streaming job which inserts records into Paimon, so that today's data can be queried in time.
You also have a batch job which runs at every night to insert corrected records of yesterday into Paimon,
so that the preciseness of the data can be promised.

When you query from this Paimon table, you would like to first read from the results of batch job.
But if a partition (for example, today's partition) does not exist in its result,
then you would like to read from the results of streaming job.
In this case, you can create a branch for streaming job, and set `scan.fallback-branch` to this streaming branch.

Let's look at an example.

{{< tabs "read-fallback-branch" >}}

{{< tab "Flink" >}}

```sql
-- create Paimon table
CREATE TABLE T (
    dt STRING NOT NULL,
    name STRING NOT NULL,
    amount BIGINT
) PARTITIONED BY (dt);

-- create a branch for streaming job
CALL sys.create_branch('default.T', 'test');

-- set primary key and bucket number for the branch
ALTER TABLE `T$branch_test` SET (
    'primary-key' = 'dt,name',
    'bucket' = '2',
    'changelog-producer' = 'lookup'
);

-- set fallback branch
ALTER TABLE T SET (
    'scan.fallback-branch' = 'test'
);

-- write records into the streaming branch
INSERT INTO `T$branch_test` VALUES ('20240725', 'apple', 4), ('20240725', 'peach', 10), ('20240726', 'cherry', 3), ('20240726', 'pear', 6);

-- write records into the default branch
INSERT INTO T VALUES ('20240725', 'apple', 5), ('20240725', 'banana', 7);

SELECT * FROM T;
/*
+------------------+------------------+--------+
|               dt |             name | amount |
+------------------+------------------+--------+
|         20240725 |            apple |      5 |
|         20240725 |           banana |      7 |
|         20240726 |           cherry |      3 |
|         20240726 |             pear |      6 |
+------------------+------------------+--------+
*/

-- reset fallback branch
ALTER TABLE T RESET ( 'scan.fallback-branch' );

-- now it only reads from default branch
SELECT * FROM T;
/*
+------------------+------------------+--------+
|               dt |             name | amount |
+------------------+------------------+--------+
|         20240725 |            apple |      5 |
|         20240725 |           banana |      7 |
+------------------+------------------+--------+
*/
```

{{< /tab >}}

{{< /tabs >}}
