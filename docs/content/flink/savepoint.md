---
title: "Savepoint"
weight: 99
type: docs
aliases:
- /flink/savepoint.html
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

# Savepoint

Paimon has its own snapshot management, this may conflict with Flink's checkpoint management, causing exceptions when
restoring from savepoint (don't worry, it will not cause the storage to be damaged).

It is recommended that you use the following methods to savepoint:

1. Use Flink [Stop with savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint).
2. Use Paimon Tag with Flink Savepoint, and rollback-to-tag before restoring from savepoint.

## Stop with savepoint

This feature of Flink ensures that the last checkpoint is fully processed, which means there will be no more uncommitted
metadata left. This is very safe, so we recommend using this feature to stop and start job.

## Tag with Savepoint

In Flink, we may consume from kafka and then write to paimon. Since flink's checkpoint only retains a limited number,
we will trigger a savepoint at certain time (such as code upgrades, data updates, etc.) to ensure that the state can
be retained for a longer time, so that the job can be restored incrementally.

Paimon's snapshot is similar to flink's checkpoint, and both will automatically expire, but the tag feature of paimon
allows snapshots to be retained for a long time. Therefore, we can combine the two features of paimon's tag and flink's
savepoint to achieve incremental recovery of job from the specified savepoint.

{{< hint warning >}}
Starting from Flink 1.15 intermediate savepoints (savepoints other than created with
[stop-with-savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint))
are not used for recovery and do not commit any side effects.

For savepoint created with [stop-with-savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint),
tags will be created automatically. For other savepoints, tags will be created after the next checkpoint succeeds.
{{< /hint >}}

**Step 1: Enable automatically create tags for savepoint.**

You can set `sink.savepoint.auto-tag` to `true` to enable the feature of automatically creating tags for savepoint.

**Step 2: Trigger savepoint.**

You can refer to [flink savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#operations)
to learn how to configure and trigger savepoint.

**Step 3: Choose the tag corresponding to the savepoint.**

The tag corresponding to the savepoint will be named in the form of `savepoint-${savepointID}`. You can refer to
[Tags Table]({{< ref "maintenance/system-tables#tags-table" >}}) to query.

**Step 4: Rollback the paimon table.**

[Rollback]({{< ref "maintenance/manage-tags#rollback-to-tag" >}}) the paimon table to the specified tag.

**Step 5: Restart from the savepoint.**

You can refer to [here](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#resuming-from-savepoints) to learn how to restart from a specified savepoint.
