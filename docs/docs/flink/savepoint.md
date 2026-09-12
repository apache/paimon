---
title: "Savepoint"
sidebar_position: 99
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

A Flink savepoint preserves job state; a Paimon snapshot preserves table state. Restoring a
writer requires these two states to agree. A savepoint alone does not retain the Paimon snapshot
or undo commits made after it.

| Recovery task | Approach |
| --- | --- |
| Stop a writer and resume from its stopping point | [Stop with savepoint](#stop-with-savepoint). |
| Retain a recovery point while the job continues writing | [Create a tag with the savepoint](#tag-with-savepoint), then roll the table back before restoring. |
| Restart a streaming reader using stored table progress | See [Consumer ID](./consumer-id). |

![Savepoint and Paimon tag preserve matching job and table states; recovery stops writers, rolls back the table, then restores the job.](/img/flink-savepoint-recovery.svg)

## Stop with savepoint

Use Flink's [stop-with-savepoint operation](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint)
to stop the writer after processing its final checkpoint. Wait for the operation to finish
successfully before resuming the job. If the table has advanced through another writer, account
for those commits before restoring the saved writer state.

## Tag with Savepoint

In Flink, we may consume from Kafka and then write to Paimon. Since Flink's checkpoint only retains a limited number,
we will trigger a savepoint at certain time (such as code upgrades, data updates, etc.) to ensure that the state can
be retained for a longer time, so that the job can be restored incrementally.

Paimon's snapshot is similar to Flink's checkpoint, and both will automatically expire, but the tag feature of Paimon
allows snapshots to be retained for a long time. Therefore, we can combine the two features of Paimon's tag and Flink's
savepoint to achieve incremental recovery of job from the specified savepoint.

**Step 1: Enable automatically create tags for savepoint.**

Set the option before submitting the writer:

```sql
ALTER TABLE my_table SET ('sink.savepoint.auto-tag' = 'true');
```

Restart an existing writer so it picks up the changed option.

**Step 2: Trigger savepoint.**

You can refer to [Flink savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#operations)
to learn how to configure and trigger savepoint.

**Step 3: Choose the tag corresponding to the savepoint.**

The tag is named `savepoint-<checkpoint-id>`, where the suffix is the numeric Flink checkpoint
ID associated with the savepoint, not its directory name. Inspect the
[Tags Table](../concepts/system-tables#tags-table) and match the tag to the savepoint you retained.

**Step 4: Rollback the paimon table.**

Stop writers to the affected table, then [roll back](../maintenance/manage-tags#rollback-to-tag)
to the matching tag. Rollback removes later snapshots and can remove later tags; verify the
chosen recovery point before executing it. Coordinate this for each Paimon table written by the job.

**Step 5: Restart from the savepoint.**

[Resume the job from the matching savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#resuming-from-savepoints).
Verify that checkpoints complete and new Paimon snapshots appear before resuming dependent work.
