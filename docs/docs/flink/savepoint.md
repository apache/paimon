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
| Change the job around a Paimon table and keep its state | [Give every operator a stable UID](#operator-uids) before the change. |

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

## Operator UIDs

Flink stores state per operator and finds it again by operator id, which comes from the UID when
the operator has one. An operator without one gets an id derived from its position in the job
graph. Adding an operator upstream, or changing a parallelism that alters operator chaining, then
gives it a new id and orphans its checkpoint entry.

A Paimon source or sink is several operators. The suffix options name some of them; the
cover-all options name the rest:

| Option | Operators named |
| --- | --- |
| `sink.operator-uid.suffix` | Writer, Global Committer, dynamic-bucket-assigner |
| `sink.operator-uid.cover-all-operators` | Also the row conversions, `local merge`, the compaction operators, `Collect Statistics` and `Strip Statistics`, `INDEX_BOOTSTRAP` and `cross-partition-bucket-assigner`, and the final `end` sink |
| `source.operator-uid.suffix` | The source |
| `source.operator-uid.cover-all-operators` | Also the split monitor and reader used by dedicated split generation and exactly-once consumers, the watermark assigner, and the DataStream row conversion |

Each UID is `<operator>_<table name>_<suffix>`. The table name does not include the database, so
two tables with the same name and the same suffix in one job collide. Flink then rejects the job
with `Hash collision on user-specified ID`. Give each table its own suffix.

The `cover-all-operators` options are off by default and do nothing without the matching suffix.
Set the sink pair on a table before the first streaming job that writes it, and the source pair
before the first streaming job that reads it. The example below sets all four, for a table used
both ways:

```sql
ALTER TABLE my_table SET (
    'sink.operator-uid.suffix' = 'my_table_v1',
    'sink.operator-uid.cover-all-operators' = 'true',
    'source.operator-uid.suffix' = 'my_table_v1',
    'source.operator-uid.cover-all-operators' = 'true'
);
```

Batch jobs are not covered. They do not restore from checkpoints today, and the options name only operators that a streaming job also builds.

### How an orphaned entry fails

Flink handles an entry that no operator claims in two different ways, and the way depends on how
the job is restored:

- **From a savepoint or an explicit checkpoint path** (`execution.state-recovery.path`), Flink
  skips an unclaimed entry that holds no state and rejects one that does.
- **From the high-availability checkpoint store**, which is what a JobManager failover and the
  Flink Kubernetes Operator's `last-state` upgrade mode use, Flink rejects every unclaimed entry,
  empty or not:

  ```
  JobInitializationException: Could not start the JobMaster.
  Caused by: IllegalStateException: There is no operator for the state <operator id>
  ```

  The job then stays in a terminal state. The `allowNonRestoredState` setting of a job
  submission does not apply on this route. Nor does `execution.state-recovery.ignore-unclaimed-state`;
  Flink reads it only together with `execution.state-recovery.path`. A recovery from the HA
  checkpoint store has no way to skip an entry. Restore from a savepoint or an explicit
  checkpoint path instead.

### Migrate a running job

Turning `cover-all-operators` on changes the ids of the operators it newly covers, so their
entries in the job's existing checkpoints become unclaimed. Migrate through a savepoint, never
through a `last-state` upgrade or a JobManager failover.

1. **Stop the job with a savepoint.** Use
   [stop-with-savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint)
   and wait for it to finish.

2. **Set the options** on the table with `ALTER TABLE ... SET`, or in the job's SQL hints. Keep the
   existing suffix. Changing it renames the Writer and Global Committer UIDs too, and their state
   would be lost.

3. **Restart from the savepoint.** With `'partition.sink-strategy' = 'PARTITION_DYNAMIC'`, or a
   read with dedicated split generation or an exactly-once consumer, go to step 4 first. Otherwise
   restart with the same topology as before. The newly covered sink operators keep nothing in a
   checkpoint, so Flink skips their old entries and the restore succeeds without
   `allowNonRestoredState`. The JobManager log shows one `Skipping empty savepoint state for
   operator` line per operator that gained a UID. Confirm the job resumes committing from the
   snapshot it stopped at.

4. **Partition-dynamic append tables need one more step.** With
   `'partition.sink-strategy' = 'PARTITION_DYNAMIC'` the `Collect Statistics` operator has an
   operator coordinator, and a coordinator's entry is never empty. Restarting the normal way
   then fails with `Cannot map checkpoint/savepoint state for operator <id>...`. For that one
   restart, submit with `allowNonRestoredState`. The JobManager then logs `Skipping savepoint
   state for operator <id>` once per dropped entry. The only dropped entry that held state is
   the statistics coordinator, which rebuilds it from the records it sees next. The source,
   Writer and Global Committer already carry UIDs and keep their ids, so Flink still finds
   their state.

5. **Reads with dedicated split generation or an exactly-once consumer hold their position in
   the split monitor.** Only these read routes gain operators from
   `source.operator-uid.cover-all-operators`. Every other read already carries its UID on the
   source. A DataStream job that calls `buildForRow` gains the row conversion operator on every
   route. The monitor's entry holds the next snapshot to read, so step 3 rejects it the same
   way. Restart with `allowNonRestoredState`, and one of the dropped entries is the monitor's.
   With a [consumer ID](./consumer-id) the reader resumes from the snapshot recorded in
   the consumer and nothing is lost. Without one it starts again from the configured scan
   mode, so plan for replayed or skipped snapshots, or set the source option on a fresh job
   instead of migrating.

6. **Take a new savepoint** once the job checkpoints again. Later restores, including
   `last-state` upgrades, then start from the new UID layout.

After the migration, changing the topology around the table, for example adding a map function
in front of the sink or changing parallelism, keeps every Paimon operator's state.
