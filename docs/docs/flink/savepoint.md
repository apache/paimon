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

Flink maps checkpoint state to operators by id. An operator without a UID gets an id from its
place in the job graph, so adding an operator upstream, or a parallelism change that alters
chaining, gives it a new id and orphans its state.

A Paimon source or sink is several operators. The suffix options name some of them; the cover
options name the rest:

| Option | Operators named |
| --- | --- |
| `sink.operator-uid.suffix` | Writer, Global Committer, dynamic-bucket-assigner |
| `sink.operator-uid.cover-table-sink-operators` | Also the row conversions, `local merge`, the compaction operators, `Collect Statistics` and `Strip Statistics`, `INDEX_BOOTSTRAP` and `cross-partition-bucket-assigner`, and the final `end` sink |
| `source.operator-uid.suffix` | The source |
| `source.operator-uid.cover-table-source-operators` | Also the split monitor and reader used by dedicated split generation and exactly-once consumers, the watermark assigner, and the DataStream row conversion |

Each UID is `<operator>_<table name>_<suffix>`. The table name leaves out the database, so two
tables with the same name and suffix in one job collide, and Flink rejects the job with
`Hash collision on user-specified ID`. Give each table its own suffix.

The cover options do nothing without the matching suffix. Set the sink pair before the first
streaming job writes the table, and the source pair before the first streaming job reads it:

```sql
ALTER TABLE my_table SET (
    'sink.operator-uid.suffix' = 'my_table_v1',
    'sink.operator-uid.cover-table-sink-operators' = 'true',
    'source.operator-uid.suffix' = 'my_table_v1',
    'source.operator-uid.cover-table-source-operators' = 'true'
);
```

Not covered:

- The CDC sync actions: the operators they add around the table sink, such as Side Output,
  Schema Evolution and Case-insensitive Convert, and the shared operators of a database sync.
  Such jobs cannot run with `pipeline.auto-generate-uids=false`.
- Operators only a `BATCH` job builds, such as sort and clustering. Flink does not checkpoint in
  `BATCH` mode. A bounded job in `STREAMING` mode is covered.

### Migrate a running job

Turning on a cover option gives the newly covered operators new ids, so their entries in the
job's checkpoints become unclaimed. A restore from a savepoint or from
`execution.state-recovery.path` skips an unclaimed entry that holds no state and rejects one
that does. A restore from the high-availability checkpoint store, as in a JobManager failover or
a Kubernetes Operator `last-state` upgrade, rejects every unclaimed entry, and neither
`allowNonRestoredState` nor `execution.state-recovery.ignore-unclaimed-state` helps:

```
JobInitializationException: Could not start the JobMaster.
Caused by: IllegalStateException: There is no operator for the state <operator id>
```

So migrate through a savepoint:

1. **Stop the job** with
   [stop-with-savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/#stopping-a-job-with-savepoint)
   (`flink stop`) and wait for it to finish.

2. **Set the options** with `ALTER TABLE ... SET` or SQL hints. Keep the existing suffix:
   changing it renames the Writer and Global Committer too, and loses their state.
3. **Restart from the savepoint** with the same topology. The operators the sink options newly
   cover hold no state, so Flink skips their entries without `allowNonRestoredState` and logs
   `Skipping empty savepoint state for operator` once per operator. Check that the snapshot ids
   in `<table>$snapshots` continue past the one the job stopped at, under the same `commit_user`.

   Two cases drop an entry that holds state. Without `allowNonRestoredState` the restore fails
   with `Cannot map checkpoint/savepoint state for operator <id>`, so submit this one restart
   with it. Flink then logs `Skipping savepoint state for operator <id>` for each entry it skips,
   empty ones included. The Writer and Global Committer keep their UIDs, so Flink still restores
   their state.

   - **`'partition.sink-strategy' = 'PARTITION_DYNAMIC'`**: the `Collect Statistics` coordinator's
     entry is dropped. It rebuilds its statistics from the next records.
   - **A read with dedicated split generation or an exactly-once consumer**: the split monitor's
     entry is dropped. An exactly-once read needs a [consumer ID](./consumer-id) with the default
     `consumer.mode`. After `flink stop` the consumer holds the same next snapshot as the
     monitor, and the reader resumes from it, so no snapshot is skipped or read twice. This holds
     only if you stop with `flink stop` (`flink savepoint` then cancel can re-read snapshots),
     restart before `consumer.expiration-time`, and keep `consumer.ignore-progress` false. Check
     that `next_snapshot_id` in `<table>$consumers` holds, then advances.

4. **Do not restore from savepoints or checkpoints taken before the migration.** They still hold
   the old ids.

After the migration, topology changes around the table, such as a map in front of the sink or a
new parallelism, keep every Paimon operator's state on the routes above.
