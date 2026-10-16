---
title: "Concurrency Control"
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

# Concurrency Control

Paimon uses optimistic concurrency control. Writers prepare new files independently, then
validate and publish their changes against the latest committed snapshot. Readers continue to
use committed snapshots while writers prepare their work.

## Commit Flow

1. Write new data files and prepare file additions and deletions.
2. Read the latest snapshot and validate the proposed changes against its file set.
3. Prepare the new snapshot and publish it atomically through the configured commit mechanism.
4. If another writer publishes first, retry against the new latest snapshot. If the changes
   conflict with the table state, reject the commit.

A file becomes visible through a committed snapshot, not merely because it exists in storage.
Compaction's file deletions are logical: older snapshots can still reference the replaced files
until [snapshot expiration](../maintenance/manage-snapshots#expire-snapshots) removes them.

| Conflict | Example | Outcome |
| --- | --- | --- |
| Snapshot conflict | Two writers try to publish snapshot `N + 1`. | The losing writer retries against the latest state, subject to the retry limits. |
| File conflict | Two compactors replace the same input file. | The stale file changes are rejected; the job must recover or recompute them. |

## Snapshot conflict

Suppose two writers both start from snapshot `N`. Writer A publishes `N + 1` first. Writer B
cannot publish a second snapshot with that ID, so it reloads the latest state, revalidates its
changes, and attempts `N + 2`. If those changes are still compatible, both writers' changes
become part of the table history.

[![Writer A publishes snapshot N plus 1. Writer B loses the same snapshot ID, reloads and validates, then publishes N plus 2 if its changes are compatible.](/img/concepts-snapshot-conflict.svg)](/img/concepts-snapshot-conflict.svg)

### Atomic Publication

The commit mechanism depends on the catalog and storage:

- **Catalog-managed snapshots:** when a catalog supports snapshot version management, Paimon
  delegates publication to the catalog. The REST Catalog provides this path through its snapshot
  commit API; the server must implement the corresponding contract.
- **Filesystem-managed snapshots:** Paimon uses the atomic-write operation provided by its
  filesystem implementation. The default implementation writes a temporary file and renames it;
  HDFS supports atomic rename. Some storage implementations provide atomic conditional creation
  instead, such as Paimon's OSS implementation. If the selected implementation cannot publish
  atomically without overwriting an existing snapshot, configure a suitable shared lock, such as
  a Hive or JDBC catalog lock with `lock.enabled = true`. Do not assume an object store's rename
  operation has HDFS semantics.

All writers of the same table must use a compatible commit mechanism and shared locking
configuration. See [Catalog](./catalog) when choosing the metadata backend.

## Files conflict

A writer validates file-level changes as well as the snapshot ID. For example, if two compactors
both replace files A and B, only one replacement can be committed. After the first succeeds,
the second still asks to delete files that are no longer live and must be rejected.

[![Two compactors replace the same input files. The first replacement commits; the second is rejected because its input files are no longer live.](/img/concepts-files-conflict.svg)](/img/concepts-files-conflict.svg)

File validation also checks other table invariants. For example, primary-key tables using the
standard LSM layout reject overlapping key ranges within the same partition, bucket, and level
above level 0. Retrying an unchanged snapshot ID alone cannot fix incompatible file changes.

In a streaming job, a failed commit can trigger recovery and a restart. Repeated conflicts can
therefore cause repeated restarts even though the commit validation protects the table state.

## Plan Concurrent Writers

- Prefer independent partitions where the workload allows it, for example streaming into the
  current partition while a batch job overwrites a historical partition.
- When multiple writers target the same files, consider moving compaction into a
  [dedicated compaction job](../maintenance/dedicated-compaction#dedicated-compaction-job).
  Set `write-only = true` for the ingestion writers and let the dedicated job perform compaction
  and snapshot expiration.
- Check the concurrency restrictions of the selected [bucket mode](../primary-key-table/data-distribution)
  and table features before running multiple writers against the same partition.

Dedicated compaction reduces conflicts caused by writers independently rewriting the same files.
It does not remove snapshot publication races or make every combination of writes, overwrites,
and table features compatible.
