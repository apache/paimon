---
title: "Maintenance"
sidebar_position: 94
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

# Maintenance

Use these guides to manage table history, clean up stored data, run compaction jobs, and tune and monitor workloads.
Storage setup and configuration references are also collected here.

## Choose a Task

| Task | Start here |
| --- | --- |
| Reduce storage usage | [Choose a cleanup operation](#clean-up-stored-data) |
| Keep daily versions available for queries | [Automatic tag creation and retention](./manage-tags#automatic-creation) |
| Recover from an incorrect write | [Choose a recovery operation](#recover-table-data) |
| Validate changes on a separate branch | [Manage Branches](./manage-branches) |
| Run compaction separately from writers | [Dedicated Compaction](./dedicated-compaction) |
| Investigate slow writes or memory pressure | [Choose metrics](./metrics#choose-metrics-for-a-task), then see [Write Performance](./write-performance) |
| Connect to HDFS or an object store | [Filesystems](./filesystems) |
| Look up a table, catalog, or connector option | [Configurations](./configurations) |

## Clean Up Stored Data

Choose the operation based on what you want to remove:

- [Expire partitions](./manage-partitions#expiring-partitions) to remove old partitions from the latest table state.
  Physical file deletion depends on snapshot expiration.
- [Expire snapshots](./manage-snapshots#expire-snapshots) to limit retained history and remove files that are no longer
  needed. Review the retention requirements of batch queries and streaming readers before changing the policy.
- [Remove orphan files](./manage-snapshots#remove-orphan-files) to clean up files that are no longer referenced.
  Follow the cleanup guide's age cutoff to account for files being added by active writers.

[Tags](./manage-tags) preserve historical data independently of snapshot expiration. Review tag retention as part of
your storage policy. To remove empty directories left after file deletion, see the
`snapshot.clean-empty-directories` option in [Expire Snapshots](./manage-snapshots#expire-snapshots).

## Recover Table Data

Choose a recovery mode, then follow the instructions for a snapshot ID or a tag.
The linked guides include the supported engine commands and operation-specific limitations.

| Recovery mode | Effect on table history | Instructions |
| --- | --- | --- |
| Rollback | Restore the target state and remove snapshots and tags after the target. | [Snapshot](./manage-snapshots#rollback-to-snapshot) or [tag](./manage-tags#rollback-to-tag) |
| Rollback as latest | Restore the target state as a new latest snapshot, preserving later snapshots and tags. | [Snapshot](./manage-snapshots#rollback-to-snapshot-as-latest) or [tag](./manage-tags#rollback-to-tag-as-latest) |

For a workflow that validates corrected data on a separate branch before updating the main branch, see
[Manage Branches](./manage-branches) and [Fast Forward](./manage-branches#fast-forward).

## Browse by Topic

### Data Lifecycle & Versioning

- [Manage Snapshots](./manage-snapshots): retain or expire table history, roll back, and remove orphan files.
- [Manage Tags](./manage-tags): preserve named versions, automate tag retention, and restore tagged data.
- [Manage Branches](./manage-branches): create isolated branches, validate changes, and fast-forward the main branch.
- [Manage Partitions](./manage-partitions): expire partitions and mark partitions ready for downstream consumers.

### Compaction & Data Layout

- [Dedicated Compaction](./dedicated-compaction): run table or database compaction jobs and select compaction targets.
- [Rescale Bucket](./rescale-bucket): change bucket counts and reorganize existing data.

### Performance & Monitoring

- [Write Performance](./write-performance): tune parallelism, buffering, file formats, and memory usage.
- [Metrics](./metrics): access metrics through Flink and choose metrics for investigation.

### Storage & Configuration

- [Filesystems](./filesystems): install filesystem dependencies and configure storage access.
- [Configurations](./configurations): look up table, catalog, connector, and file-format options.
