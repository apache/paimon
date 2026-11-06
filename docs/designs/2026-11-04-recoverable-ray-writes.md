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

# Recoverable Python / Ray append writes

Implement the agreed first stage: one bucket-unaware append table, immutable
input plan, deterministic task identities, durable prepared messages and bounded
task-count commits. LeRobot's existing multi-table writer is not made atomic by
this single-table protocol; its companion tables require a separate publication
protocol before it can report episodes as completed.

## Protocol

* `write_id` scopes a persisted ordered input plan. Each task has a stable ID,
  episode IDs and JSON input. Input manifest and processing versions are pinned.
* A worker writes files and serializes CommitMessages using the existing v14
  wire format. It conditionally creates a task result before acknowledging it.
  Concurrent attempts race for one immutable result; no pickle is read.
* Coordinators conditionally create numbered immutable batch plans containing
  the selected results and a snapshot anchor. A competing coordinator follows
  the winning plan. Publication uses the same deterministic batch identity.
* An opt-in append commit guard checks that identity against uninterrupted
  snapshot history at the same base used for each atomic publication attempt.
  Lost acknowledgements and competing coordinators must not duplicate rows.
  Expired history or incompatible identity fails closed.
* Checkpointing is optional and requires an explicitly configured external
  location outside the target table path. Without it, every invocation processes
  and appends the entire input; no durable skip or resume is promised.
* A durable receipt records the snapshot and task membership. All checkpoints,
  including detailed prepared messages, plans and receipts, remain until the
  application removes them. There is no automatic cleanup or TTL. Delete only
  after related workers/coordinators stop and recovery or replay is no longer
  needed. Unknown outcomes are never treated as uncommitted, and a resume request
  with missing checkpoints fails rather than starting a fresh append.
* Progress exposes committed, prepared and pending episodes, plus the first
  unfinished position in the ordered episode list, not the largest episode ID.

## Bounds

Only append of full input rows is supported; no PK upserts, overwrites, schema
changes, table recreation, branch switching or rollback during a write session.
No table-wide or cross-system exactly-once claim is made. Inputs must be
immutable and task groupings stable. Processors may execute more than once.
Checkpoint backends must support conditional creation (initially local and OSS
with versioning disabled); unsafe rename fallbacks are rejected. Keep prepared
data and sidecar files intact and pause orphan cleanup for the recovery window;
this change does not teach external maintenance engines to honor session files.
Do not run legacy filesystem rename-based writers concurrently: they can
overwrite a winning snapshot across processes. Recoverable writers use
conditional creation and can compete with each other. REST publication requires
the server to enforce atomic snapshot compare-and-set.

`episodes_per_task` fixes consecutive groups in the ordered input plan and cannot
change under one write ID. `commit_every_tasks` controls publication separately
and may change for unsealed work. This is grouping generic episode descriptors,
not integration with `PaimonLeRobotWriter.save_episode`; managed LeRobot table
groups remain unsupported by this single-table recovery entry point.

## Validation

Fault-inject before and after snapshot publication and after checkpoint
persistence but before acknowledgement. Verify manual checkpoint deletion,
retention without automatic cleanup, retries,
competing attempts/coordinators, empty tasks, threshold and tail batches,
out-of-order episode progress, plan mismatch, and expired-history rejection.
Run real local Ray workers in addition to direct API integration tests.
