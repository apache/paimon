---
title: "Recoverable Episode Writes"
description: "Write fixed episode groups with Ray and resume from external checkpoints."
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

# Recoverable Episode Writes

Use `write_episodes` to process an ordered episode list in Ray, write data files
on workers, and publish completed tasks in batches. An optional external
checkpoint lets a restarted job skip committed tasks and reuse files prepared
by workers before the driver failed.

This API appends to one existing bucket-unaware table without primary keys.
It is separate from `PaimonLeRobotWriter`: that writer's frames and companion
tables do not form one transaction, and their recovery is not provided here.
Managed LeRobot table groups are rejected. Use the existing
[LeRobot writer](./lerobot#capture-lerobot-frames-directly-into-paimon) for its
`add_frame` / `save_episode` interface; this API groups episode descriptors and
does not intercept those calls.

## Define a Fixed Input Plan

Every episode has a unique, nonempty string `episode_id` and optional JSON
application input. Supply the same ordered episode list when resuming. Source
objects must be immutable, or their versions must be pinned by that list and
the `input_manifest_id`. An OSS path whose contents can change is not a source
version.

`episodes_per_task=N` groups consecutive episodes into tasks. Only the final
task can contain fewer than N episodes. The framework derives stable task IDs
from the fixed input identity and ranges; Ray's task IDs, workers, completion
order and concurrency do not define those groups.

`commit_every_tasks=M` publishes up to M completed tasks in one batch, with a
smaller tail batch when all work finishes. These settings control different
things: N defines the recovery work unit, while M defines publication frequency.
Collecting more task results does not merge the data files those tasks wrote.

| Setting | Resume rule |
| --- | --- |
| `write_id` | Reuse the original value. It scopes this write and its checkpoints. |
| `input_manifest_id` | Keep the immutable input version unchanged. |
| Ordered episodes and their application input | Keep unchanged. |
| `processing_version` | Keep unchanged; change it for a different model, code or transformation. |
| `episodes_per_task` | Keep unchanged. |
| `commit_every_tasks` | May change for tasks not yet assigned to a sealed batch. |
| Ray concurrency and resources | May change. |

## Write with Ray

Install `pypaimon[ray]` on the driver and workers. The target table must already
exist, and the processor must return rows matching its schema. The processor
receives one task containing `episode_ids` and the original episode objects in
`episodes`. It returns an Arrow Table, RecordBatch, or an iterable of them.

The following local example assumes `default.frames` has `episode_id STRING`
and `value BIGINT` columns. A distributed cluster needs shared storage for both
the warehouse and checkpoints.

```python
import pyarrow as pa
import ray

from pypaimon.ray import write_episodes


def process_task(task):
    for episode in task["episodes"]:
        # Replace this with parsing, inference, or feature extraction.
        yield pa.table({
            "episode_id": [episode["episode_id"]],
            "value": [episode["input"]["value"]],
        })


episodes = [
    {"episode_id": "ep-%04d" % i, "input": {"value": i}}
    for i in range(43)
]

ray.init()
progress = write_episodes(
    "default.frames",
    catalog_options={"warehouse": "/shared/warehouse"},
    episodes=episodes,
    processor=process_task,
    write_id="capture-2026-11-04",
    input_manifest_id="capture-manifest-v1",
    processing_version="features-v3",
    episodes_per_task=20,
    commit_every_tasks=2,
    checkpoint_location="/shared/checkpoints",
    resume=False,
    concurrency=4,
    ray_remote_args={"num_cpus": 1},
)
```

This creates tasks of 20, 20 and 3 episodes. Two completed tasks can publish
before the last task finishes. Completion order can differ from input order.
Workers send prepared metadata rather than the table's full data to the driver.

`resume=False` starts a new checkpointed write and rejects an existing session
with that write ID. To restart, use the same arguments with `resume=True`.
Resume requires the original checkpoint; a missing or incompatible checkpoint
is an error, not permission to append the whole input again.

### Without Checkpoints

Omit `checkpoint_location` for ordinary batched writing. Every invocation
processes the whole input, including episodes written by an earlier invocation.
There is no durable skip, resume, or cross-invocation deduplication guarantee.
Do not request `resume=True` in this mode.

### External Checkpoint Storage

Checkpoints must be outside the target table's path. Their lifecycle belongs to
the application: PyPaimon does not automatically delete checkpoints or apply a
retention policy, including after successful commits.

For OSS checkpoints, use a shared external prefix such as
`oss://checkpoint-bucket/robot-pipeline` and pass the required FileIO settings
through `checkpoint_options`. Target-table credentials do not automatically
grant access to this location. Driver and workers need access to both stores.
Keep credentials out of episode inputs and source-version identifiers, which
are persisted as part of the input plan.

Checkpoint publication requires an atomic create-if-absent operation. The
provided implementations support local filesystems and OSS buckets with
versioning disabled. OSS requires permission to inspect bucket versioning;
enabled or suspended versioning, unavailable permissions, and unsupported
backends fail closed. Do not change bucket versioning during a write.

## Resume and Inspect Business Progress

Recovery first resolves sealed batches. A prepared task is reusable without
rerunning its processor; a committed task is skipped. Only tasks without a
complete prepared result are scheduled again.

| Persisted state | Recovery action |
| --- | --- |
| No complete task checkpoint | Run the task again. |
| Prepared files and CommitMessages, no batch yet | Reuse the task result in a new batch. |
| Sealed batch, no confirmed publication | Resolve its original commit before forming a replacement batch. |
| Snapshot published, response or checkpoint receipt lost | Find the matching publication and record its result without appending again. |
| Durable batch receipt | Skip its tasks. |

Batch identities and task membership are immutable once sealed. A competing
attempt can prepare another set of files, but only one complete task result is
selected. The commit path checks the selected batch identity against retained
snapshot history at the same base used for atomic publication.

Progress contains ordered `committed_tasks`, `prepared_tasks`, `pending_tasks`
and corresponding `committed_episodes`, `prepared_episodes`, `pending_episodes`.
`resume_position` is the first unfinished position after the contiguous
committed prefix of the input episode list. It is not the largest completed
episode ID. For example, if A and C are committed but B is missing, B remains
pending and the resume position must not skip it.

Applications do not need to query the data table and remove completed episodes
themselves. The framework checks task metadata, using the fixed task-to-episode
mapping. There is no remote checkpoint lookup for each frame or episode.

For custom schedulers, use `RecoverableWriteSession` from
`pypaimon.write.recoverable_write`. It exposes `prepare_task`, `write_task`,
`accept_task`, `accept_task_id`, `commit_ready`, `recover`, `finish` and `progress`. Workers prepare
task results; a coordinator publishes them. `finish()` flushes the tail and
requires the complete input plan to have committed.

For example, recover the earlier write in a single Python process:

```python
from pypaimon import CatalogFactory
from pypaimon.write.recoverable_write import RecoverableWriteSession

table = CatalogFactory.create({"warehouse": "/shared/warehouse"}).get_table("default.frames")
session = RecoverableWriteSession(
    table,
    write_id="capture-2026-11-04",
    episodes=episodes,
    input_manifest_id="capture-manifest-v1",
    processing_version="features-v3",
    episodes_per_task=20,
    commit_every_tasks=2,
    checkpoint_location="/shared/checkpoints",
    resume=True,
)
state = session.recover()
session.commit_ready()
for task_id in state["pending_tasks"]:
    session.write_task(task_id, process_task)
    session.commit_ready()
progress = session.finish()
```

With independently scheduled workers, notify the coordinator after each
completed task. With durable checkpoints, call `coordinator.accept_task_id(task_id)`
before `commit_ready()` so it loads the worker's result. Without checkpoints,
return the object from `worker.prepare_task(...)` and pass it to
`coordinator.accept_task(result)`. The Ray entry point handles both forms.
`progress()` reports known completion receipts; call `recover()` first when a
previous commit's outcome is uncertain. Resume always receives the complete
original episode list; do not slice it at `resume_position` or regroup the tail.

## Limits and Cleanup

* This is full-row append recovery for one bucket-unaware table. Primary-key
  upserts, overwrites, partial updates and multi-table publication are outside
  its scope. Existing `write_paimon` and `PaimonLeRobotWriter` behavior is
  unchanged.
* This entry point uses Python write and commit execution, including when the
  table enables native execution. Native commit recovery is not implemented.
* Recovery applies within the same write identity and fixed input plan. Starting
  another write ID does not deduplicate overlapping business episodes. Business
  keys are not storage-enforced primary keys.
* Processors may run more than once before their result is durable. External
  side effects such as notifications or writes to another database are not
  covered by Paimon commit idempotence.
* Do not alter the target schema, recreate or roll back the table, or change
  branches during the write or its recovery window.
* Do not run legacy filesystem rename-based writers concurrently with a
  recoverable write. Their publication can overwrite another process's winning
  snapshot, defeating the recovery guarantee. Recoverable writers use
  conditional creation and can safely compete with each other. For REST
  catalogs, atomic publication depends on the server correctly enforcing its
  snapshot compare-and-set contract; this client cannot supply a missing
  server-side guarantee.
* Keep the snapshot anchor and intervening history needed by unresolved batches.
  If history has expired and publication cannot be established, recovery fails
  instead of assuming the batch was never committed.
* Keep prepared data files and all their sidecar files intact, and pause orphan
  cleanup for the recovery window. Prepared files are not yet referenced by a
  published snapshot, and external maintenance engines do not learn about them
  merely because a checkpoint exists. Competing attempts can leave unselected
  files for later orphan cleanup.
* All checkpoint records are retained, including prepared messages and completion
  receipts. Delete them manually only after all related workers and coordinators
  have stopped and recovery or replay is no longer needed. Deleting them removes
  the ability to skip completed tasks. A resume request then fails; deliberately
  starting a new write processes the input again.
* Deleting checkpoint metadata does not delete table data. Do not manually
  remove data files from a possibly committed batch. Reclaim orphan files using
  the normal maintenance workflow after resolving publication and stopping the
  writers.

For LeRobot table groups, frames, episodes, tasks, information and statistics
need a group publication protocol before episodes can be reported as completed
across all components. Do not wrap their independent commits in this single-table
API and infer cross-table atomicity.
