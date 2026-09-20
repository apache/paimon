---
title: "RoboMIND AgileX"
sidebar_position: 7
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

# RoboMIND AgileX

The RoboMIND AgileX importer lives in `pypaimon.benchmark.act.robomind_agilex`.
It converts a downloaded HDF5 directory into a Paimon table group for direct
ACT training from a frame wide table. Tables follow `<group>__<role>` in the
`robomind` database, with `agilex` as the group name and singular role names:

| Role | Table | Contents |
| --- | --- | --- |
| info | `agilex__info` | Group identity, feature definitions, quality codes, and member table names. |
| episode | `agilex__episode` | Trajectory identity, frame ranges, split, outcome, and source metadata. |
| frame | `agilex__frame` | Frame indices, robot state, raw and canonical actions, RGB, and depth. |
| stat | `agilex__stat` | Raw train-frame statistics and the ACT normalization scope in the same row. |

The local and Ray paths share transforms and schemas. Discovery opens HDF5
headers to obtain frame counts and assigns immutable indices in sorted source
path order. Each Ray task processes a complete HDF5 file; transforms validate
its arrays and reject a frame count that changed after discovery.

## Run ingestion and training

```shell
pip install 'pypaimon[act,hdf5]'
python -m pypaimon.benchmark.act ingest \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --warehouse /data/warehouse \
  --statistics-version act-release-1
```

Use Python 3.10 or newer and a new warehouse. Input files must already be
available locally; ingestion does not download them. The command validates
`**/data/trajectory.hdf5`, writes episode/frame data, materializes canonical
actions, and publishes statistics and metadata. It prints row counts and
snapshot IDs and leaves the warehouse on disk.

Follow [ACT Storage Benchmark](./robomind-act-benchmark) to prepare an experiment,
train ACT directly from `ContiguousWindowDataset`, and compare HDF5 and Paimon
sample tensors, losses, and throughput.

## Four-table contract

Types below are logical Paimon types. Required fields cannot be null; JSON
fields are encoded as `STRING`. The default database is `robomind`; the four
tables below form the complete group. `info.tables` registers the `episode`,
`frame`, and `stat` roles, without a self-reference to info.

### info: `robomind.agilex__info`

The table contains one group description in each published version.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `group_id` | STRING | Yes | Stable group identity. |
| `robot` | `ROW<type STRING, instance_id STRING>` | Yes | Type is `robomind_agilex`; device identity is unknown and null. |
| `storage_mode` | STRING | Yes | `frame`: images are stored with aligned frame rows. |
| `fps` | DOUBLE | No | Reference-row frequency; null because this adapter has no verified clock. |
| `total_episodes` | BIGINT | Yes | Number of episode rows. |
| `total_frames` | BIGINT | Yes | Sum of episode frame counts. |
| `total_tasks` | BIGINT | No | Null; no task table is created. |
| `features` | STRING | Yes | JSON field definitions with dtype, shape, and dimension names. |
| `quality_statuses` | `MAP<INT, STRING>` | Yes | Codes `0=valid`, `1=review`, `2=invalid`. |
| `tag` | STRING | Yes | Published group Tag, supplied as `--statistics-version`. |
| `tables` | `MAP<STRING, STRING>` | Yes | `episode`, `frame`, and `stat` mapped to fully qualified table names. |
| `metadata` | STRING | No | Source dataset/adapter and action semantics. |

### episode: `robomind.agilex__episode`

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `episode_index` | BIGINT | Yes | Immutable trajectory identity within the group. |
| `frame_count` | BIGINT | Yes | Number of frames in this trajectory. |
| `dataset_from_index` | BIGINT | Yes | Inclusive start in the global frame `index`. |
| `dataset_to_index` | BIGINT | Yes | Exclusive end in the global frame `index`. |
| `split` | STRING | Yes | Contract values: `train`, `eval`, `test`, `unassigned`; RoboMIND `val` maps to `eval`. |
| `success` | BOOLEAN | No | Outcome derived from the success/failed directory. |
| `quality_status` | INT | Yes | Quality code from info; validated imports use `0`. |
| `instruction` | STRING | No | Source `language_raw` when present. |
| `task_index` | BIGINT | No | Null; no task dictionary is constructed. |
| `source_episode_key` | STRING | No | Original RoboMIND episode ID. |
| `stats` | STRING | No | Reserved episode feature statistics; null in this importer. |
| `metadata` | STRING | No | JSON source URI, relative `source_key`, optional language embedding, and HDF5 `compress`/`sim` flags. |

Optional language arrays are validated when present. Missing instruction and
embedding values remain null. Outcome and data quality are separate: a valid
recording can describe a failed episode.

### frame: `robomind.agilex__frame`

One row is one aligned source HDF5 timestep. This is the schema after canonical
action backfill; `VECTOR<T, N>` has a fixed length of N. Image BLOBs retain the
original encoded bytes, including JPEG compression where present; ingestion
does not decode and re-encode them.

| Field | Type | Required | Source / meaning |
| --- | --- | --- | --- |
| `episode_id` | STRING | Yes | Original source episode ID, matching `episode.source_episode_key`. |
| `frame_index` | BIGINT | Yes | Zero-based timestep within the episode. |
| `episode_index` | BIGINT | Yes | References `episode.episode_index`. |
| `index` | BIGINT | Yes | Global identity: `episode.dataset_from_index + frame_index`. |
| `timestamp_ns` | BIGINT | No | Null; no verified timestamps are available to this adapter. |
| `quality_status` | INT | Yes | Code from `info.quality_statuses`; validated imports use `0`. |
| `observation_images_rgb_front` | BLOB | Yes | `observations/rgb_images/camera_front`. |
| `observation_images_rgb_wrist_left` | BLOB | Yes | `observations/rgb_images/camera_left_wrist`. |
| `observation_images_rgb_wrist_right` | BLOB | Yes | `observations/rgb_images/camera_right_wrist`. |
| `observation_images_depth_front` | BLOB | Yes | `observations/depth_images/camera_front`. |
| `observation_images_depth_wrist_left` | BLOB | Yes | `observations/depth_images/camera_left_wrist`. |
| `observation_images_depth_wrist_right` | BLOB | Yes | `observations/depth_images/camera_right_wrist`. |
| `state_end_effector_left` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/end_effector_left`. |
| `state_end_effector_right` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/end_effector_right`. |
| `state_joint_effort_left` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_effort_left`. |
| `state_joint_effort_right` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_effort_right`. |
| `state_joint_position_left` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_position_left`; ACT state, left side. |
| `state_joint_position_right` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_position_right`; ACT state, right side. |
| `state_joint_velocity_left` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_velocity_left`. |
| `state_joint_velocity_right` | `VECTOR<DOUBLE, 7>` | Yes | `puppet/joint_velocity_right`. |
| `action_end_effector_left` | `VECTOR<DOUBLE, 7>` | Yes | `master/end_effector_left`. |
| `action_end_effector_right` | `VECTOR<DOUBLE, 7>` | Yes | `master/end_effector_right`. |
| `action_joint_effort_left` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_effort_left`. |
| `action_joint_effort_right` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_effort_right`. |
| `action_joint_position_left` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_position_left`; source of canonical action. |
| `action_joint_position_right` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_position_right`; source of canonical action. |
| `action_joint_velocity_left` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_velocity_left`. |
| `action_joint_velocity_right` | `VECTOR<DOUBLE, 7>` | Yes | `master/joint_velocity_right`. |
| `action` | `VECTOR<FLOAT, 14>` | No in schema; populated before publication | Concatenate left then right master joint positions and cast to float32. |

The three integer identities remain unchanged during backfill. All seven
source vector components are preserved in source order, including the gripper
component of joint-position vectors. RGB/depth shapes are recorded in
`info.features`; no clock frequency, axis units, or sensor values are invented.

ACT uses the two puppet joint-position vectors as state, the canonical
14-dimensional action vector as supervision, and three RGB cameras. Each
logical training sample takes state/images from its anchor and a contiguous
action horizon; windows never cross `episode_index` boundaries. The original
master vectors are demonstration commands, not evidence that identical commands
were received by the robot.

### stat: `robomind.agilex__stat`

The importer writes one `action` / `train` row. Both general feature statistics
and the ACT-specific normalization values live in this row's `stats` JSON.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `feature` | STRING | Yes | `action`, the canonical 14-dimensional frame field. |
| `split` | STRING | Yes | `train`; evaluation and test data do not contribute. |
| `source_table` | STRING | Yes | Fully qualified frame table: `robomind.agilex__frame`. |
| `source_tag` | STRING | Yes | The same immutable Tag as `info.tag`. |
| `stats` | STRING | Yes | JSON raw feature moments, source snapshot, and nested ACT normalization scope. |

At the top level, `stats.count` is an integer frame count; `min`, `max`, `mean`,
and `std` are arrays of 14 numbers. They cover frames with `quality_status = 0`
in train episodes with `quality_status = 0`, including valid failed episodes.
`std` is the raw population standard deviation. `source_snapshot_id` is the
integer frame snapshot used to compute the row.

The nested `stats.act` object supplies the narrower training policy: `count`,
14-dimensional `mean` and raw `std`, `std_floor = 0.01`,
`episode_filter = "success = true AND quality_status = 0"`, and
`split_manifest_sha256`, the digest of sorted eligible source episode keys.
Only valid frames in eligible train episodes contribute. The reader applies
`(action - mean) / max(std, std_floor)` elementwise. Keeping the floor and scope
inside `stats.act` preserves raw general statistics while supplying ACT's
normalization without an extra table or a normalized action column.

### Publication and training reads

All members receive the same immutable Tag; info is written and tagged last.
Readers resolve roles through `info.tables` at that Tag and read episode,
statistics, and frame data from the matching version. Publication uses a single
writer and ordered table commits, not a cross-table transaction. Failed work
without a published info Tag is not a complete training version. Retry a failed
backfill/publication under a new Tag; partial member Tags are not reused.

## Tests

Tests generate small HDF5 trajectories with AgileX field names, shapes, dtypes,
and source layout. To also exercise downloaded data explicitly:

```shell
pytest -q pypaimon/tests/robomind_agilex_pipeline_test.py \
  --robomind-agilex-input /data/RoboMIND/h5_agilex_3rgb
```

## Python API

```python
from pypaimon.benchmark.act.robomind_agilex import (
    backfill_canonical_action,
    backfill_canonical_action_ray,
    ingest_local,
    ingest_ray,
    run_local_pipeline,
    run_ray_pipeline,
)

# Run local ingestion and canonical-action backfill together.
pipeline = run_local_pipeline(
    "/data/RoboMIND/h5_agilex_3rgb",
    "/data/warehouse",
)

# Run distributed ingestion and backfill on one managed Ray cluster.
pipeline = run_ray_pipeline(
    "/data/RoboMIND/h5_agilex_3rgb",
    "/data/warehouse",
    concurrency=8,
    statistics_version="robomind-agilex-joint-position@1",
    num_partitions=8,
    ray_address="ray://cluster:10001",
)
```

Episode and frame ingestion commit separately and use the generic
`pypaimon.ray.load_from_hdf5` API in Ray mode. Canonical action materialization
and statistics refresh also commit separately. The Ray backfill uses the
optimized self-merge path: each target file group is processed by one task, so
updates originating from multiple input batches cannot produce competing delta
files for the same target file. The driver coordinates one commit after all
file groups finish. After backfill, both paths publish the table group under a new immutable Tag.
A published Tag cannot be reused for another backfill. Low-level
`refresh_action_statistics` refreshes the combined stat row; it does not publish
a complete group version. Publication verifies the stat row matches the current
frame snapshot and Tag, then tags the members without recomputing statistics.

Use `backfill_canonical_action` for the local iterable path and
`run_ray_pipeline` for managed distributed ingestion and backfill. The Ray
pipeline requires Ray 2.50 or newer. The lower-level `ingest_ray` and
`backfill_canonical_action_ray` stages assume that Ray has already been
initialized, which allows either stage to be retried independently.

The canonical `action` is `float32(concat(master/joint_position_left,
master/joint_position_right))`. The backfill materializes only this consumed
14-dimensional column. It does not materialize normalized actions. Instead,
`stat.stats.act` stores the successful-train moments and normalization policy
described above. A training reader normalizes `action` at read time with the
stat row resolved from the group Tag.

The tables use non-primary-key storage. The ingestion entry point refuses
existing episode/frame tables to prevent duplicate identities. Row-level
update/delete remains supported. The importer keeps deletion vectors enabled and sets
`blob-as-descriptor=false` because its transforms emit raw image/depth bytes
rather than external BLOB descriptors. Parquet data format, dynamic bucket
mode, and global-index search mode are inherited defaults and are not repeated
in the sample options.

Run local and Ray modes against separate new warehouses when comparing them.
