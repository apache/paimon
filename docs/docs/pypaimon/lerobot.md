---
title: "LeRobot Datasets"
description: "Import a LeRobot Dataset v3 table group, capture frames directly, or train from retained data."
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

# LeRobot Datasets

Import a LeRobot Dataset v3 table group, capture frames directly, or train from retained data. These workflows use a `conn` from [Multimodal Tables](./multimodal-tables#connect). Importing a complete dataset and capturing a frame-only table produce different metadata; choose the workflow needed by your training reader.

## Load LeRobot Dataset v3

`load_from_lerobot` imports a local directory, FileIO URI, or Hugging Face
repository. It derives the schema from `meta/info.json` and writes one row per
frame. The import creates `<table>__episodes`, `<table>__tasks`, `<table>__info`,
and optional `<table>__stats` and `<table>__subtasks` companion tables. Task text
remains in the task table; frames retain `task_index`.

Info and stats use `key STRING, value STRING`: each top-level info property or
stats feature becomes one row. Each value is JSON-encoded, preserving nested
objects, arrays, nulls, and scalar types. Decode it with `json.loads`; for example,
`fps` stores `30`, while `codebase_version` stores `"v3.0"` (including quotes).
Statistics may contain `NaN` and `Infinity`, supported by Python's JSON decoder.
Missing or empty stats create no stats table. The frame table's
`pypaimon.lerobot.<component>-table` options identify the components created.

```shell
pip install 'pypaimon[lerobot]'
```

```python
conn.load_from_lerobot(
    "robot_data",
    "/data/lerobot_dataset",
    tag_name="initial-import",  # Optional: tag all imported component snapshots.
)
```

The call returns `None` on success. Omitting `tag_name` imports the tables
without creating tags. The one-time importer requires a new target table and
a non-empty source dataset; subsequent table edits use the normal Paimon APIs.

For FileIO URIs, pass credentials through `source_options`:

```python
conn.load_from_lerobot(
    "robot_data",
    "oss://source-bucket/lerobot_dataset",
    source_options={
        "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.accessKeyId": "SOURCE_ACCESS_KEY_ID",
        "fs.oss.accessKeySecret": "SOURCE_ACCESS_KEY_SECRET",
    },
)
```

Before training, finish any related data/metadata updates and pause writes to
this table group. Create a common named tag over the current component snapshots:

```python
tag = "train-2026-09-07"
snapshots = conn.create_lerobot_tag("robot_data", tag)
frames = conn.get_table("robot_data").scan(tag_name=tag).to_arrow()

# Companion tables are ordinary Paimon tables, read with the same tag.
info_table = conn.catalog.get_table("default.robot_data__info").copy(
    {"scan.tag-name": tag})
builder = info_table.new_read_builder()
info_rows = builder.new_read().to_arrow(builder.new_scan().plan().splits())

import json
info = {
    row["key"]: json.loads(row["value"])
    for row in info_rows.to_pylist()
}
```

`create_lerobot_tag` returns component names mapped to snapshot IDs; these IDs
may differ across tables. Later appends do not change tagged reads. Read every
required component (including training statistics) through the same tag, and
never fall back to latest if a tag is missing.

Cross-table tagging is not atomic. Use a tag only after the creation call
succeeds. A failure may leave partial tags; retry with writes still paused and
unchanged snapshots, or choose a new name after repairing the group. Existing
tags are never moved to different snapshots. Retain or delete component tags
together, and keep writers paused until the call returns.

Scalars map to scalar types, vectors to `VECTOR`, higher-rank tensors to nested
`ARRAY`, and images to `BLOB`. Images keep their compressed bytes.

Video features map to `BLOB`. Frame rows reference MP4 payloads copied once per
aligned file group. Video imports use the video grouping policy and check
rolling before each Episode. They require a bucket-unaware table. Read them
with a Paimon scan and `VideoFrameCollator`; `PaimonLeRobotDataset` currently
supports image features only.

## Capture LeRobot frames directly into Paimon

`PaimonLeRobotWriter` implements the write-side surface used by LeRobot's
recording loop without first creating a LeRobot Parquet/image dataset. Pass the
same user feature mapping that would be passed to `LeRobotDataset.create`.
The writer adds the standard `timestamp`, `frame_index`, `episode_index`,
`index`, and `task_index` features itself.

The writer preserves native LeRobot recording metadata in a managed Paimon
table group. The named table stores frames, while `<table>__episodes`,
`<table>__tasks`, `<table>__info`, and `<table>__stats` store episode
boundaries, task labels, JSON-encoded dataset information, and global
statistics. When `subtask_index` is declared, `<table>__subtasks` stores its
ordered text vocabulary. The root table's managed options identify these
companions.

```python
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter

writer = PaimonLeRobotWriter(
    conn,
    "recorded_frames",
    fps=30,
    features=dataset_features,
    # Required for a new table when features includes subtask_index:
    # subtasks=["approach object", "grasp object"],
)

# LeRobot's record_loop only needs writer.fps, writer.features, and
# writer.add_frame(frame), so the writer can be passed as its dataset argument.
record_loop(..., fps=30, dataset=writer)
writer.save_episode()

# Optional durability/visibility boundary before finalize.
writer.flush()
writer.finalize()
```

Like native LeRobot, `add_frame` requires every declared user feature plus a
string `task`, and rejects caller-provided generated fields. Numeric features
must be NumPy arrays (Torch tensors are converted) with the declared dtype and
shape. Image dimension names may declare CHW or native HWC layout; image values
may be CHW, HWC, or PIL. Images are encoded as PNG bytes and stored in Paimon
`BLOB` columns; no LeRobot data directory or MP4 is created. `video` features
remain unsupported. Task text is stored once in the tasks table; frame rows
retain only `task_index`. Optional subtask text is likewise stored once in the
subtasks table, while each frame supplies its declared NumPy `subtask_index`.
Subtask labels must be non-empty and unique, and frame indices must reference
that ordered vocabulary. On resume, the writer restores the vocabulary from
the existing table; an explicitly supplied vocabulary must match it exactly.

When `save_episode()` accepts an episode, the writer uses LeRobot's native
statistics implementation to calculate `min`, `max`, `mean`, `std`, `count`,
`q01`, `q10`, `q50`, `q90`, and `q99` for every non-string feature. Flattened
episode statistics are appended to the episodes table. Image statistics use
the encoded PNG frames and LeRobot's sampling, downsampling, CHW, and `[0,1]`
normalization rules. Each flush aggregates the accepted episode statistics and
replaces the global stats table; it does not rescan frame data.

`save_episode` accepts the current episode and writes it to a long-lived Paimon
batch writer. The default `episodes_per_commit=-1` keeps all completed episodes
in that batch until `finalize()`. Set a positive threshold for periodic commits,
or call `flush()` at an operational boundary. `save_episode`, `flush`, and
`finalize` always return `None`. Calling `save_episode()` without any buffered
frames raises `ValueError`, matching native LeRobot.

Call `clear_episode_buffer()` before `save_episode()` to discard a re-recorded
episode without advancing frame or episode indices. Once `save_episode()`
accepts an episode, `clear_episode_buffer()` no longer affects it, even when the
batch has not yet been committed. `finalize()` rejects an unfinished episode
instead of silently dropping its frames.

The writer creates a missing table group and appends to an existing compatible
group. Before writing, it requires the root columns, order, Arrow types,
nullability, LeRobot feature metadata, managed options, companion schemas, and
component counts to match. A legacy frame-only writer table is not implicitly
migrated. An existing group must have the stats companion and must not have a
subtasks companion unless its frame schema declares `subtask_index`. On resume,
new `index` and `episode_index` values continue after the published metadata,
existing task and subtask mappings come from their companion tables, and the
episode-local `frame_index` starts again at zero. Snapshot properties retain
only the next global frame and episode indices; resume restores global
statistics from the stats table and does not scan frame data.

When configured, the first `flush()` writes the immutable subtask vocabulary.
Each flush appends new tasks and episodes, replaces global stats and info, and
commits frames last. Paimon
does not provide a transaction across these tables. A component commit
exception leaves the group result unknown, makes the writer terminal, and is
not automatically retried. Reopening validates the component state and rejects
a partial batch. Pause writes and use `create_lerobot_tag` before training to
pin one named snapshot on every component.

## Train with Paimon LeRobot data

For map-style training, read a tagged table group created by
`load_from_lerobot` directly from Paimon. `PaimonLeRobotDataset` requires the
complete table group; a frame-only table created by `PaimonLeRobotWriter` is
not sufficient.

```python
from torch.utils.data import DataLoader
from pypaimon.multimodal import PaimonLeRobotDataset

dataset = PaimonLeRobotDataset(
    conn.get_table("robot_data"),
    tag_name=tag,
)
loader = DataLoader(dataset, batch_size=32, shuffle=True, num_workers=4)
```

If `tag_name` is omitted, the latest snapshots are used. Metadata is available
through `dataset.meta`. Frame lookups use the BTree on `index`; payload columns
remain lazy.
