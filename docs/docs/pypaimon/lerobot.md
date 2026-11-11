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

Import a LeRobot Dataset v3 table group, capture frames directly, or train from
retained data. These workflows use a `conn` from
[Multimodal Tables](./multimodal-tables#connect).

## Data model

A LeRobot dataset contains frame data, Episode boundaries, and metadata shared
by the source dataset. Paimon represents these at three granularities. For
`load_from_lerobot(table_name="robot_data", ...)`, the name is a table-group
prefix:

| Table | One row represents | Contents |
| --- | --- | --- |
| `robot_data_datasets` | One source dataset | Feature definitions, FPS, task/subtask vocabularies, and global statistics. |
| `robot_data_episodes` | One Episode in a source | Episode length, frame range, task labels, and source metadata extensions. |
| `robot_data_frames` | One frame in an Episode | Frame indices, timestamp, observations, actions, and image/video features. |

All three are [Data Evolution append tables](../multimodal-table/data-evolution)
with row tracking, no primary key, and `bucket=-1`. The indices below identify
LeRobot records; they are independent of Paimon's internal row IDs. These
relationships are used by the reader and do not declare database foreign keys.

The diagram and field tables below describe **list imports**, including a
one-element list such as `source=["/data/source_a"]`. This form adds
`dataset_index` to frames and Episodes and also supports later appends.
[Single-source shorthand](#single-source-shorthand) has a smaller schema.

[![LeRobot metadata files map to the Dataset table, Episode Parquet maps to the Episode table, and frame Parquet plus media map to the Frame table. Dataset and Episode indices relate the three tables.](/img/lerobot-data-model.svg)](/img/lerobot-data-model.svg)

### Dataset fields

`robot_data_datasets` has one row for each source, even when multiple sources
have the same label. Source ordinals follow the input order; appends allocate
new ordinals after the existing sources.

| Field | Paimon type | Meaning |
| --- | --- | --- |
| `index` | `BIGINT` | Source ordinal, starting at 0. Referenced by frame and Episode `dataset_index`. |
| `name` | `STRING`, nullable | Optional label supplied through `dataset_name`; omitted names are null. |
| `info` | `STRING` | JSON containing the source's original `meta/info.json`, including `features`, `fps`, robot type, and counts. FPS is not a separate column. |
| `tasks` | `ARRAY<ROW<task_index BIGINT, task STRING>>` | Source-local task IDs and their text from `meta/tasks.parquet`. |
| `subtasks` | `ARRAY<ROW<subtask_index BIGINT, subtask STRING>>`, nullable | Optional source-local subtask IDs and text from `meta/subtasks.parquet`. |
| `stats` | `STRING`, nullable | Source-level feature statistics as JSON; null when `meta/stats.json` is absent. |
| Extra text columns | `STRING` or `MAP<STRING, STRING>`, nullable | Columns declared by [extra dataset text fields](#extra-dataset-text-fields). |

Decode `info` and `stats` with `json.loads`. `tasks` and `subtasks` are typed
arrays of records, not JSON strings. No separate info, task, subtask, or global
stats tables are created.

### Episode fields

`robot_data_episodes` preserves source-local Episode IDs and frame ranges.
Fields without `/` retain their source Arrow types; integer widths are not
forced to `BIGINT`.

| Field | Representation | Meaning |
| --- | --- | --- |
| `dataset_index` | `BIGINT` | Source ordinal referencing `_datasets.index`. |
| `episode_index` | Source integer type | Episode number within that source, starting at 0. |
| `dataset_from_index` | Source integer type | Inclusive start of the Episode's source-local frame `index` range. |
| `dataset_to_index` | Source integer type | Exclusive end of that frame range. |
| `length` | Source integer type | Number of frames; equals `dataset_to_index - dataset_from_index`. |
| `tasks` | Source task-label list | Task texts used by the Episode, rather than frame task IDs. |
| `meta`, `data`, `videos`, `stats` | `VARIANT` columns | Grouped source fields describing metadata locations, data shards, per-camera video locations/timestamps, and per-Episode feature statistics. Empty groups are `{}`. |
| Other source fields | Source type, or a `VARIANT` for a `/` prefix group | Custom Episode metadata, such as quality labels or camera calibration. |

For list imports, a field such as `data/chunk_index` becomes the
`"chunk_index"` key inside `_episodes.data`. A field such as
`videos/observation.images.front/from_timestamp` becomes the
`"observation.images.front/from_timestamp"` key inside `_episodes.videos`.
Likewise, `camera_intrinsics/head_left` becomes the `"head_left"` key in a
`camera_intrinsics` VARIANT column. Episode `stats` is grouped by feature and then
statistic, for example `{"action":{"mean":[0.1,0.2]}}`.
In Python, decode a scanned VARIANT value with
`GenericVariant.from_arrow_struct(value).to_python()`.

These data/video locations describe the original LeRobot files. Paimon reads
the imported media through the frame table's BLOB values.

### Frame fields

`robot_data_frames` derives its feature columns from `info.features`. Feature
names, including dots such as `observation.state`, remain column names.

| Field | Paimon type | Meaning |
| --- | --- | --- |
| `dataset_index` | `BIGINT` | Source ordinal referencing `_datasets.index`. |
| `index` | `BIGINT` | Frame position within the original source, across all its Episodes. |
| `episode_index` | `BIGINT` | Episode containing this frame, within the same source. |
| `frame_index` | `BIGINT` | Frame position within the Episode; restarts at 0 for each Episode. |
| `timestamp` | `FLOAT` | Seconds from the Episode's start, consistent with `frame_index / fps` for that source. |
| `task_index` | `BIGINT` | ID resolved through that source's `_datasets.tasks`. |
| `subtask_index` | `BIGINT`, nullable, when declared | ID resolved through that source's `_datasets.subtasks`; null means no subtask label. |
| Feature columns | Scalars, `ARRAY`, or `BLOB` | Observations, actions, and other features, mapped as described below. |

Within a table group, `(dataset_index, index)` identifies a frame and
`(dataset_index, episode_index)` identifies an Episode. Task/subtask IDs are
also scoped to a source. For example, two sources can both contain frame
`index=0`, Episode `episode_index=0`, and task `task_index=0` with different
task texts; `dataset_index` keeps them distinct. The training reader derives
continuous sample positions at read time without rewriting the stored IDs.

### Mapping the native LeRobot v3 format

The importer reads the Parquet/MP4 layout described by the
[LeRobot Dataset v3 format](https://huggingface.co/docs/lerobot/lerobot-dataset-v3).
File paths below are relative to each source root; data/video path templates
come from `meta/info.json`.

| LeRobot source | Paimon destination | Mapping |
| --- | --- | --- |
| `meta/info.json` | `_datasets.info` and frame schema | Retain the original source info as JSON; derive frame feature columns from `features`. |
| `meta/tasks.parquet` | `_datasets.tasks` | Retain source-local ID-to-text mappings; frames keep `task_index`. |
| Optional `meta/subtasks.parquet` | `_datasets.subtasks` | Retain subtask mappings; declared frame `subtask_index` values may be null. |
| Optional `meta/stats.json` | `_datasets.stats` | Store source-level statistics as JSON. |
| `meta/episodes/**/*.parquet` | `_episodes` | Keep one row per Episode, its frame range, and extensions; group `/` fields as described above. |
| `data/**/*.parquet` | `_frames` | Write one row per source frame, preserving its indices and feature names. |
| Image feature bytes or referenced images | Image `BLOB` columns in `_frames` | Store compressed image bytes. |
| MP4 files under `videos/` | Video `BLOB` columns in `_frames` | Copy shared video data into Paimon and associate each logical frame with its video sample; the default import does not decode every frame into an image. |

Scalar features with shape `[]` or `[1]` become scalar columns. Larger numeric
shapes become `ARRAY` columns, nested for multiple dimensions. For example,
`action` with `dtype="float32", shape=[6]` becomes `ARRAY<FLOAT>`. Feature
shape, dtype, and dimension names are retained in column metadata. Images and
videos become `BLOB`, so each camera remains a feature column rather than a
separate Paimon table. See [video import options](#load-lerobot-dataset-v3)
for optional Episode remuxing.

### Single-source shorthand

For a new import with a single source string and no `dataset_name` or non-empty
`extra_dataset_fields`, `load_from_lerobot("robot_data", "/data/source_a")`
still creates the same three tables. Its Dataset row has `index=0` and
`name=null`, but frames and Episodes do not gain a source `dataset_index`.
Episode fields such as `data/chunk_index` and `stats/action/mean` retain their
original names and types instead of being grouped into VARIANT columns.

To use the source-indexed schema above, or to append more sources later, pass
`source=["/data/source_a"]`. Supplying `dataset_name` or non-empty
`extra_dataset_fields` also selects this schema, even for one source.

## Load LeRobot Dataset v3

`load_from_lerobot` imports a local directory, FileIO URI, or Hugging Face
repository into the [three-table model](#data-model) above.

Frames with a null `subtask_index` are imported. `PaimonLeRobotDataset[index]`
returns `None` for `subtask_index` and `subtask`. DataLoader batches use a
validity mask for missing indices (see training below).

The frame table's `pypaimon.lerobot.<component>-table` options identify the
components created.

```shell
pip install 'pypaimon[lerobot]'
```

Python 3.10–3.11 uses LeRobot 0.4.x; Python 3.12+ uses LeRobot 0.6.x.

```python
conn.load_from_lerobot(
    table_name="robot_data",
    source="/data/lerobot_dataset",
    dataset_name="dataset_a",  # Optional source label.
    tag_name="initial-import",  # Optional: tag all imported component snapshots.
)
```

The call returns `None` on success. Omitting `tag_name` imports the tables
without creating tags. The importer requires a new target and a non-empty source.

Pass an ordered list to merge sources into one table group. `robot_merged` is
just an example prefix; choose any unused name:

```python
conn.load_from_lerobot(
    table_name="robot_merged",
    source=["oss://bucket/dataset_a", "oss://bucket/dataset_b"],
    dataset_name=["dataset_a", "dataset_b"],
)
```

`dataset_name` accepts a string for one source or a list/tuple with one label per
source in input order. Omitted names are null, and duplicate labels do not merge
sources. [Dataset fields](#dataset-fields) describes the retained source metadata.

Grouped Episode fields must use JSON-native Arrow types: null, boolean, integer,
floating point, string, list, or struct, including nested combinations.
Binary, temporal, decimal, map, and extension types in these source groups are
rejected before any target table is created. Episode fields without `/` retain
their original names and Paimon-supported Arrow types as columns in `_episodes`;
unsupported types are rejected before table creation.
Training metadata reads only Episode controls; query `_episodes` for extensions.
Control-field statistics reflect merged IDs; their original quantiles are
omitted. Training reads skip `stats`. Repeated task/subtask text within one
source is rejected before creating tables to avoid losing original frame IDs.

Sources must share robot type and compatible definitions for common features. Each source retains its own
FPS from `info`; different source frame rates are handled automatically. Use
`PaimonLeRobotDataset` to train from mixed-FPS groups. Pass `dataset_index=N` to
that reader to select one source. Frame-rate handling is automatic and does not
require an import option.

### Schema evolution

`schema_evolution=True` is the default. The importer plans the union of source
columns before creating `_frames`, `_episodes`, and any declared extra
`_datasets` columns. Episode fields with `/` are grouped by prefix into VARIANT:
`camera_intrinsics/head_left` becomes the `head_left` key in
`_episodes.camera_intrinsics`. Other Episode fields retain their Arrow types.
For appends, it compares the incoming columns with the existing table group and
adds new nullable columns. Existing rows read as null in those columns; incoming
rows receive nulls for omitted optional columns. Existing optional columns are
made nullable when necessary. All three component schemas are checked before
catalog changes, and existing field IDs are preserved.
Existing table groups with JSON `STRING` Episode columns must be re-imported to
use the new VARIANT schema; append does not change those column types in place.
Missing columns receive typed nulls. Optional columns become nullable; source
indices, timestamps, task indices, and required Episode controls remain required.
Common columns must have identical types, and common frame features must retain
their dtype, shape, and names. Type promotion and nested schema changes are
rejected before creating or altering tables.

```python
conn.load_from_lerobot(
    table_name="robot_all",
    source=["/data/dataset_a", "/data/dataset_b"],
    schema_evolution=True,  # Optional; this is the default.
)
```

For example, sources with `action, temperature` and `action, pressure` produce
`action, temperature, pressure`. Rows from the first source have null pressure;
rows from the second have null temperature. The training reader reconstructs
`meta.info.features` as the union of the original info stored in `_datasets`;
feature statistics combine only sources with that feature.

With `schema_evolution=False`, the first source defines the columns. Later
sources may omit optional columns, which still receive nulls, but adding a column
is rejected. For appends, the existing tables define the allowed columns instead
of the first incoming source. The setting does not allow type or tensor-shape
changes in either workflow.

Readers return `None` for missing features. The built-in training batch reader
keeps columns containing missing features as lists with `None` entries; fully
populated features retain their tensor behavior. Select compatible sources or
handle these missing entries when supplying inputs to a model.

### Extra dataset text fields

`extra_dataset_fields` maps new `_datasets` column names to file paths or glob
patterns relative to each source root. The same mapping applies independently to
every source. UTF-8 `.json`, `.txt`, `.md`, `.yaml`, `.yml`, `.jsonl`, `.ndjson`,
`.csv`, and `.tsv` files are supported, including uppercase suffixes. Contents
remain verbatim text, including whitespace and line endings; they are not parsed
as JSON, YAML, or tables.

```python
conn.load_from_lerobot(
    table_name="robot_all",
    source=["/data/dataset_a", "/data/dataset_b"],
    dataset_name=["dataset_a", "dataset_b"],
    extra_dataset_fields={
        "config": "config/*.json",
        "notes": "notes.md",
    },
)
```

A literal file path creates a `STRING` column. A glob creates a
`MAP<STRING, STRING>` column, even when it matches only one file. Map values are
file contents. Keys are relative to the directory prefix before the first glob
component, with the final suffix removed. For example, `config/model_a.json`
matched by `config/*.json` has key `model_a`, accessed in SQL as
`config['model_a']`. Recursive patterns such as `config/**/*.json` retain nested
directories in keys: `config/nested/model_a.json` has key `nested/model_a`.

Missing extra files and empty globs produce null values, with their declared
`STRING` or `MAP<STRING, STRING>` column types retained. Unsupported
suffixes, invalid UTF-8, paths outside the source root, duplicate keys after
removing suffixes, and names that overwrite standard `_datasets` columns are
rejected before creating target tables. Training readers project only required
source metadata and skip extra text columns.

Append another Dataset to a source-local group with `append_to_lerobot`.
Create the group by importing a list, including a one-element list for one source:

```python
conn.load_from_lerobot("robot_merged", ["/data/source_a", "/data/source_b"])
conn.append_to_lerobot("robot_merged", "/data/source_c", tag_name="after-append")
```

One driver submits and commits the entire append; Ray workers can prepare its
sources concurrently. Do not run separate `append_to_lerobot` calls against the
same table group at the same time. The three component tables do not share a
transaction, so interleaved appends can leave the group inconsistent. Queue
additional appends through the same driver. To append multiple sources with
Ray-parallel preparation:

```python
import ray

ray.init(address="auto")
conn.append_to_lerobot("robot_merged", ["/data/source_d", "/data/source_e"],
                       parallelism=2)
```

If source C adds `humidity`, the append adds that column to `_frames`, keeps A
and B, and reads their humidity values as null. New `extra_dataset_fields` columns
are added to `_datasets` in the same way; omitted extra fields receive nulls.
Pass `schema_evolution=False` to `append_to_lerobot` to reject new columns.

If the sole writer stops between component commits, call
`conn.recover_lerobot_append("robot_merged")` before retrying. This rolls
unfinished Frame and Episode commits back to the Dataset metadata boundary.
Nullable columns added before a failed append remain in the schema and are
reused on retry. This recovery is not a repair mechanism for concurrent appends.

For FileIO URIs, pass credentials through `source_options`:

```python
conn.load_from_lerobot(
    table_name="robot_data",
    source="oss://source-bucket/lerobot_dataset",
    source_options={
        "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.accessKeyId": "SOURCE_ACCESS_KEY_ID",
        "fs.oss.accessKeySecret": "SOURCE_ACCESS_KEY_SECRET",
    },
)
```

For distributed import, install `pypaimon[ray,lerobot]` on each worker:

```python
import ray

ray.init(address="auto")
conn.load_from_lerobot(table_name="robot_data", source="oss://source-bucket/lerobot_dataset",
                       parallelism=8)
```

The default `parallelism=1` uses one process. Ray groups Episodes within each source,
keeping shared files together and limiting total concurrency to `parallelism`.
Workers need source and target
storage access; pass OSS `source_options` as above. Ray accepts FileIO URIs or
shared directories, not Hub IDs. Keep the source unchanged during import.
Failed imports may leave partial tables; retry with a new name.

Before training, finish any related data/metadata updates and pause writes to
this table group. Pass the same name used for `load_from_lerobot` or
`PaimonLeRobotWriter` to create a common tag over the component snapshots:

```python
tag = "train-2026-09-07"
snapshots = conn.create_lerobot_tag("robot_data", tag)
frames = conn.get_table("robot_data_frames").scan(tag_name=tag).to_arrow()

# Companion tables are ordinary Paimon tables, read with the same tag.
datasets_table = conn.catalog.get_table("default.robot_data_datasets").copy(
    {"scan.tag-name": tag})
builder = datasets_table.new_read_builder()
dataset_rows = builder.new_read().to_arrow(builder.new_scan().plan().splits())

import json
info = json.loads(dataset_rows["info"][0].as_py())
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

Scalars map to scalar types, numeric tensors to `ARRAY` (nested for higher ranks),
and images to `BLOB`. Tensor shapes remain in the feature metadata; images keep
their compressed bytes. Previously imported `VECTOR` features remain readable.

Video features map to `BLOB`. Each MP4 is copied once per aligned file group for range
reads. Imports require a bucket-unaware table and check rolling before each Episode. Use
`VideoFrameCollator` for scans or `PaimonLeRobotDataset` for training.

By default, imports copy the original videos. Set
`options={"write.lerobot.remux-episodes": "true"}` to split them by Episode without
re-encoding (requires PyAV 14.2+), retaining decoding dependencies. Each clip is limited
to 64 MiB and 65,536 packets; unsupported timestamps fail the import.

## Capture LeRobot frames directly into Paimon

`PaimonLeRobotWriter` implements the write-side surface used by LeRobot's
recording loop without first creating a LeRobot Parquet/image dataset. Pass the
same user feature mapping that would be passed to `LeRobotDataset.create`.
The writer adds the standard `timestamp`, `frame_index`, `episode_index`,
`index`, and `task_index` features itself.

The writer creates three tables: the named frame table, `<table>_episodes`,
and `<table>_datasets`. The Dataset row stores info, statistics, task labels,
and optional subtask labels; it is updated when new Episodes are committed.

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
remain unsupported. Task and optional subtask text are stored in the Dataset
row; frames retain only their indices. Each frame supplies its declared NumPy
`subtask_index` when present.
Subtask labels must be non-empty and unique, and frame indices must reference
that ordered vocabulary. On resume, the writer restores the vocabulary from
the existing table; an explicitly supplied vocabulary must match it exactly.

When `save_episode()` accepts an episode, the writer uses LeRobot's native
statistics implementation to calculate `min`, `max`, `mean`, `std`, `count`,
`q01`, `q10`, `q50`, `q90`, and `q99` for every non-string feature. Flattened
episode statistics are appended to the episodes table. Image statistics use
the encoded PNG frames and LeRobot's sampling, downsampling, CHW, and `[0,1]`
normalization rules. Each flush aggregates the accepted episode statistics and
replaces the Dataset row's global statistics; it does not rescan frame data.

`save_episode` accepts the current episode and writes it to a long-lived Paimon
batch writer. The default `episodes_per_commit=-1` keeps all completed episodes
in that batch until `finalize()`. Set a positive threshold for periodic commits,
or call `flush()` at an operational boundary. Each flush indexes the new frame
rows for training lookups. `save_episode`, `flush`, and `finalize` always return
`None`. Calling `save_episode()` without any buffered frames raises
`ValueError`, matching native LeRobot.

Call `clear_episode_buffer()` before `save_episode()` to discard a re-recorded
episode without advancing frame or episode indices. Once `save_episode()`
accepts an episode, `clear_episode_buffer()` no longer affects it, even when the
batch has not yet been committed. `finalize()` rejects an unfinished episode
instead of silently dropping its frames.

The writer creates a missing table group and appends to an existing compatible
group. Before writing, it requires the root columns, order, Arrow types,
nullability, LeRobot feature metadata, managed options, companion schemas, and
component counts to match. On resume, new `index` and `episode_index` values
continue after the published metadata; task and subtask mappings and global
statistics come from the Dataset row. The episode-local `frame_index` starts
again at zero. Snapshot properties retain only the next global frame and
episode indices; resume does not scan frame data.

Each flush appends Episodes, replaces the Dataset row, and commits frames last. Paimon
does not provide a transaction across these tables. A component commit
exception leaves the group result unknown, makes the writer terminal, and is
not automatically retried. Reopening validates the component state and rejects
a partial batch. Pause writes and use `create_lerobot_tag` before training to
pin one named snapshot on every component.

## Train with Paimon LeRobot data

For datasets with `subtask_index`, `to_dataloader()` always returns integer
`subtask_index` tensors and matching boolean `subtask_index_is_valid` masks,
including time windows. Missing labels use `-1` with a false mask; individual
sample reads retain `None`.
If a source feature uses the mask name, underscores are prepended until the name is unused.

For map-style training, pass an image- or video-backed table group with LeRobot
Episode and Dataset companions to `PaimonLeRobotDataset`.

```python
from pypaimon.multimodal import PaimonLeRobotDataset

table = conn.get_table("robot_data_frames")
dataset = PaimonLeRobotDataset(
    table,
    tag_name=tag,
)
loader = dataset.to_dataloader(
    batch_size=32,
    shuffle=True,
    num_workers=4,
)
```

`to_dataloader` writes decoded video windows directly into shared worker
batches, avoiding a second full-resolution copy during collation.

Without `tag_name`, the latest snapshots are used. Frame lookups use the BTree
on `index`; payloads remain lazy. Ordinary MP4 sample tables locate target frames and GOPs; other videos use the
TorchCodec/PyAV scan path. Set
`video_backend` to force either decoder. With `video_backend="torchcodec"`, supported
MP4 videos use timestamp seeks verified against their sample table and returned PTS.
Decoder replicas share read-only timelines; uniform timing is stored compactly.
Unsupported timelines or mismatched timestamps fall back to TorchCodec exact mode.

With `pypaimon-rust` installed, LeRobot defaults to Rust frame lookup and table reads
unless native read/planning options are explicitly configured. Set
`table = table.copy({"read.native.enabled": "false"})` to use Python.
Reads stay on the selected snapshot; unavailable or unsupported native paths use Python.

Set `table = table.copy({"read.video.max-open-decoders": "8"})` before
constructing the dataset to limit cached decoders per camera per worker (default: 16).
Each open decoder uses memory and a file handle, so usage grows with cameras and workers.
`read.video.decode-parallelism` limits concurrent video decodes across all cameras in each
Dataset worker (default: 16). Set it to 1 for serial decoding. Parallel decoding
applies to video windows without `image_transforms` and uses more CPU and memory.
To reduce remote requests when sampling small videos, enable whole-video prefetch:

```python
table = table.copy({"read.video.prefetch-max-bytes": str(32 * 1024 * 1024)})
```

Default: `0` (disabled). The budget covers cached video bytes per camera per worker;
when it is full, reads remain range-based. Prefetch may increase bytes read.
Decoded frames and temporary buffers use additional memory.

`read.video.codec-threads` controls FFmpeg threads per PyAV or indexed TorchCodec decoder (default: 1),
avoiding nested codec parallelism with multiple workers and concurrent videos.
Set it to 0 for automatic selection. TorchCodec exact-mode fallback uses its own defaults.

`read.video.decoders-per-payload` (default: 4) also splits single-video windows across
independent cached decoders, within those limits. Set it to 1 to disable this splitting;
the reader automatically reduces the decoder count when overlapping windows would add
more than 50% duplicate frame requests. Additional decoders can increase initialization
cost and repeated reads.

Subclass `PaimonDatasetReader` for a custom logical frame layout:

```python
from pypaimon.multimodal import PaimonDatasetReader, PaimonLeRobotDataset

class CustomDatasetReader(PaimonDatasetReader):
    def __init__(self, metadata, source, **kwargs):
        self._source = source
        super().__init__(
            metadata,
            file_io=getattr(source, "file_io", None),
            **kwargs,
        )

    def read_indices(self, indices, columns):
        return self._source.read_indices(indices, columns)

reader = CustomDatasetReader(
    metadata, source, delta_timestamps=delta_timestamps
)
dataset = PaimonLeRobotDataset(reader)
```

`source` exposes `read_indices` and optional `file_io`. Pass
`schema=source.schema` for eager schema validation. `PaimonDatasetReader`
reuses the standard Episode, delta-window, media, and Torch handling.
