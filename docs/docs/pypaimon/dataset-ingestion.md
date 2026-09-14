---
title: "HDF5 and ROSBag Ingestion"
description: "Transform HDF5 datasets or ROS messages into rows in an existing multimodal table."
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

# HDF5 and ROSBag Ingestion

Transform HDF5 datasets or ROS messages into rows in an existing multimodal table. Start with a `conn` from [Multimodal Tables](./multimodal-tables#connect), then install the extra for your input format. Source paths and transforms depend on your dataset.

## Load HDF5

`MultimodalConnection.load_from_hdf5` streams one or more local or remote HDF5
files into an existing multimodal table. HDF5 loading requires Python 3.8 or
newer. Install the optional dependency first:

```shell
pip install 'pypaimon[hdf5]'
```

The transform receives an open `h5py.File` and an `Hdf5File`. Its `path` is the
resolved local `file://` URI or remote URI; `name` and `stem` provide convenient
path components. For local sources, `local_path` returns a decoded `Path` that
can be used to read sibling files, including paths containing spaces or Unicode;
it is `None` for remote sources. The transform can return one Arrow table or
record batch, or an iterable that yields multiple tables or batches:

```python
import pyarrow as pa

EMBEDDING_VECTOR_TYPE = pa.list_(pa.float32(), 3)
IMAGE_BLOB_TYPE = pa.large_binary()

schema = pa.schema([
    pa.field("episode_id", pa.string(), nullable=False),
    pa.field("frame_index", pa.int32(), nullable=False),
    # Arrow fixed-size lists map to Paimon VECTOR columns.
    pa.field("embedding", EMBEDDING_VECTOR_TYPE, nullable=False),
    # Arrow binary and large-binary values map to Paimon BLOB columns.
    pa.field("image", IMAGE_BLOB_TYPE),
])

frames = conn.create_table("frames", schema=schema)


def transform(h5, source):
    episode_id = source.stem
    for begin in range(0, len(h5["embedding"]), 128):
        end = min(begin + 128, len(h5["embedding"]))
        yield pa.RecordBatch.from_pydict({
            "episode_id": [episode_id] * (end - begin),
            "frame_index": list(range(begin, end)),
            "embedding": h5["embedding"][begin:end].tolist(),
            "image": [bytes(value) for value in h5["image"][begin:end]],
        }, schema=schema)


result = conn.load_from_hdf5(
    "frames",
    "/data/episodes",
    transform=transform,
)
print(result.file_count)
print(result.batch_count)
print(result.row_count)
print(result.snapshot_id)
```

A path can be one `.h5` or `.hdf5` file, an iterable of paths, or a directory.
Directories are searched recursively. Before opening files, `load_from_hdf5`
resolves every discovered path, removes duplicate files within that call, and
sorts by the resolved path. Overlapping directory and file arguments therefore
read a physical file once per call. Local paths and `file://`, `hdfs://`,
`viewfs://`, `oss://`, `s3://`, and `gs://` URIs use PyPaimon's existing
FileIO implementations.

Remote credentials and endpoints belong to the HDF5 source, not necessarily
the target warehouse. Pass standard FileIO settings through `source_options`;
they are not inherited from the target table, written to table options, or
retained after the call. This also applies to HDFS client settings: provide
source-specific settings explicitly, while configuration discoverable by the
underlying filesystem client can still come from its normal environment:

```python
remote_result = conn.load_from_hdf5(
    "frames",
    "oss://source-bucket/episodes",
    transform=transform,
    source_options={
        "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.accessKeyId": "SOURCE_ACCESS_KEY_ID",
        "fs.oss.accessKeySecret": "SOURCE_ACCESS_KEY_SECRET",
    },
)
```

Because native HDFS Kerberos credentials use process-global ticket state, HDF5
sources cannot run an explicit-keytab login in a shared process. Acquire the
ticket in a process-isolated worker, omit the source principal/keytab options,
and then call `load_from_hdf5` in that worker.

HDF5 requires random access. `load_from_hdf5` passes the seekable stream returned
by `FileIO.new_input_stream` directly to h5py and never downloads it to a local
temporary file. A non-seekable stream fails before commit. Recursive directory
discovery requires backend listing support; legacy OSS on PyArrow before 16
must use explicit file paths, Jindo, or a newer PyArrow version. HDF5 performs
many small seeks, so benchmark remote-object latency for large production
inputs even though no full-file download is required.

An empty path iterable or existing directory with no HDF5 files is a no-op. It
returns zero file, batch, and row counts with `snapshot_id=None`, without
creating a writer or snapshot. A nonexistent path or unsupported file suffix
is still an error, as is a discovered file whose transform produces no rows.

The target schema is strict. Each batch must contain exactly the target columns
in target order and must be safely convertible to the target Arrow schema.
Missing columns are not filled with null, extra columns are not discarded, and
invalid nullability, incompatible types, or fixed-size vector lengths fail the
whole call.

One call uses one writer and one commit for all discovered files, and a
successful non-empty call creates exactly one snapshot. Any failure before the
commit aborts the writer and leaves no partial snapshot. An exception after the
commit starts has Paimon's unknown-commit-result semantics: `load_from_hdf5` does
not retry and does not abort files that a snapshot may already reference.

For a future Ray integration, split HDF5 file descriptions across worker tasks,
apply the same transform and strict batch contract in each task, and collect
writer commit messages for one coordinator commit. Do not call `load_from_hdf5`
independently in every task, because that would create one commit per task. The
HDF5 core itself has no Ray dependency.

`load_from_hdf5` is append-only and is **not retry-safe**. It does not add source
provenance columns, maintain a source ledger, skip prior inputs, or detect
source drift. Calling it again with the same input appends the rows again.

## Load ROSBag

`MultimodalConnection.load_from_rosbag` imports ROS1 `.bag`, ROS2 SQLite3 or
MCAP recording directories, and standalone ROS2 `.mcap` files through a user
Arrow transform. It requires Python 3.10 or newer:

```shell
pip install 'pypaimon[rosbag]'
```

```python
import pyarrow as pa


schema = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("timestamp", pa.int64(), nullable=False),
    pa.field("value", pa.string(), nullable=False),
])
conn.create_table("messages", schema=schema)


def transform(reader, source):
    rows = []
    for connection, timestamp, rawdata in reader.messages():
        message = reader.deserialize(rawdata, connection.msgtype)
        rows.append({
            "source": source.name,
            "timestamp": timestamp,
            "value": message.data,
        })
    return pa.Table.from_pylist(rows)


result = conn.load_from_rosbag(
    "messages",
    "/data/recordings",
    transform=transform,
)
print(result.source_count, result.row_count, result.snapshot_id)
```

A transform receives an open `rosbags.highlevel.AnyReader` and a
`RosbagSource`. The source exposes the original normalized `uri`, the temporary
or original `local_path`, `format`, `name`, `stem`, and `is_remote`. Reader and
local staging paths are valid only while the transform is running. A transform
returns an Arrow table, record batch, or an iterable of either.

Directories are searched recursively, but a directory containing
`metadata.yaml` is one ROS2 logical source and is not searched below that
point. Standalone `.db3` files are rejected because they may be one split of a
larger recording. Set `allow_storage_fragment=True` only when importing exactly
that SQLite storage fragment is intentional; the loader performs a SQLite
integrity check but cannot prove that other recording splits are absent.

Local paths and `file://`, `oss://`, `s3://`, `hdfs://`, `viewfs://`, and
`gs://` URIs use PyPaimon FileIO. Pass source credentials through
`source_options`; target warehouse options are not inherited. Unlike HDF5,
`rosbags` requires local paths, so remote files are copied in bounded chunks to
an attempt-scoped temporary directory. ROS2 metadata member paths are checked
for traversal and normalization collisions before copying.

Before Paimon creates a writer, the loader:

1. validates every source manifest and checks for active or recovery sidecars;
2. scans every recording to EOF and compares declared and readable messages;
3. executes each transform once and applies strict target Arrow schema checks;
4. stores all validated output in a temporary Arrow IPC file.

This front-loaded validation normally reads each recording twice and needs
temporary disk capacity, but a source, transform, or schema failure does not
create Paimon data files. Source size, modification time, and ROS2 directory
members are compared during the call. These checks detect observable changes;
without a versioned URI they are not a transactional snapshot of an object
store directory.

Use `RosbagStagingConfig` to choose the local temporary directory, reserve free
space with `min_free_bytes`, cap serial temporary usage with `max_bytes`, and
set the remote copy chunk size with `copy_buffer_bytes`. In Ray mode,
`max_bytes` applies to each worker's raw ROSBag staging; Arrow output capacity
is controlled by Ray object-store and spill settings.

For Ray, install `pypaimon[ray,rosbag]` and call
`pypaimon.ray.load_from_rosbag`. The driver discovers manifests, each worker
materializes and validates complete sources locally, and the transformed Ray
Dataset is explicitly `materialize()`d before `write_paimon`. Success uses one
coordinator commit. Ray may retry a transform, so transforms must be
deterministic and free of non-idempotent external side effects.

Both APIs are append-only and not retry-safe after a commit exception. A
successful call creates one snapshot; write-stage failures may leave uncommitted
orphan files for normal Paimon cleanup.
