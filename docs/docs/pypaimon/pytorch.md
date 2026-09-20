---
title: "PyTorch"
sidebar_position: 4
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

# PyTorch

## Read

This requires `torch` to be installed.

You can read all the data into a `torch.utils.data.Dataset` or `torch.utils.data.IterableDataset`:

```python
from torch.utils.data import DataLoader

table_read = read_builder.new_read()
dataset = table_read.to_torch(splits, streaming=True, prefetch_concurrency=2)
dataloader = DataLoader(
    dataset,
    batch_size=2,
    num_workers=2,  # Concurrency to read data
    shuffle=False
)

# Collect all data from dataloader
for batch_idx, batch_data in enumerate(dataloader):
    print(batch_data)

# output:
#   {'user_id': tensor([1, 2]), 'behavior': ['a', 'b']}
#   {'user_id': tensor([3, 4]), 'behavior': ['c', 'd']}
#   {'user_id': tensor([5, 6]), 'behavior': ['e', 'f']}
#   {'user_id': tensor([7, 8]), 'behavior': ['g', 'h']}
```

When the `streaming` parameter is true, it will iteratively read. When it is
false, eligible data-evolution reads fetch each DataLoader batch lazily by row
ID; other reads retain an Arrow table in memory for map-style access.

**`prefetch_concurrency`** (default: 1): In streaming row mode, controls
reader threads per DataLoader worker. It has no effect in non-streaming mode.

### Distributed Sharding

Streaming reads shard splits across DDP ranks and DataLoader workers:

```python
def main():
    dataset = table_read.to_torch(
        splits,
        streaming=True,
        auto_detect_rank=True,
    )
    dataloader = DataLoader(
        dataset,
        batch_size=32,
        num_workers=2,
        multiprocessing_context="spawn",
    )

    with model.join():
        for batch in dataloader:
            train(batch)


if __name__ == "__main__":
    main()
```

Automatic rank sharding is opt-in. Enable it only when every rank receives the
same ordered, complete splits from one snapshot; leave it disabled for splits
already sharded by the application.
Automatic detection uses the default process group. For subgroup DDP, resolve
the context from the group before creating the DataLoader:

```python
import torch.distributed as dist

dataset = table_read.to_torch(
    splits,
    streaming=True,
    sharding_rank=dist.get_rank(ddp_group),
    sharding_world_size=dist.get_world_size(ddp_group),
)
```

With multi-worker DDP, use `spawn` (or `forkserver`) and create and iterate the
DataLoader through an `if __name__ == "__main__":` guarded entry point.
A rank may receive fewer rows because splits have different sizes; `join()`
keeps DDP collectives aligned while preserving every row without duplication.
A limit that may truncate the input is rejected when multiple ranks are active.

### Batch Streaming

For batch-oriented training, make the streaming dataset yield batches directly:

```python
dataset = table_read.to_torch(
    splits,
    streaming=True,
    batch_format="torch",
    batch_size=1024,
)
dataloader = DataLoader(dataset, batch_size=None, num_workers=2)

for batch in dataloader:
    train(batch["features"], batch["label"])
```

`batch_format="pyarrow"` yields PyArrow `RecordBatch` objects instead;
`batch_format="torch"` yields dictionaries of tensors. The default Tensor
converter supports non-null numeric, boolean, and numeric fixed-size-list
columns. Use `to_tensor_fn` for other types or custom conversion.

Without shuffle, omit `batch_size` to preserve native reader batches. Otherwise,
batches are combined or sliced to the requested size. Use
`DataLoader(batch_size=None)` to disable a second batching step.
Numeric tensors may share read-only Arrow buffers; clone them before in-place
mutation. Batch formats currently require `prefetch_concurrency=1`.

### Shuffled Batch Streaming

Set `shuffle=True` to mix rows across input batches while keeping values in
Arrow until the final Tensor conversion. The same options work with
`batch_format="pyarrow"` and custom `to_tensor_fn` converters:

```python
dataset = table_read.to_torch(
    splits,
    streaming=True,
    batch_format="torch",
    batch_size=256,
    shuffle=True,
    seed=42,
    buffer_size=4096,
    max_buffer_input_splits=4,
)
loader = DataLoader(dataset, batch_size=None, num_workers=2)

for epoch in range(10):
    dataset.set_epoch(epoch)
    for batch in loader:
        train(batch["features"], batch["label"])
```

Each worker retains at most `buffer_size` rows in a rolling shuffle buffer and
replaces randomly selected slots with rows from incoming Arrow blocks. Input
blocks contain at most `buffer_size` rows; gathering replacements, output
batching, and each open format reader use additional memory. This is a row
bound, not a byte bound. The buffer drains early if combining Arrow blocks
would overflow a 32-bit offset. Without `batch_size`, output blocks contain at
most `buffer_size` rows.

`max_buffer_input_splits` bounds the number of interleaved split readers per
worker; `1` reads splits in order. A binding limit keeps the existing ordered
read path so it selects the same rows before shuffling. Filters and distributed
worker/rank sharding also precede shuffle. Each selected row is emitted once.

The shuffle is local to each worker's buffer, not a uniform permutation of the
whole dataset. The same seed, epoch, input batches and worker/rank configuration
reproduce the same order; `set_epoch()` also reaches persistent workers. Changing
reader batching or worker configuration can change the order. Arrow and Tensor
formats share the same shuffle path, while row-format split interleaving can
produce a different order. The existing `prefetch_concurrency=1` requirement
also applies to shuffled batches.

## Video frame descriptors

For a multimodal frame table, use the higher-level scan API:

```python
dataset = (
    frames.scan()
    .select(["episode_id", "state", "action", "video"])
    .to_torch(streaming=True)
)
```

The `.video` column yields serialized `VideoFrameDescriptor` values whose
embedded frame ordinals keep frame mapping out of the normal data file. Use
`pypaimon.multimodal.VideoFrameCollator` as the DataLoader `collate_fn` to open
physical video ranges and cache decoder sessions per worker. See
[Multimodal API: Video Frame Storage](video#video-frame-storage)
for the write path and a complete decoder example.
## Contiguous Windows

Use a map-style `ContiguousWindowDataset` to construct training samples from
one materialized frame table. Each field can have its own history or future
window, and windows never cross a sequence boundary. The dataset builds an index
from only the group column, order column, and Paimon row IDs. Projected values,
including BLOB payloads, are read from the pinned snapshot when a sample is
requested; they are not retained in the index.

```python
from functools import partial

import torch
from torch.utils.data import DataLoader
from pypaimon.multimodal.window_transforms import images_to_tensor, to_tensor

dataset = (
    frames.scan()
    .to_contiguous_window_dataset(
        columns=["state", "image", "action"],
        frame_offsets={"state": [-2, -1, 0], "action": list(range(16))},
        group_key="episode_index",
        order_key="frame_index",
        boundary="pad",
        column_transforms={
            "state": partial(to_tensor, dtype=torch.float32),
            "action": partial(to_tensor, dtype=torch.float32),
            "image": images_to_tensor,
        },
    )
)

loader = DataLoader(dataset, batch_size=32, num_workers=4, shuffle=True)
```

Each raw item contains scalar group/order keys, a list for every selected column,
and a Boolean `<column>_is_pad` tensor where `True` marks padding. Unspecified
field offsets default to `[0]`. The example produces state/action tensors with
time lengths 3/16 and an image tensor of shape `(1, C, H, W)`; DataLoader adds the
batch dimension. `pad_values` can replace endpoint repetition for individual
columns, before any transforms. `adapter` can normalize or rename fields and
produce a model-specific mapping. Keep callbacks picklable for multiple workers.

Scheduled anchors start at each group's first row and advance by `stride`
(default `1`). `boundary="drop"` (default) omits anchors incomplete for any
field, `boundary="pad"` pads either end, and `boundary="error"` rejects any
scheduled incomplete window. `delta_timestamps` with an explicit `fps` is an
alternative to integer offsets, with frame-grid alignment checked against
`tolerance_s`. Existing `window_size`/`anchor_columns`/`tail` calls retain their
single `is_pad` output. See [multimodal reading](multimodal-reading#contiguous-windows-for-pytorch)
for the full sample and conversion contract.
Rows are sorted by `order_key` inside each `group_key` value. Order values must
be integers which increase by exactly one; duplicates and missing steps are
rejected, and windows never cross groups. The resolved Paimon
snapshot is pinned for the lifetime of the dataset, so later commits cannot
change its index or sample contents. A dataset pinned through `tag_name` fails
its reads if the tag is moved to another snapshot, rather than mixing rows from
the two snapshots.

Use normal PyTorch samplers for shuffling and distributed training. For DDP,
pass `DistributedSampler(dataset)` to DataLoader, omit `shuffle=True`, and call
the sampler's `set_epoch(epoch)` each epoch. Its padding or drop behavior still
applies when the sample count is not divisible by the number of ranks. Dataset
version pinning does not save sampler progress or random augmentation state.

Columns configured by `video-frame-field` are rejected: a window read would drop
the `frame_index` and other metadata carried by their `VideoFrameDescriptor`
values. Read those columns with `to_torch()` instead.
## File Format Metadata Cache

Reusable PyArrow Dataset metadata is cached across reads. Configure its estimated
size limit in the catalog options:

```python
catalog = CatalogFactory.create({
    "warehouse": "file:///path/to/warehouse",
    "file-format.metadata-cache.max-size": "50 mb",
})
table = catalog.get_table("database.table")
read_builder = table.new_read_builder()
```

The default limit is 50 MB; set it to `0 b` to disable and clear the cache. The
cache is local to each process and benefits workers reused with
`DataLoader(..., persistent_workers=True)`. The cache uses a conservative
per-entry memory estimate and an internal entry-count safeguard; actual native
PyArrow memory may still be higher. The cache assumes immutable Paimon data files.

## Shuffle

PyPaimon supports streaming shuffle for PyTorch `IterableDataset`. The shuffle
pipeline can be composed of three layers:

1. **Chunk shuffle**: split files into row chunks during scan planning and
   shuffle the generated chunk splits. This is enabled by
   `TableScan.with_chunk_shuffle(seed, chunk_size)`.
2. **Split interleave**: read from multiple splits in round-robin order inside
   each DataLoader worker.
3. **Buffer shuffle**: apply a reservoir-style row shuffle buffer before rows
   are yielded to PyTorch.

Chunk shuffle is a scan planning feature for append tables, including
Data Evolution append tables. For Data Evolution tables, chunk shuffle keeps
row-id-aligned data files and sidecar files together while slicing by row-id
range. Chunk shuffle should be used with file formats that **support random
access**. Currently, the random-access file formats are Lance, Vortex, Row, and
Blob. Primary-key tables and deletion-vector scans are not supported by
`with_chunk_shuffle`.

The second and third layers are Dataset features. They work on the splits you
pass to `to_torch`, so they can be used with either normal splits or
chunk-shuffled splits.

### Use Dataset Shuffle Only

Use this when normal scan splits are enough and you only want split interleave
plus row buffer shuffle:

```python
from torch.utils.data import DataLoader

table_scan = read_builder.new_scan()
table_read = read_builder.new_read()
splits = table_scan.plan().splits()

dataset = table_read.to_torch(
    splits,
    streaming=True,
    shuffle=True,
    seed=42,
    buffer_size=1000,
    max_buffer_input_splits=10,
)

dataloader = DataLoader(
    dataset,
    batch_size=32,
    num_workers=2,
    shuffle=False,
)
```

`buffer_size` controls the row shuffle buffer. Larger values produce a better
approximation of global shuffle, at the cost of more memory. If
`max_buffer_input_splits` is `1`, split interleave is skipped and only buffer
shuffle is applied. `shuffle=True` requires `streaming=True` and does not
support `prefetch_concurrency > 1`.

### Use All Three Layers

For append tables, enable chunk shuffle during scan planning, then enable
Dataset shuffle when converting to PyTorch:

```python
from torch.utils.data import DataLoader

seed = 42

table_scan = read_builder.new_scan().with_chunk_shuffle(
    seed=seed,
    chunk_size=1000,
)
table_read = read_builder.new_read()
splits = table_scan.plan().splits()

dataset = table_read.to_torch(
    splits,
    streaming=True,
    shuffle=True,
    seed=seed,
    buffer_size=1000,
    max_buffer_input_splits=10,
)

dataloader = DataLoader(
    dataset,
    batch_size=32,
    num_workers=2,
    shuffle=False,
)
```

Call `dataset.set_epoch(epoch)` before creating or iterating a DataLoader for a
new training epoch if you want a different buffer-shuffle order for each epoch.
