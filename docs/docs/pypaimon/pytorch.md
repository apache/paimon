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

When the `streaming` parameter is true, it will iteratively read;
when it is false, it will read the full amount of data into memory.

**`prefetch_concurrency`** (default: 1): When streaming is true, number of threads used for parallel prefetch within each DataLoader worker. Set to a value greater than 1 to partition splits across threads and increase read throughput. Has no effect when streaming is false.

## Parquet Metadata Cache

For repeated Parquet reads in long-lived workers, enable metadata reuse with:

```python
table = table.copy({
    "parquet.metadata-cache-enabled": "true",
    "parquet.metadata-cache-max-entries": "256",
})
read_builder = table.new_read_builder()
```

The cache is disabled by default and is local to each process and `FileIO`.
It benefits workers reused with `DataLoader(..., persistent_workers=True)`.
The limit counts entries, not bytes. Only use it with immutable files, such as
Paimon-managed data files.

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
