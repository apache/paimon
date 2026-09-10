---
title: "Multimodal Reads and Row IDs"
description: "Filter and project multimodal rows, fetch BLOB payloads, and carry row IDs between processing stages."
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

# Multimodal Reads and Row IDs

Filter and project multimodal rows, fetch BLOB payloads, and carry row IDs between processing stages. The examples use the `docs` table from [Multimodal Tables](./multimodal-tables). Examples with `frames` require a frame table with the named grouping, ordering, and payload columns.

## Scan

Use `scan()` for ordinary table reads. `where()` accepts SQL-like predicate
strings; it does not accept lower-level `Predicate` objects.

```python
result = (
    docs.scan()
    .where("category = 'lake'")
    .select(["id", "content"])
    .limit(10)
    .to_pandas()
)
```

Use `to_arrow_batch_reader()` to consume an ordinary scan without collecting
all rows into one Arrow table:

```python
with docs.scan().where("category = 'lake'").to_arrow_batch_reader() as reader:
    for batch in reader:
        consume(batch)
```

### Reading BLOB columns

`scan().read_blobs(column)` bulk-fetches a BLOB column's bytes for the filtered
rows using concurrent, same-file coalesced ranged reads. This is much faster than
a per-row loop and avoids the slow row-by-row blob resolution on data-evolution
tables. It returns `(scalar_table, {column: rows})`, row-aligned. Scalar BLOB
rows are `bytes | None`; `MAP<K, BLOB>` rows are `None` or ordered lists of
`(key, bytes | None)` pairs. The scalar table drops the readable BLOB columns.

```python
scalar, blobs = (
    docs.scan()
    .where("category = 'lake'")
    .read_blobs("image", parallelism=32)
)
images = blobs["image"]          # list[bytes | None], aligned with scalar rows
```

`where()` takes SQL-like strings only, so filter by a list of keys with `IN (...)`:

```python
ids = [1, 2, 3]
in_clause = ", ".join(str(int(value)) for value in ids)
scalar, blobs = docs.scan().where(f"id IN ({in_clause})").read_blobs("image")
```

`where()` is a SQL string, so build the `IN (...)` clause only from trusted,
already-escaped ids -- do not interpolate untrusted external input.

Read several BLOB columns at once, including `MAP<K, BLOB>`:

```python
scalar, blobs = docs.scan().where("category = 'lake'").read_blobs(
    ["image", "audio", "renditions"]
)
renditions = [None if row is None else dict(row) for row in blobs["renditions"]]
```

For a memory-bounded read (e.g. streaming into a trainer), `stream_blobs` yields
one batch at a time, so peak memory is a single batch rather than the whole result:

```python
for scalar, blobs in docs.scan().where("category = 'lake'").stream_blobs("image"):
    for img in blobs["image"]:
        ...  # feed the trainer, then drop the batch
```

Notes:

- `parallelism` ~16–32 is the sweet spot over the public network; go higher on an
  internal/VPC endpoint.
- Fastest when the matching rows are contiguous (bulk/range reads coalesce into a
  few large reads); scattered point reads coalesce less.
- Blob reads are available only on `scan()`, not on the `search()` queries.

### Contiguous windows for PyTorch

Install the `torch` extra, then use `to_contiguous_window_dataset` to expose
map-style windows without loading the selected rows or BLOB payloads into Python
memory up front. The Dataset builds a compact index from the group column, order
column, and Paimon row IDs. Each `__getitem__` call fetches only that window from
the snapshot recorded in `dataset.snapshot_id`.

```shell
pip install 'pypaimon[torch]'
```

```python
import torch


def float32_window(values):
    return torch.tensor(values, dtype=torch.float32)


windows = (
    frames.scan()
    .where("split = 'train'")
    .to_contiguous_window_dataset(
        window_size=16,
        columns=["state", "action"],
        group_key="episode_index",
        order_key="frame_index",
        tail="pad",
        column_transforms={
            "state": float32_window,
            "action": float32_window,
        },
    )
)

sample = windows[0]
assert sample["action"].shape == (16, action_size)
assert sample["is_pad"].shape == (16,)
```

The group and order keys in a sample identify the window anchor. Every projected
column contains the whole window. With `tail="drop"`, only full windows are
exposed. With `tail="pad"`, every real row is an anchor; missing suffix values
repeat the last real value by default and `is_pad` is `True` exactly at those
positions. With `tail="error"`, construction fails if any scheduled anchor is
incomplete. Use `pad_values` to override the repeated value for individual
columns. Anchors advance by `stride`, which defaults to one row.

`column_transforms` receive one padded Python list per projected column. This is
where applications define tensor dtype and shape or decode BLOB bytes. The
optional `adapter` receives the resulting sample mapping and can rename or
combine fields for a model-specific batch contract. The core Dataset does not
know model field names, image formats, or normalization rules. Top-level
functions and callable classes are recommended for transforms and adapters so
the Dataset remains picklable by multi-worker `torch.utils.data.DataLoader`
instances.

Columns configured by `video-frame-field` are rejected: a window read would drop
the `frame_index` and other metadata carried by their `VideoFrameDescriptor`
values. Read those columns with `to_torch()` instead.

### Distributed BLOB processing with Ray

For larger jobs, read descriptors with `to_ray()`, then fetch and process BLOB
bytes on Ray workers with `map_with_blobs`.

```python
import ray
import pyarrow as pa
import pypaimon.multimodal as pm

ray.init(num_cpus=8)

conn = pm.connect(database="default", options={"warehouse": "file:///tmp/warehouse"})
docs = conn.get_table("docs")

ids = ["a", "b", "c"]
scalar_cols = ["id", "category"]
blob_cols = ["image"]
in_clause = ", ".join(f"'{i}'" for i in ids)


def process_batch(scalar_batch, blobs):
    images = blobs["image"]
    # Decode, infer, write samples, or train here.
    return pa.table({"rows": [scalar_batch.num_rows]})


ds = (
    docs.scan()
    .where(f"id IN ({in_clause})")
    .select(scalar_cols + blob_cols)
    .to_ray()
)

# Optional Ray transforms are fine if BLOB descriptor columns remain.
# ds = ds.filter(lambda row: row["category"] == "lake")

result_ds = docs.map_with_blobs(ds, blob_cols, process_batch)
```

Notes:

- `process_batch` must return a small Ray-compatible batch; return an empty
  `pyarrow.Table` for side-effect-only jobs.
- Avoid returning raw BLOB bytes, which would materialize payloads in Ray's
  object store.
- Tune `to_ray(...)` and `map_with_blobs(...)` parameters only when needed.

## Row IDs

Multimodal tables enable `row-tracking.enabled` by default, so each row has a
Paimon system column named `_ROW_ID`. Use `_ROW_ID` as an internal coordination
key for retrieval, reranking, inference, and training jobs. Keep user-visible
document IDs, object keys, or primary keys in normal columns.

Use `with_row_id()` to include `_ROW_ID` in scan or search results. The method
appends `_ROW_ID` to the current projection; if no projection is set, it returns
all table columns plus `_ROW_ID`.

```python
candidates = (
    docs.search([0.1, 0.2, 0.3], column="embedding")
    .where("category = 'lake'")
    .select(["id", "content"])
    .limit(100)
    .with_row_id()
    .to_pandas()
)

row_ids = candidates["_ROW_ID"].tolist()
```

When a row-id manifest is persisted beyond a short-lived latest-snapshot
workflow, record the source snapshot or tag and pass it back to the read API.
`scan`, `search`, `search_vectors`, `search_hybrid`, and `take_row_ids` accept
`snapshot_id` or `tag_name`. These two options are mutually exclusive.

Use `take_row_ids` to fetch rows selected by an earlier scan, vector search,
full-text search, hybrid search, sampler, or split manifest. Results are not
guaranteed to follow the input row-id order. Include `_ROW_ID` and reorder on
the client if order matters.

```python
payload = (
    docs.take_row_ids(row_ids, snapshot_id=source_snapshot_id)
    .select(["id", "content", "image"])
    .with_row_id()
    .to_arrow()
    .to_pylist()
)

payload_by_row_id = {row["_ROW_ID"]: row for row in payload}
ordered_payload = [payload_by_row_id[row_id] for row_id in row_ids]
```

This pattern keeps broad candidate generation cheap: first search or filter to
produce a compact row-id manifest, then fetch only the columns needed by the
next stage.

```python
# Stage 1: broad retrieval with a narrow projection.
manifest = (
    docs.search(
        query_vector,
        column="embedding",
        snapshot_id=source_snapshot_id,
    )
    .select(["id"])
    .limit(500)
    .with_row_id()
    .to_pandas()
)

# Stage 2: expensive reranking payload, fetched only for candidates.
rerank_payload = (
    docs.take_row_ids(
        manifest["_ROW_ID"].tolist(),
        snapshot_id=source_snapshot_id,
    )
    .select(["id", "content", "image"])
    .with_row_id()
    .to_list()
)

payload_by_row_id = {row["_ROW_ID"]: row for row in rerank_payload}
rerank_inputs = [
    {
        "row_id": row_id,
        "candidate_rank": rank,
        "doc": payload_by_row_id[row_id],
    }
    for rank, row_id in enumerate(manifest["_ROW_ID"])
]
```

For offline inference, feature backfills, or training splits, store row IDs in a
work queue or manifest table together with the source table snapshot or tag used
to produce them. Workers can then read row-id batches and fetch only the columns
they need:

```python
def run_inference_worker(docs, row_id_batch, source_snapshot_id):
    rows = (
        docs.take_row_ids(row_id_batch, snapshot_id=source_snapshot_id)
        .select(["id", "content"])
        .with_row_id()
        .to_list()
    )

    outputs = []
    for row in rows:
        outputs.append(
            {
                "id": row["id"],
                "source_row_id": row["_ROW_ID"],
                "prediction": model_predict(row["content"]),
            }
        )
    return outputs
```

Best practices:

- Treat `_ROW_ID` as an internal row handle, not a business identifier.
- Store the source snapshot or tag with persisted row-id manifests.
- Pass the stored `snapshot_id` or `tag_name` when consuming persisted row-id
  manifests.
- Use business keys or application columns when writing inference or training
  outputs back to a table.
- Reorder `take_row_ids` results client-side when input order matters.
