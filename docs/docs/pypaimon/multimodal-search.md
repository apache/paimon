---
title: "Vector and Full-text Search"
description: "Build indexes and retrieve candidates using vector, full-text, or hybrid queries."
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

# Vector and Full-text Search

Build indexes and retrieve candidates using vector, full-text, or hybrid queries. Start with the `docs` table from [Multimodal Tables](./multimodal-tables#create), including its `content` and `embedding` columns. Examples with additional vector or partition columns require a table with those fields.

## Prerequisites

The index implementations used below require optional packages:

```shell
python -m pip install 'pypaimon[vindex,full-text]'
```

Use Python 3.9 or newer for this combination. Index creation requires data in the
target columns; replace the small illustrative vectors with data appropriate for
the chosen index's training requirements. In the examples, `docs` is the table,
`pm` is `pypaimon.multimodal`, and query vectors must match their column dimensions.

## Create Index

Use `create_index` to create the global indexes used by search APIs. The
`index_type` argument is required; the multimodal API does not choose a default
index implementation. Use `full-text` for full-text indexes.

```python
docs.create_index("embedding", index_type="ivf-pq")
docs.create_index("content", index_type="full-text")
```

### Build vector indexes with Ray

Pass `execution="ray"` to distribute native vector-index shards across Ray
workers. BTree, Bitmap and full-text builds still use local execution.

```python
docs.create_index(
    "embedding", index_type="ivf-flat",
    options={"global-index.row-count-per-shard": "1000000"},
    execution="ray", concurrency=4, ray_remote_args={"num_cpus": 1},
)
```

The driver plans uncovered row ranges from one snapshot. Workers build files;
the driver publishes their index entries in one commit after all shards succeed.
`concurrency` bounds running shards; `global-index.build.parallelism` controls
only local thread-based builds. Install the same `pypaimon[ray,vindex]`
dependencies on every worker and use storage accessible to the entire cluster.

Write tasks require `max_retries=0` and `retry_exceptions=False` (set by default).
On a task failure, the driver stops submissions, waits for dispatched tasks,
and removes their uncommitted output files. Rerun the build after the failure;
already committed index coverage is skipped. Driver/cluster termination can
still leave orphan files for normal orphan-file maintenance. Retain the source
snapshot's files until the build and its commit finish.

## Search

Use `search` for one vector query or one full-text query.

If the table has exactly one vector column, `column` can be omitted for vector
search. A string query is shorthand for a full-text match query when the table
has exactly one text column. To target a specific text column, pass `column`.

Use `pre_filter` to prune search candidates before ranking. Use `where()` to
filter the rows read from the search result. Both `pre_filter` and `where()`
accept SQL-like predicate strings. On data-evolution tables, full-text
`pre_filter` supports ordinary data columns as well as partition columns.
For primary-key full-text search, only partition predicates are supported.

Full-text data predicates are evaluated before Top-K selection. PyPaimon reads
the predicate's columns and row IDs at the search snapshot, then passes the
matching row IDs to the full-text index. Scalar indexes can prune this read,
but partial scalar-index coverage does not exclude matching rows covered by
the full-text search plan. This filter read can scan all candidate rows;
partition-only predicates keep the existing partition-pruning path. For
unindexed data in `full-text-index.search-mode=full`, filtering preserves the
unfiltered corpus used to calculate BM25 statistics.

For data-evolution vector search, a scalar index may return candidates rather than exact
matches, for example for BTree string-prefix or substring predicates, or when
part of a conjunction is unsupported. Such index candidates are excluded with
a warning by default, so the result can contain fewer than the requested rows.
Set the table option `global-index.filter.refine-from-data=true` to verify those
candidates before vector top-k selection. This reads the filter columns at the
search snapshot and may scan every candidate row; exact index matches need no
extra read. This applies to single and batch vector queries, locally and on Ray.

Each execution of `search`, `search_vectors`, or `search_hybrid` reads one
snapshot across candidate search, filtering, reranking, and result lookup.
Concurrent commits become visible on the next execution, including when reusing
the same query object. Explicit snapshot, tag, and timestamp selectors are
honored. All routes in a hybrid search and all vectors in a batch share that
execution's snapshot. Keep the snapshot's data files available for the duration
of the query; capturing a read view does not prevent snapshot expiration.

```python
neighbors = (
    docs.search(
        [0.1, 0.2, 0.3],
        column="embedding",
        pre_filter="category = 'lake'",
    )
    .limit(10)
    .to_arrow()
)

matches = (
    docs.search("paimon vector", column="content", pre_filter="category = 'lake'")
    .limit(10)
    .to_pandas()
)
```

```python
matches = (
    docs.search(
        '{"match":{"query":"paimon vector","operator":"And"}}',
        column="content",
    )
    .limit(10)
    .to_list()
)
```

## Maintain index coverage and rebuild indexes

Keep the desired index definitions in your ingestion/backfill job and call
`maintain_indexes` after committing data, or from a scheduled job:

```python
indexes = [{"column": "embedding", "type": "ivf-flat", "options": {
    "ivf-flat.nlist": "128", "global-index.row-count-per-shard": "1000000",
}}]
added_files = docs.maintain_indexes(indexes)
```

Each pass automatically discovers uncovered ranges and builds them. Repeating a
completed pass is a no-op. For embedding updates, configure the table with
`global-index.column-update-action=DROP_PARTITION_INDEX`: the data commit removes
stale coverage, and a later maintenance pass rebuilds the affected partitions.
Keep query search mode `full` if queries must include rows before that rebuild.
Definitions must remain in your job configuration because dropping the last
index file also removes the manifest information needed to rediscover its type.

To change build parameters or repair coverage left stale by `IGNORE`, use an
explicit rebuild, optionally scoped to partitions:

```python
docs.maintain_indexes(indexes, rebuild=True, partitions={"date": "2026-10-08"})
```

All requested indexes are built against one captured snapshot and published in
one commit. Rebuild deletes the matching old entries and adds replacements in
that same commit; old files remain available to retained snapshots. Any
concurrent snapshot commit causes publication to fail. Schedule another pass
from the latest snapshot rather than reusing its commit messages. A failed build
leaves existing coverage intact; ambiguous commit failures leave output files
for normal orphan-file maintenance rather than risking deletion of published
files.

The API requires data evolution and a latest-table handle. It uses the Python
committer for its strict snapshot check. This is an opt-in maintenance pass, not
a background daemon: an application scheduler or Ray job owns its trigger,
backoff, and retry policy. For example, a Ray task can load its catalog/table and
run a pass after a completed backfill; serialize maintenance jobs for each table
to avoid competing rebuilds. Continuous writers may require a quiet interval
because this initial conservative guard rejects even unrelated appends.

## Scores and Result Ordering

On data-evolution tables, use `with_score()` to append a `float64` relevance
column and `order_by_score()` to sort by descending score, with ascending
`_ROW_ID` for ties. Both methods are optional: `with_score()` alone preserves
the existing result order, and `order_by_score()` does not require projecting
scores. Without either method, result behavior is unchanged.

```python
neighbors = (
    docs.search([0.1, 0.2, 0.3], column="embedding")
    .select(["id", "content"])
    .with_score("relevance")
    .order_by_score()
    .limit(10)
    .to_arrow()
)
```

The default score column is `_score`. A custom name must not conflict with a
table column or a system field. Scores use the search engine's existing
higher-is-better convention: L2 uses `1 / (1 + squared_distance)`, cosine uses
cosine similarity, and inner product uses the dot product. Full-text results
expose BM25 scores; hybrid results expose the selected ranker's fusion scores.
Scores from different metrics or rankers are not directly comparable.

These methods also work with full-text, hybrid, and batch vector queries, and
with local or Ray vector execution. Batch output retains input-query order and
each row receives its score for that query. `where()` still filters selected
rows during lookup, so it can return fewer than the requested number of hits.

When only row IDs and scores are needed, explicitly project `_ROW_ID`:

```python
hits = (
    docs.search([0.1, 0.2, 0.3], column="embedding")
    .select(["_ROW_ID"])
    .with_score()
    .order_by_score()
    .limit(10)
    .to_arrow()
)
```

Use `select([]).with_score()` for scores alone. When either score method is
enabled and the explicit projection contains only `_ROW_ID` or is empty, the
query skips final row lookup if there is no `where()` and query authorization
is disabled. Raw search, prefiltering, and vector refinement can still read
data. Historical snapshot and deletion semantics remain the same. Plain
`select([])` without either method retains its existing behavior.

## Distributed Vector Search

Use `execution="ray"` to execute vector queries across Ray workers and return
Arrow results to the driver. This supports single and batch vector queries on
data-evolution tables; primary-key tables and hybrid queries are not supported
by this execution mode.

Install the same PyPaimon and index dependencies on the driver and workers:

```shell
python -m pip install 'pypaimon[ray,vindex]'
```

```python
import ray

ray.init()  # Use address="auto" to connect to an existing cluster.

neighbors = (
    docs.search(
        [0.1, 0.2, 0.3],
        column="embedding",
        pre_filter="category = 'lake'",
    )
    .select(["id", "content"])
    .limit(10)
    .to_arrow(
        execution="ray",
        concurrency=4,
        ray_remote_args={"num_cpus": 1},
    )
)
```

`execution` defaults to `"local"`. `concurrency` is a positive integer limiting
the number of in-flight Ray tasks, with a default of 4. `ray_remote_args` supplies
Ray task options, including resources and retry settings; `num_returns` is
managed by PyPaimon. These two arguments require `execution="ray"`.

For single and batch vector queries, `to_pandas()` and `to_list()` accept the
same `execution`, `concurrency`, and `ray_remote_args` arguments as `to_arrow()`.
For example, use `query.to_pandas(execution="ray", concurrency=4)` to return a
DataFrame, or one DataFrame per query for a batch. Result conversion runs on the
driver after the search and lookup complete.
Single-vector Ray queries require finite query values and fail if stored
vectors produce NaN scores.

Batch queries use the same execution options and return one Arrow table per
input vector, in input order:

```python
batch_neighbors = (
    docs.search_vectors([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]], column="embedding")
    .select(["id", "content"])
    .limit(10)
    .to_arrow(execution="ray", concurrency=4)
)
```

Each batch task handles all query vectors for one split: index workers reuse
an open shard across bounded query blocks, and raw workers stream each data
split once with a separate top-k for each query. For refinement, the driver
selects each query's global candidates, then workers stream their union once per
read split, scoring each row only for the queries that selected it. Shared final
row lookup runs on the driver. The entire batch uses one read snapshot.
Candidate traffic and result memory grow with the number of query vectors;
split large query collections into smaller batches when necessary.

The driver fixes one read snapshot and plans the query. Workers search individual
index shards and, when required by the table's search mode, scan unindexed data.
They return candidate row IDs and scores. For vector queries with
refinement enabled, the driver first selects the global candidate set, then
workers read and score those candidate vectors using the index's persisted
metric. The driver merges their results and retains the local rules for
filtering and final row lookup.
In particular, `pre_filter` filters candidates before ranking; `where()` filters
the selected rows and can return fewer than the requested number of results.

For indexed vector searches, workers evaluate scalar predicates and build
live-row filters at the query snapshot. Tasks using the same scalar index files
share one Ray scalar-index evaluation; its result is passed directly to dependent
tasks without materializing the bitmap on the driver. Candidate verification
reads only filter columns and row IDs in the task's shard range, without scanning
the scalar indexes again. The final shard filter is reused across a batch's query
vectors. This lets expensive verification scale with search concurrency; partially
overlapping scalar index inputs can still cause repeated index reads.

Index search, raw scans, refinement, lookup, and task retries use the same read
snapshot. The existing `snapshot_id` and `tag_name` arguments to `search()` also
work with Ray execution. A failed task fails the query rather than returning
partial results. Snapshot pinning does not prevent file expiration; the files
needed by the query must remain available for its duration.

All workers must be able to access the table's storage. Local filesystem paths
are suitable for a local Ray cluster; multiple nodes require shared storage.
Each index task searches one shard, while its native index I/O settings still
apply. Raw-scan parallelism is limited by the number of planned read splits,
controlled by the table's `source.split.target-size` option. Refinement also
uses planned read splits; workers return scores for merging.
Final row lookup runs on the driver. Candidate traffic grows
with the number of index shards and the configured refinement budget, so Ray
execution is most useful when shard search or raw scanning outweighs scheduling
and transfer costs. Small queries can be faster locally.

Local and Ray index searches merge shard results incrementally in plan order.
The concurrency limit bounds running tasks plus completed results waiting for
earlier shards, so a slow shard can delay further submissions. Global top-k
selection still uses all merged candidate scores, whose storage grows with the
number of unique candidate rows per query.

## Full-text and Hybrid queries on Ray

Full-text and Hybrid queries accept `execution="ray"`, `concurrency`, and
`ray_remote_args` on `to_arrow`, `to_pandas`, and `to_list`, matching vector queries:

```python
rows = (docs.search("robot grasp", column="content")
        .with_score().order_by_score().limit(20)
        .to_arrow(execution="ray", concurrency=4))
```

This path supports data-evolution tables and requires `pypaimon[ray,full-text]`
on every worker (plus `vindex` for native vector routes). Index shards run as
bounded Ray tasks. Full-text raw fallback runs on one worker with the complete
uncovered corpus, preserving existing BM25 statistics; that corpus must fit the
worker's memory. Pre-filters and deletion vectors keep the local ranking semantics.

Hybrid routes share one resolved snapshot, use Ray for their shard work, and
reuse the existing RRF, MRR, or weighted-score fusion on the driver. Routes run
successively so `concurrency` bounds the whole query. Final Arrow/Pandas row lookup
remains on the driver. Native resources close on workers; task errors propagate
and pending read tasks are cancelled.

## Search Hybrid

Use `search_hybrid` to combine vector and full-text routes, then rerank the
merged candidates. Create the indexes required by the routes you use.

Pass route specs to `search_hybrid`. Each route can set its own candidate
`limit`, `weight`, and vector index `options`. String text routes infer the
text column when the table has exactly one text column. To target a specific
text column, pass `column` to `pm.text_route`.

`pre_filter` is applied before ranking. It accepts a SQL-like predicate string.
On data-evolution tables, ordinary data predicates are applied to both vector
and full-text routes before each route selects its candidates. The vector
route retains the `global-index.filter.refine-from-data` behavior described
above, while the full-text route verifies data predicates through a filter-column
read. Partition-only predicates prune both routes without that extra read.

```python
# This example assumes the table is partitioned by dt.
hybrid = (
    docs.search_hybrid(
        [
            pm.vector_route("embedding", query_vector, limit=50),
            pm.text_route("paimon vector", column="content", weight=0.2),
        ],
        pre_filter="dt = '2026-07-01'",
    )
    .rerank("rrf")
    .limit(10)
    .to_list()
)
```

For multiple vector fields, add multiple vector routes:

```python
hybrid = (
    docs.search_hybrid(
        [
            pm.vector_route(
                "image_embedding",
                image_vector,
                weight=0.7,
                limit=50,
                options={"nprobe": "8"},
            ),
            pm.vector_route(
                "text_embedding",
                text_vector,
                weight=0.3,
                limit=50,
            ),
            pm.text_route("paimon vector", column="content", weight=0.2),
        ],
        ranker="weighted_score",
    )
    .limit(10)
    .to_list()
)
```

Dictionary route specs are also accepted:

```python
hybrid = docs.search_hybrid(
    [
        {
            "column": "image_embedding",
            "vector": image_vector,
            "weight": 0.7,
            "limit": 50,
            "options": {"nprobe": "8"},
        },
        {
            "column": "text_embedding",
            "vector": text_vector,
            "weight": 0.3,
            "limit": 50,
        },
    ],
)
```

Dictionary vector routes also accept `anns_field`, `data`, and `param` aliases.

## Search Vectors

Use `search_vectors` for multiple query vectors against one vector column. It
returns one result set for each input vector, preserving input order.
Batch results share a lookup of their combined row IDs, so overlapping results
do not require reading the same row separately for each query. The limit and
post-filter still apply to each query's result set.

```python
batch_neighbors = (
    docs.search_vectors(
        [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
        ],
        column="embedding",
        pre_filter="category = 'lake'",
    )
    .limit(10)
    .to_list()
)
```

## Return search rows as a Ray Dataset

Vector queries support `to_ray()` to run candidate search on Ray and fetch the
selected columns on workers. Single queries return one Dataset; `search_vectors`
returns one Dataset per input vector. Candidate IDs and scores are merged on the
driver, while projected row data stays in Ray blocks.

```python
ds = (docs.search([0.1, 0.2, 0.3], column="embedding")
      .select(["content", "image"])
      .with_score().order_by_score().limit(100)
      .to_ray(concurrency=4, override_num_blocks=4))
```

Search runs eagerly; row lookup is lazy and uses the same captured snapshot.
Retain that snapshot's files until downstream Dataset actions finish. `where`
filters the candidates during lookup without refilling top-k. `order_by_score`
performs a distributed global sort, breaking ties by ascending row ID. Empty
results retain their output schema, including the score column.

BLOB values are serialized descriptors, as with `scan().to_ray()`. Pass the
Dataset to `docs.map_with_blobs(...)` to resolve payloads on workers. Projections
must be nonempty and have unique names. This API requires data evolution;
`execution="local"` selects local candidate search while keeping distributed
lookup. Batch queries share candidate-search work but have separate lazy lookup
pipelines for each query.
