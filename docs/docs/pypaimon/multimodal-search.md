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

## Search

Use `search` for one vector query or one full-text query.

If the table has exactly one vector column, `column` can be omitted for vector
search. A string query is shorthand for a full-text match query when the table
has exactly one text column. To target a specific text column, pass `column`.

Use `pre_filter` to prune search candidates before ranking. Use `where()` to
filter the rows read from the search result. Both `pre_filter` and `where()`
accept SQL-like predicate strings. For full-text search, `pre_filter` must only
reference partition columns.

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
    docs.search("paimon vector", column="content")
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

## Search Hybrid

Use `search_hybrid` to combine vector and full-text routes, then rerank the
merged candidates. Create the indexes required by the routes you use.

Pass route specs to `search_hybrid`. Each route can set its own candidate
`limit`, `weight`, and vector index `options`. String text routes infer the
text column when the table has exactly one text column. To target a specific
text column, pass `column` to `pm.text_route`.

`pre_filter` is applied before ranking. It accepts a SQL-like predicate string.
When a hybrid query has a full-text route, `pre_filter` must only reference
partition columns.

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

## Explain and Profile Search

Call `explain()` on a vector, batch vector, full-text, or hybrid query to inspect
its planned index and raw-data work. It returns a `SearchExplainResult` with
structured fields and a printable summary. It does not run vector/text searches
or fetch result rows. Planning can read data to evaluate primary-key scalar
predicates, so it is not always metadata-only.

```python
query = docs.search(query_vector, column="embedding").select(["content"]).limit(10)
plan = query.explain()
print(plan)

route = plan.routes[0]
print(route.index_file_count, route.index_bytes, route.index_types)
print(route.indexed_range_rows, route.raw_range_rows, route.overlapping_range_rows)
```

Each route reports its search mode, column, candidate limit, query count,
planning snapshot, index splits/files/bytes, raw splits, scalar-index file count,
and whether scalar or partition filters are configured.
Hybrid plans retain route order, weights, the fusion
ranker, and the route worker limit. A query's projection and post-filter flag
are included; `where()` applies during lookup after search top-k and can reduce
the number of returned rows.

Coverage counts are unions of inclusive row-ID ranges in the plan, not live-row
counts or measured ANN recall. Indexed and raw ranges can overlap, for example
when scalar-index coverage requires fallback. Do not add these counts as if they
were disjoint. Primary-key source-file plans report `None` for global row-ID
coverage because their positions are local to source files.

Call `profile()` to execute the search once and obtain its result together with
runtime measurements. Use `profile.result` directly; subsequently calling
`to_arrow()` would execute a second search.

```python
profile = query.profile()
print(profile)
neighbors = profile.result  # Arrow table, with the normal projection and filters
print(profile.elapsed_ms, profile.lookup_ms, profile.output_rows)
print(profile.route_metrics[0]["timings_ms"])
print(profile.route_metrics[0]["counters"])

batch_profile = docs.search_vectors(query_vectors, column="embedding").limit(10).profile()
batch_neighbors = batch_profile.result  # One Arrow table per query vector
```

`SearchProfileResult.plan` describes that execution's plan. A separate earlier
`explain()` may describe a different snapshot if the table changes. Each
`profile()` execution shares one snapshot across hybrid routes, filtering,
raw fallback, and result lookup, just like normal search. Each route's
`snapshot_id` and the entries in `lookup_snapshot_ids` describe that same read
view, with one lookup entry per batch query. Reusing the query captures a fresh
snapshot unless it has an explicit time-travel selector.

The built-in local vector, batch vector, full-text, and hybrid search builders
also expose `explain()` and `profile()`. Builder profiles return their normal
scored index result (a list for batch search), without fetching Arrow rows, so
`lookup_ms`, `lookup_snapshot_ids`, and `output_rows` are `None`.

Runtime metrics are opt-in. Normal execution does not collect profiling clocks
or counters. The available measurements are:

| Measurement | Meaning |
| --- | --- |
| `elapsed_ms` | Wall time for this call, including lookup for multimodal queries. |
| `planning`, `search` | Per-route planning and search wall time in `timings_ms`. |
| `index_open`, `index_search` | Reader opening and accumulated index-search time. Full-text lazy loading is included in `index_search`. |
| `pre_filter`, `raw_read_score`, `refine` | Instrumented filtering, raw-data reading/scoring, and vector refinement time. Single-vector refinement also includes its nested `raw_read_score` stage. |
| `fusion_ms`, `lookup_ms` | Hybrid result fusion and multimodal result lookup time. |
| `index_searches`, `peak_index_searches` | Number of index calls and peak outstanding calls per route, not native worker-thread counts. A batch call counts once. |
| `index_rows_before_filter`, `index_rows_after_filter` | Sum of submitted index row-range sizes before and after include-row-ID filtering; fully pruned splits make no index call. These are not counts of vectors visited by ANN. |
| `index_candidates`, `refine_candidates` | Candidates returned by indexes or submitted for refinement, summed across shards/queries before final merging. |
| `raw_rows_read`, `refine_rows_read` | Rows yielded to raw scoring or refinement. Batch refinement reads shared candidate rows once; these are not storage-level I/O row counts. |
| `result_rows`, `output_rows` | Per-route result count before fusion/lookup, and final Arrow row count. Batch counts sum all queries. |

Times are milliseconds. Stage times are inclusive and may overlap or sum across
concurrent work; adding them does not yield wall time. Missing measurements mean
the phase was not instrumented or invoked, not necessarily that it was free.
Primary-key readers expose total planning/search time and inherited index/refine
stages, but do not currently report every raw-read or refinement row counter.
Profiling incurs the real search cost plus measurement overhead; it is not an
estimate or a benchmark isolated from caches and other queries.
