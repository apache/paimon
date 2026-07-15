---
title: "Full-Text Index"
sidebar_position: 10
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Full-Text Index

Primary-key tables can maintain a bucket-local full-text index together with compacted data. The
index follows the primary-key table's write and compaction lifecycle and maps search results back
to physical rows in compact-output files. It is different from a global full-text index created by
`create_global_index`, which uses global row IDs and is built separately from normal writes.

Use a primary-key full-text index when indexed text can be updated or deleted and search results
must follow the table's deletion vectors. For append-only or Data Evolution tables, see
[Global Index](../multimodal-table/global-index).

## Requirements

A table with a primary-key full-text index must satisfy all of the following:

- It is a primary-key table in fixed-bucket mode (`bucket > 0`) or postpone-bucket mode
  (`bucket = -2`).
- The indexed column is `CHAR`, `VARCHAR`, or `STRING`.
- Exactly one full-text column is configured in the first release.
- `deletion-vectors.enabled` is `true` and `deletion-vectors.merge-on-read` is `false`, except for
  the `first-row` merge engine, where deletion vectors must remain disabled.
- `pk-clustering-override` is disabled.
- The `paimon-full-text` module and its native `paimon-full-text-index` dependency are available on
  every writer, compactor, and reader classpath.

A column can belong to only one source-backed primary-key index family. For example, the same
column cannot own both a primary-key Bitmap index and a primary-key full-text index.

## Create Table

The full-text implementation is selected automatically. Configure the indexed column and,
optionally, its analyzer settings.

The following Flink SQL example uses the `jieba` tokenizer:

```sql
CREATE TABLE articles (
    id BIGINT,
    title STRING,
    content STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket' = '16',
    'deletion-vectors.enabled' = 'true',
    'pk-full-text.index.columns' = 'content',
    'fields.content.pk-full-text.index.options' = '{"tokenizer":"jieba"}'
);
```

Use the same properties in Spark SQL:

```sql
CREATE TABLE articles (
    id BIGINT,
    title STRING,
    content STRING
) USING paimon
TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '16',
    'deletion-vectors.enabled' = 'true',
    'pk-full-text.index.columns' = 'content',
    'fields.content.pk-full-text.index.options' =
        '{"tokenizer":"jieba"}'
);
```

### Options

| Option | Required | Description |
|---|---|---|
| `pk-full-text.index.columns` | Yes | Indexed character column. Exactly one column is supported in the first release. |
| `fields.<column>.pk-full-text.index.options` | No | JSON object containing native full-text analyzer options. Keys may omit the `full-text.` prefix. |
| `fields.<column>.pk-index.compaction.level-fanout` | No | Number of similarly sized full-text archive groups which triggers an LSM rebuild and maximum row-count ratio within one size tier. Shared by source-backed primary-key indexes. Default: `5`. |
| `fields.<column>.pk-index.compaction.stale-ratio-threshold` | No | Ratio of rows from inactive source files which triggers an archive rebuild. Shared by source-backed primary-key indexes. Default: `0.2`. |
| `global-index.search-mode` | No | Coverage policy for search. `fast` is the default; `full` and `detail` also search uncovered compaction-visible files through temporary indexes. |
| `global-index.thread-num` | No | Maximum concurrency shared by full-text index reads and temporary fallback builds. |

Analyzer options can also be supplied as table-level `full-text.*` properties. A conflicting
table-level and field-scoped value is rejected so that the effective index definition remains
deterministic. Common settings include:

| Analyzer option | Default | Description |
|---|---|---|
| `tokenizer` | `default` | `default`, `simple`, `whitespace`, `raw`, `ngram`, or `jieba`. |
| `ngram.min-gram` | `2` | Minimum gram length for the `ngram` tokenizer. |
| `ngram.max-gram` | `2` | Maximum gram length for the `ngram` tokenizer. |
| `lower-case` | `true` | Whether configurable tokenizers lowercase emitted tokens. |
| `stem` | `false` | Whether to apply stemming. |
| `remove-stop-words` | `false` | Whether to remove built-in stop words for the configured language. |
| `with-position` | `true` | Whether to store term positions for phrase queries. |

See the [`paimon-full-text` README](https://github.com/apache/paimon/blob/master/paimon-full-text/README.md)
for the complete native analyzer option list.

## Index Maintenance and Visibility

Paimon initially builds an immutable full-text archive for uncovered complete compact-output data
files. An eligible source must have `COMPACT` source metadata and a level greater than zero. It then
uses the same logical LSM policy as the primary-key Vector, BTree, and Bitmap indexes to consolidate
similarly sized archives and rebuild archives containing too many inactive sources. Consolidation
re-reads the selected active data files; it does not merge native archive bytes.

An archive therefore covers one or more ordered source files. Its native row-ID space concatenates
their physical positions: each source starts at the cumulative row count of the sources before it.
Every physical row consumes an ordinal, including a row whose indexed value is null; null text is
not added to the native term index. Existing single-source archives remain compatible.

The data-file and archive changes are committed atomically. Updates and deletes are filtered with
each source file's deletion vector after shifting its live positions into the archive-global row-ID
space, so old physical versions do not reappear in search results. When compaction replaces a
source file, maintenance first indexes uncovered replacement files and incrementally reclaims stale
archive sources according to the configured threshold. Builds may run asynchronously when the
calling write does not wait for index compaction; a later snapshot publishes the completed archive.

This is a **compaction-visible** index:

- Level-0 files created by foreground writes are not full-text sources, even when normal fixed-
  bucket reads can already see those rows.
- Rows become searchable after compaction publishes a complete level-1-or-higher output and its
  archive.
- Postpone-bucket writes remain pending until the batch compaction publishes them in a real bucket.
- `full` and `detail` coverage do not make level-0 files eligible. They only cover eligible compact
  files whose persistent archive is missing.

Choose the search mode according to the required latency and coverage:

- `fast`: search only persistent archives. This has the lowest query-time cost and is the default.
- `full` or `detail`: build temporary per-file native archives for uncovered compaction-visible
  files, search them with the same effective analyzer options, and delete them when the query
  finishes. Both modes have the same fallback behavior for primary-key full-text search.

## Search

The native engine ranks matches within each archive segment. Paimon maps archive-global row IDs
back to source-file positions, combines segment-local rankings with reciprocal-rank fusion (RRF),
and applies the requested global Top-K. Therefore,
`__paimon_search_score` is a deterministic synthetic RRF score, not the original native BM25
score. Deletion-vector filtering happens before each segment-local Top-K.

### Spark SQL

Use the `full_text_search` table-valued function:

```sql
SELECT id, title, __paimon_search_score
FROM full_text_search(
    'articles',
    'content',
    '{"match":{"query":"paimon lake"}}',
    10
)
ORDER BY __paimon_search_score DESC;
```

For partitioned tables, Spark can push a partition predicate before full-text ranking. Non-
partition filters cannot be pushed before Top-K and are not supported on a full-text route.

### Flink SQL

Flink exposes the same search as a procedure. Each returned procedure row contains a JSON object.
Add `__paimon_search_score` to `projection` when the RRF score is needed.

```sql
CALL sys.full_text_search(
    `table` => 'default.articles',
    `column` => 'content',
    query => '{"match":{"query":"paimon lake"}}',
    top_k => 10,
    projection => 'id,title,__paimon_search_score'
);
```

The optional `options` argument applies dynamic table options to this search, but it cannot
override the table's query authorization setting. `top_k` must be between 1 and 10,000. Results are
ordered by score descending, with deterministic JSON ordering for ties.

### Java API

```java
GlobalIndexResult result = table.newFullTextSearchBuilder()
        .withQuery("content", "{\"match\":{\"query\":\"paimon lake\"}}")
        .withLimit(10)
        .executeLocal();

ReadBuilder readBuilder = table.newReadBuilder();
TableScan.Plan plan = readBuilder.newScan()
        .withGlobalIndexResult(result)
        .plan();
try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(row -> consume(row));
}
```

## Hybrid Search

Spark `hybrid_search` can fuse primary-key vector and full-text routes by physical data-file
position when both indexes belong to the same table snapshot. The ranker may be `rrf`,
`weighted_score`, or `mrr`; route weights must be positive.

```sql
SELECT id, __paimon_search_score
FROM hybrid_search(
    'articles',
    array(named_struct(
        'field', 'embedding',
        'query_vector', array(0.1f, 0.2f, 0.3f),
        'limit', 20,
        'weight', 1.0f,
        'options', map())),
    array(named_struct(
        'column', 'content',
        'query', '{"match":{"query":"paimon lake"}}',
        'limit', 20,
        'weight', 1.0f,
        'options', map())),
    10,
    'rrf'
)
ORDER BY __paimon_search_score DESC;
```

Hybrid search cannot mix physical primary-key routes with global row-ID routes. A full-text hybrid
route supports partition filters but not non-partition filters before ranking. See
[Vector Index](vector-index) for primary-key vector maintenance and search behavior.

## Limitations

- Exactly one primary-key full-text column is supported per table in the first release.
- Dynamic-bucket and `pk-clustering-override` tables are not supported.
- Search is snapshot-scoped batch reading; streaming and lateral full-text search are not
  supported.
- Foreground level-0 files are outside full-text coverage until compaction, in every search mode.
- Full-text routes support partition pruning but not arbitrary row predicates before Top-K.
- Writers, compactors, and readers all require the native full-text implementation.
