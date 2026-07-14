---
title: "Vector Index"
sidebar_position: 9
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

# Vector Index

Primary key tables can maintain a bucket-local approximate nearest neighbor (ANN) index together
with their data. Unlike a global vector index created by `create_global_index`, a primary-key
vector index is part of the normal write and compaction lifecycle. Paimon builds it synchronously
when complete compact-output files are produced and commits the index changes together with those
files.

Use a primary-key vector index when vectors are frequently updated and the ANN index should follow
the primary-key table's compaction lifecycle. For append-only or Data Evolution tables whose index
is built separately from writes, see
[Global Vector Index](../multimodal-table/global-index/vector).

## Requirements

A table with a primary-key vector index must satisfy all of the following:

- It is a primary-key table in fixed-bucket mode (`bucket > 0`) or postpone-bucket mode
  (`bucket = -2`).
- `deletion-vectors.enabled` is `true`, except for `first-row`, where it must be `false`.
- Its merge engine is `deduplicate`, `partial-update`, `aggregation`, or `first-row`.
- The indexed column is a `VECTOR` whose element type is `FLOAT`.
- `pk-clustering-override` is disabled.
- The configured vector index implementation is available on every writer and reader classpath.

The first release supports exactly one indexed vector column per table. The option layout is
field-scoped so that more independently indexed vector columns can be supported in a future
release.

## Create Table

The following Flink SQL example creates a three-dimensional vector column and maintains an
IVF-Flat index for it. Use the dimension produced by your embedding model in production.

```sql
CREATE TABLE item_embeddings (
    id BIGINT,
    payload STRING,
    embedding ARRAY<FLOAT> COMMENT '__VECTOR_FIELD;3',
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket' = '16',
    'deletion-vectors.enabled' = 'true',
    'pk-vector.index.columns' = 'embedding',
    'fields.embedding.pk-vector.index.type' = 'ivf-flat',
    'fields.embedding.pk-vector.distance.metric' = 'cosine',
    'fields.embedding.pk-vector.index.options' = '{"nlist":"256"}'
);
```

Use the same properties in Spark SQL:

```sql
CREATE TABLE item_embeddings (
    id BIGINT,
    payload STRING,
    embedding ARRAY<FLOAT> COMMENT '__VECTOR_FIELD;3'
) USING paimon
TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '16',
    'deletion-vectors.enabled' = 'true',
    'pk-vector.index.columns' = 'embedding',
    'fields.embedding.pk-vector.index.type' = 'ivf-flat',
    'fields.embedding.pk-vector.distance.metric' = 'cosine',
    'fields.embedding.pk-vector.index.options' = '{"nlist":"256"}'
);
```

The vector comment directive converts the SQL `ARRAY<FLOAT>` column to Paimon's fixed-length
`VECTOR<FLOAT>` type. Java API users can define the column directly with
`DataTypes.VECTOR(3, DataTypes.FLOAT())`.

### Options

| Option | Required | Description |
|---|---|---|
| `pk-vector.index.columns` | Yes | Indexed vector column. Exactly one column is supported in the first release. |
| `fields.<column>.pk-vector.index.type` | Yes | ANN implementation, such as `ivf-flat`, `ivf-pq`, `ivf-hnsw-flat`, `ivf-hnsw-sq`, or `lumina`. |
| `fields.<column>.pk-vector.distance.metric` | No | `l2`, `cosine`, or `inner_product`. The default is `inner_product`. |
| `fields.<column>.pk-vector.index.options` | No | JSON object containing build options for the selected ANN implementation. Unqualified keys are scoped to that implementation. |
| `fields.<column>.pk-index.compaction.level-fanout` | No | Number of similarly sized index groups which triggers a rebuild and maximum row-count ratio within one size tier. Shared by vector, BTree, and Bitmap primary-key indexes. Default: `5`. |
| `fields.<column>.pk-index.compaction.stale-ratio-threshold` | No | Ratio of rows from inactive source files which triggers an index rebuild. Shared by vector, BTree, and Bitmap primary-key indexes. Default: `0.2`. |

For algorithm-specific build and search options, see
[Vector Index](../multimodal-table/global-index/vector).

## Index Maintenance

Paimon builds immutable ANN segments from complete compact-output data files inside each bucket.
The index segment records the source data files and maps ANN ordinals back to their physical row
positions. Compact-output data-file and index-file changes are committed atomically, so a reader
never observes an index from a different compact-output snapshot.

For a postpone-bucket table, foreground writes remain write-only. Fixed-bucket batch writes produce
Level-0 files in real buckets, while postponed writes produce files in bucket `-2`. These rows do not
become visible to normal reads or vector search until a batch compact runs. The background compact
processes both kinds of pending files, builds their ANN indexes, and publishes the data and index
changes in one atomic commit.

ANN compaction is configured independently from data compaction. It does not inherit
`vector.target-file-size`, `num-sorted-run.compaction-trigger`, or
`compaction.delete-ratio-threshold`.

The maintenance behavior depends on the merge engine:

- `deduplicate`: an update indexes the latest row and the deletion vector hides the replaced
  physical row. A delete removes the old row from search results through the deletion vector.
- `partial-update`: Paimon builds the vector index from the lookup-completed Level-1
  compact-output row.
- `aggregation`: Paimon builds the vector index from the aggregated Level-1 compact-output row.
- `first-row`: Paimon indexes the retained first row. Deletion vectors must be disabled because
  later rows with the same primary key are ignored rather than deleting the retained row.

When compaction replaces source data files, Paimon removes ANN segments that reference those files
and creates replacement segments for the new compact-output files. Small outputs are indexed as
well; there is no minimum-row threshold before a new segment can be built.

The index follows compaction freshness. Newly appended level-0 files are not ANN sources, so a
streaming write may not be searchable until compaction has produced and committed its complete
level-1 output. Wait for that compaction when read-after-write vector-search visibility is
required. Batch writes which wait for compaction can publish the data and its index together.

## Search

### Exact Rerank

Primary-key vector search can retrieve more ANN candidates and rerank them with the original
vectors stored in the table. For example, the following table option retrieves up to four times
the requested Top-K from the `ivf-flat` index before computing exact distances:

```sql
'fields.embedding.ivf-flat.refine_factor' = '4'
```

The option is disabled by default. Its configuration semantics are the same as for a Data
Evolution vector index:

- `refine_factor`, `refine-factor`, `rerank_factor`, and `rerank-factor` are accepted.
- A query option overrides every table option. Within either set of options, field and index
  prefixes take precedence over less specific prefixes. For example,
  `fields.embedding.ivf-flat.refine_factor` takes precedence over
  `fields.embedding.ivf.refine_factor`, which takes precedence over `ivf.refine_factor` and then
  `refine_factor`. The normalized underscore form of an index type, such as `ivf_flat`, is also
  accepted after the configured index name.
- The factor must be a positive integer. A factor of `1` performs exact reranking without
  retrieving additional ANN candidates.

Only candidates returned by ANN can win the rerank, so a larger factor can improve recall but does
not guarantee the exact global Top-K. It also increases ANN work and data-file I/O. Files without
an active ANN segment are already searched exactly and are kept separate from approximate
candidates until the final Top-K merge.

For distributed Spark searches, executors return bounded ANN and exact candidate streams. The
driver globally merges the ANN candidates and rereads their original vectors for exact reranking;
this does not start a second Spark job.

### Spark SQL

Use the `vector_search` table-valued function. Spark exposes the ANN score through the
`__paimon_search_score` metadata column.

```sql
SELECT id, payload, __paimon_search_score
FROM vector_search(
    'item_embeddings',
    'embedding',
    array(0.1f, 0.2f, 0.3f),
    10,
    map('ivf.nprobe', '32')
);
```

The query vector dimension must match the indexed column dimension. For partitioned tables, Spark
applies a partition predicate before running ANN and merging the global Top-K.
When `spark.paimon.vector-search.distribute.enabled` is `true`, Spark distributes sufficiently
large groups of bucket-local ANN searches across executors and merges their task-local Top-K
results on the driver. Small plans stay local to avoid Spark job startup overhead.

### Flink SQL

Flink exposes vector search as a procedure and returns JSON-serialized rows. Use `projection` to
avoid reading columns that are not needed.

```sql
CALL sys.vector_search(
    `table` => 'default.item_embeddings',
    vector_column => 'embedding',
    query_vector => '0.1,0.2,0.3',
    top_k => 10,
    projection => 'id,payload',
    options => 'ivf.nprobe=32'
);
```

### Java API

```java
GlobalIndexResult result = table.newVectorSearchBuilder()
        .withVectorColumn("embedding")
        .withVector(queryVector)
        .withLimit(10)
        .withOption("ivf.nprobe", "32")
        .executeLocal();

ReadBuilder readBuilder = table.newReadBuilder();
TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(row -> consume(row));
}
```

## Query Planning

A search captures one table snapshot, plans the active ANN segments for every selected bucket,
searches those segments, and merges their candidates into one global Top-K. The returned candidates
are materialized from the source data files by physical row position. Deletion vectors are applied
while searching and reading, so stale versions and deleted rows are not returned.

For low latency on object storage, cache data files and ANN payloads with a caching file system.
The first query may still need to download index files; subsequent queries can search the local
cached payloads and fetch only the selected data-file positions.

## Limitations

- Exactly one vector index column is supported per table in the first release.
- Only `FLOAT` vectors are supported.
- Dynamic-bucket and `pk-clustering-override` tables are not supported.
- Flink's procedure returns rows but does not expose the ANN score as a separate column.
- Vector search is snapshot-scoped batch reading; streaming search and lateral vector search for
  primary-key tables are not supported.
