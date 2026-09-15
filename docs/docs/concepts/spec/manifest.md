---
title: "Manifest"
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

# Manifest

Manifests describe the files belonging to a table state. A [snapshot](./snapshot) references
manifest lists for data and changelog files, and an optional index manifest for table indexes.
This metadata lets readers plan a scan without listing every data directory.

## File Kinds and Encoding

The default `manifest/` directory contains three kinds of Avro files:

| File name pattern | Contents | On-disk `_VERSION` identifier |
| --- | --- | --- |
| `manifest-list-<uuid>-<id>` | Metadata about data or changelog manifests. | `2` |
| `manifest-<uuid>-<id>` | Data-file or changelog-file additions and deletions. | `2` |
| `index-manifest-<uuid>-<id>` | Table index additions and deletions. | `1` |

Each serialized record begins with an integer field named `_VERSION`. This is a permanent
format identifier, not a counter that increases whenever nullable fields are added. The field
tables below describe the remaining fields. Newer nullable metadata can be absent in older files.

## Manifest List

A manifest list contains one record per referenced manifest. Its statistics allow readers to
skip manifests before opening them.

| Field | Type | Meaning |
| --- | --- | --- |
| `_FILE_NAME` | STRING | Manifest file name. |
| `_FILE_SIZE` | BIGINT | Manifest size in bytes. |
| `_NUM_ADDED_FILES` | BIGINT | Number of ADD entries. |
| `_NUM_DELETED_FILES` | BIGINT | Number of DELETE entries. |
| `_PARTITION_STATS` | SimpleStats | Minimums, maximums, and null counts for partition fields. |
| `_SCHEMA_ID` | BIGINT | Schema ID used when writing the manifest. |
| `_MIN_BUCKET`, `_MAX_BUCKET` | INT, nullable | Bucket bounds in the manifest. |
| `_MIN_LEVEL`, `_MAX_LEVEL` | INT, nullable | Data-file level bounds in the manifest. |
| `_MIN_ROW_ID`, `_MAX_ROW_ID` | BIGINT, nullable | Row-ID bounds when available. |
| `_EXTRA_FILES` | ARRAY of STRING, nullable | Names of additional files in the manifest directory; defaults to null. |

Each extra file belongs exclusively to one manifest. It is retained and cleaned up together with
that manifest during snapshot, tag, or changelog deletion.

### Manifest Sidecar

With `manifest.sidecar.write` enabled, a manifest writer can create a binary
`<manifest-file-name>.avro.sidecar` sidecar. Its name is stored in the manifest-list
record's `_EXTRA_FILES`; the existing Avro schemas and `_VERSION` identifiers are unchanged.
Readers identify the sidecar by the `.avro.sidecar` suffix among these explicit
references, not by probing for a derived file name. Other extra-file references are preserved.

With `manifest.sidecar.read` enabled and a partition, row-ID or bucket filter available, readers can use
the sidecar to select complete Avro blocks before reading manifest entries. Each option
inherits `manifest-sort.enabled` when unset; an explicit value overrides it independently.
Since manifest sorting defaults to `false`, sidecar reads and writes are also disabled when
none of these options is set. Old manifests, null or empty extra-file lists, and lists containing only
other extra-file types use the normal manifest read path. Missing sidecars and explicit container
validation failures, such as unsupported versions, checksum mismatches and byte-budget violations,
also fall back to that path. Each block's partition, row-ID and bucket
coverage is independently usable; an unavailable dimension cannot exclude a block.
Java falls back only on `IOException`. If the current thread is interrupted, it instead throws
`UncheckedIOException` with the original I/O failure. Other exceptions and errors propagate unchanged;
Java does not inspect causes or suppressed exceptions. PyPaimon explicitly propagates cancellation
and interruption exception types, including wrapped causes.

Writers choose partition, row-ID and bucket payloads through internal sidecar settings derived from
the table. Partition coverage is enabled when the supplied partition count is greater than zero;
otherwise the partition dictionary and partition payloads are omitted. Row-ID coverage follows
`data-evolution.enabled`. Bucket coverage is enabled when `bucket` is not `-1`, independently
of data evolution. A disabled payload uses encoding 0, with no length or payload bytes.
Each payload remains independently usable by readers. These settings do not add
table options or prevent readers from using payloads already present in existing sidecars.

Version 1 uses the following layout. Container integers and payload integers
are fixed-width big endian. Encoding IDs are unsigned bytes with separate namespaces.

```text
magic : 8 bytes                         // ASCII PAIMSCAR
formatVersion : int                    // 1
manifestNameHash : 32 bytes             // SHA-256 of the UTF-8 basename
manifestLength : long
manifestEntryCount : long               // ADD + DELETE
avroHeaderLength : int
avroHeader : bytes                      // original schema, codec and sync marker
partitionCount : int
partitionDictionary[]
  partitionByteLength : int
  partitionBytes : bytes                // existing manifest BinaryRow serialization
blockCount : int
blocks[]                               // original physical order
  offset : long
  length : long                         // complete encoded block, including sync marker
  recordCount : long
  partitionEncoding : byte
  if partitionEncoding != 0:
    partitionPayloadLength : int
    partitionPayload : bytes
  rowIdEncoding : byte
  if rowIdEncoding != 0:
    rowIdPayloadLength : int
    rowIdPayload : bytes
  bucketEncoding : byte
  if bucketEncoding != 0:
    bucketPayloadLength : int
    bucketPayload : bytes
checksum : 32 bytes                     // SHA-256 of all preceding bytes
```

The block ID is its position. Its first entry ordinal is the sum of preceding record
counts and is not stored. Each complete partition tuple appears once in the dictionary,
including all its fields and nulls. The scan's partition type interprets the existing
serialized tuple. Partition predicates are evaluated once per dictionary entry.

| Dimension | Encoding | Payload |
| --- | --- | --- |
| Any | `0` | Unavailable; only the encoding byte is present. |
| Partition | `1` | Positive `partitionIdCount: int` followed by sorted unique dictionary IDs (`int`). |
| Row ID | `1` | Positive `rangeCount: int` followed by sorted disjoint inclusive `(start: long, end: long)` pairs. Coverage may conservatively include gaps. |
| Bucket | `1` | Positive `pairCount: int` followed by sorted unique `(bucket: int, totalBuckets: int)` pairs. |
| Any | Other nonzero ID | Skip exactly the bounded payload length; treat only this dimension as unavailable. |

Only nonzero encodings are followed by a length and payload. Payload lengths exclude
the encoding and length fields, but include the count at the start of the payload.
For all three encoding-1 payloads below, `int` is a signed 4-byte integer and `long` is
a signed 8-byte integer, both big endian. Elements have no padding, per-element length
prefixes, or Avro variable-length integer encoding. Counts must be positive; encoding 0
represents unavailable coverage, rather than encoding 1 with a zero count.

#### Partition Payload

When `partitionEncoding == 1`, the block stores the IDs of all distinct partition tuples
represented by its entries:

```text
partitionPayload
  partitionIdCount : int               // N > 0
  partitionIds[N] : int                // N consecutive 4-byte dictionary IDs

partitionPayloadLength = 4 + 4 * N
```

An ID is the zero-based position of a complete tuple in the sidecar's shared
`partitionDictionary`, not an individual partition field or an entry ordinal. Valid IDs
satisfy `0 <= id < partitionCount` and are strictly increasing, with no duplicates.
The tuple bytes appear only in the dictionary; they are not repeated in each block's payload.
For example, IDs `[0, 3]` are stored as the three integers `[2, 0, 3]`, occupying 12 payload
bytes, or 17 bytes including `partitionEncoding` and `partitionPayloadLength`.

With a partition filter, the block matches if any referenced dictionary tuple matches.
A tuple containing a null partition value can still have a valid dictionary ID. If any
entry's partition tuple is unavailable, or partition coverage cannot fit its budget, the
block uses encoding 0 so that missing dictionary coverage cannot exclude it.

#### Row-ID Payload

When `rowIdEncoding == 1`, the block stores inclusive row-ID intervals:

```text
rowIdPayload
  rangeCount : int                     // N > 0
  ranges[N]
    start : long                       // inclusive first row ID
    end : long                         // inclusive last row ID

rowIdPayloadLength = 4 + 16 * N
```

Each pair satisfies `0 <= start <= end <= Long.MAX_VALUE`. Pairs are ordered by `start`
and do not overlap: each `start` is greater than the preceding `end`. The writer merges
overlapping and adjacent intervals contributed by the entries. An entry contributes
`[firstRowId, firstRowId + rowCount - 1]`; these are table row IDs, not manifest entry
ordinals. `rangeCount` counts intervals, not entries or individual row IDs.

There are no separate block min/max fields in this payload. The reader obtains the block
minimum from the first pair's `start` and the maximum from the last pair's `end`. It tests
this envelope first, then checks individual intervals if necessary. For example,
`[(10, 19), (30, 39)]` is stored as `rangeCount = 2` followed by four longs. Its payload
length is 36 bytes, or 41 bytes including the encoding and length fields. Its envelope
is `[10, 39]`, but a query for row ID 25 does not match either interval.

If the exact union exceeds its available budget, the writer can store one conservative
`[min,max]` pair using the same encoding. That payload has `rangeCount = 1` and length
20 bytes; it can include gaps. There is no separate flag distinguishing a coarsened pair
from an exact interval, so entry filtering remains necessary. Unknown or invalid row-ID
metadata makes coverage unavailable for the block; further byte-budget degradation can
also drop the payload entirely.

#### Bucket Payload

When `bucketEncoding == 1`, the block stores distinct bucket/count pairs:

```text
bucketPayload
  pairCount : int                      // N > 0
  pairs[N]
    bucket : int                       // entry's bucket number
    totalBuckets : int                 // entry's recorded total bucket count

bucketPayloadLength = 4 + 8 * N
```

Each pair satisfies `0 <= bucket < totalBuckets`. Pairs are sorted by `bucket`, then
`totalBuckets`, and deduplicated. `totalBuckets` comes from the entry's `_TOTAL_BUCKETS`;
it is not the number of buckets represented by this block or the table's current bucket
setting. The same bucket number can therefore occur with different totals after rescaling.
For example, `[(1, 4), (1, 8), (3, 4)]` is stored as the seven integers
`[3, 1, 4, 1, 8, 3, 4]`, occupying 28 payload bytes, or 33 bytes including the encoding
and length fields. These pairs have no partition IDs or separate bucket min/max fields.

Missing, invalid, negative/synthetic or over-budget bucket metadata makes that block's
bucket coverage unavailable (encoding 0, no length or payload). Partition and row-ID
coverage remain independently usable; no mutual-exclusion restriction is imposed.

Readers test bucket-only queries using the existing bucket-selection logic, including
the total-bucket count. Java uses conservative partition-independent bounds for
`ManifestBucketFilter`; arbitrary partition-dependent callbacks remain at the entry
filter stage. An unavailable bucket payload cannot exclude a block. Malformed payload lengths
or pair counts invalidate the container. Invalid ordering or values encountered while matching
also invalidate it; elements after the first match are skipped.

#### Validation and Coverage

Invalid lengths, known-payload framing, checksum mismatches or inconsistent physical
coverage invalidate the container. Invalid dictionary references or interval ordering
encountered in decoded payload contents also invalidate it. Byte spans must cover the
entire original manifest after its header; record counts must sum to the manifest entry
count. Readers validate the checksum, payload framing (including known count/length
consistency), and the complete block directory even when a block is rejected. Block
payload contents are decoded and validated only for dimensions still needed by the filters,
and only until that dimension matches. A row-ID min/max rejection skips individual
intervals; a match skips the remaining elements of that payload. Skipped payload contents
are not individually validated.

All entries contribute, including ADD, DELETE and every file format/column group.
Row-ID ranges are never expanded into individual values. If an exact union exceeds its
available byte budget, it becomes the inclusive `[min,max]` envelope with encoding 1. Processing
continues through the end of the block to extend those bounds and detect unknown row IDs.
An unknown or invalid row-ID range makes only that block's row-ID payload unavailable.
Partition budget exhaustion independently makes that block's partition payload unavailable.
The dictionary can consequently be incomplete for the manifest: a dictionary miss never
excludes a block with unavailable partition coverage. Later blocks can still use existing IDs.

`manifest.sidecar.max-bytes` bounds the whole serialized container, including the
partition dictionary and all three payload types. It accepts memory sizes such as
`16 mb` and, when unset, defaults to twice the configured `manifest.target-file-size`
(16 MiB with the default 8 MiB manifest target). An explicit sidecar size overrides this default.
The effective budget is capped at 2147483646 bytes to fit the in-memory byte-array representation.
Smaller budgets that cannot fit a sidecar skip this optimization; they do not prevent manifest writes
or reads. Doubling a very large manifest target saturates at the maximum representable memory size.
The Avro header and block directory share this byte budget without separate size or count limits.
Writers discard optional row-ID payloads, bucket payloads, then partition payloads/dictionary if necessary,
to fit the complete directory. If the directory itself cannot fit, no sidecar is published.
No emitted sidecar omits block descriptors. These are encoded-size bounds; Avro header parsing
and sidecar construction also incur object/buffer overhead. Query concurrency multiplies per-reader costs.

For conjunctive filters a block is retained only if each dimension is either unavailable
or matches. Within each block, matching tests row ID, partition, then bucket coverage.
It skips absent filters and short-circuits after a dimension rejects a block, skipping
the contents of later payloads. Within each payload, matching stops at the first hit. Matches in different dimensions can come
from different entries in the block, so entry filtering and deletion merging remain
necessary. Block min/max is derived from the first/last interval before testing the
individual intervals.

Readers still consume and validate the whole bounded sidecar. A partition-only query
therefore reads row-ID payload bytes too; payload lengths save decoding work for unused
payload contents and unknown encodings, not storage I/O. Selected compressed blocks are read by byte range with adjacent
spans coalesced. Existing immutable manifests are not backfilled by enabling the write option.

Java readers share the existing manifest cache for complete sidecar bytes, keyed by the
explicit sidecar path and subject to the same memory budget and single-file threshold.
Only successful reads and selections populate the cache. Each query creates independent
views and reapplies its filters and byte budget; query-specific selections are not cached.

Selected Avro blocks also share this cache. Each entry contains one complete compressed
block, keyed by the manifest's full path, original offset and encoded length, separately
from whole-manifest and sidecar entries. Different selections can reuse the same blocks.
Only successful complete reads populate the cache; oversized blocks stream through the
bounded read buffer. Adjacent uncached blocks are read together when they fit the read
buffer, then cached individually. Fully cached selections do not open the manifest file.
Block entries follow the existing memory budget, entry-size limit, expiration and eviction
settings. The Avro decoder and entry filters still run on cached bytes.

PyPaimon reuses `CachingFileIO` for sidecar bytes. Enable `local-cache.enabled` on the catalog
and include `meta` in `local-cache.whitelist` (included by default). Files ending in
`.avro.sidecar`, including custom names, use the same cache as other metadata. The cache
stores raw byte blocks by full path and block index, sharing `local-cache.max-size` and
`local-cache.block-size`. Without `local-cache.dir` it uses memory; setting that option
enables disk caching. Sidecar validation, filters and the read byte budget are reapplied
on every query. Local caching is disabled by default.

All Java sidecar selections use the block cache, including selections covering every block.
Selected reads never populate the full-manifest cache; that cache is used only without a
sidecar selection. PyPaimon explain scans disable sidecar pruning to preserve complete entry
counters.

Selected blocks still pass through entry filtering and ADD/DELETE reconciliation. Snapshot,
tag, changelog, orphan-file and failed-commit cleanup retain or remove the sidecar through
its extra-file reference together with the owning manifest.

## Manifest

Data manifests record **ADD** (`0`) and **DELETE** (`1`) entries. Readers reconcile these entries
in manifest order to determine the live files. A DELETE entry removes a file from the logical
state; it does not immediately delete the physical file, which older snapshots can still use.

For example, compaction can add file C and delete files A and B in the new snapshot. The old
snapshot still references A and B. See [snapshot expiration](../../maintenance/manage-snapshots#expire-snapshots)
for physical cleanup.

### Data Manifest

Data and changelog manifests use the same entry schema. Their snapshot references distinguish
which role they serve.

| Field | Type | Meaning |
| --- | --- | --- |
| `_KIND` | TINYINT | ADD (`0`) or DELETE (`1`). |
| `_PARTITION` | BYTES | Serialized BinaryRow containing the partition values. |
| `_BUCKET` | INT | Bucket containing the file. |
| `_TOTAL_BUCKETS` | INT | Bucket count recorded when the file was written, used for compatibility checks. |
| `_FILE` | DataFileMeta | Nested data file metadata described below. |

### Data File Metadata

The `_FILE` record includes file identity, statistics, and optional feature metadata.

| Field | Type | Meaning |
| --- | --- | --- |
| `_FILE_NAME` | STRING | Data or changelog file name. |
| `_FILE_SIZE` | BIGINT | File size in bytes. |
| `_ROW_COUNT` | BIGINT | Physical record count, including row kinds that represent deletions. |
| `_MIN_KEY`, `_MAX_KEY` | BYTES | Serialized BinaryRow key bounds, not SQL strings. |
| `_KEY_STATS`, `_VALUE_STATS` | SimpleStats | Statistics for key and value fields. |
| `_MIN_SEQUENCE_NUMBER`, `_MAX_SEQUENCE_NUMBER` | BIGINT | Sequence-number bounds. |
| `_SCHEMA_ID` | BIGINT | Schema ID used to write this file. |
| `_LEVEL` | INT | File level in the LSM layout. |
| `_EXTRA_FILES` | ARRAY of STRING | Associated files, such as external per-file indexes. |
| `_CREATION_TIME` | TIMESTAMP(3), nullable | File creation time. |
| `_DELETE_ROW_COUNT` | BIGINT, nullable | Count of deletion records within `_ROW_COUNT`, when known. |
| `_EMBEDDED_FILE_INDEX` | BYTES, nullable | Per-file index bytes stored directly in metadata. |
| `_FILE_SOURCE` | TINYINT, nullable | Whether the file was generated by append or compaction. |
| `_VALUE_STATS_COLS` | ARRAY of STRING, nullable | Names of columns represented in value statistics. |
| `_EXTERNAL_PATH` | STRING, nullable | External file path when the file is outside the default location. |
| `_FIRST_ROW_ID` | BIGINT, nullable | First row ID for row-tracked file ranges. |
| `_WRITE_COLS` | ARRAY of STRING, nullable | Columns written in this file for data evolution. |
| `_WRITE_COLS_SEQUENCES` | ARRAY of BIGINT, nullable | Maximum sequence numbers per physical table field after data-evolution compaction, ordered by `_WRITE_COLS` when present. |

These counts describe physical files; they need not equal the logical table row count. See
[Snapshot record counts](./snapshot#interpreting-record-counts) and
[Data Evolution](../../multimodal-table/data-evolution) for examples.

### Index Manifest

An index manifest describes index files through these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `_KIND` | TINYINT | ADD (`0`) or DELETE (`1`). |
| `_PARTITION` | BYTES | Serialized partition BinaryRow. |
| `_BUCKET` | INT | Bucket associated with the index entry. |
| `_INDEX_TYPE` | STRING | Index type identifier, for example `HASH` or `DELETION_VECTORS`. |
| `_FILE_NAME` | STRING | Index file name. |
| `_FILE_SIZE` | BIGINT | Index file size in bytes. |
| `_ROW_COUNT` | BIGINT | Entry count recorded by the index implementation. |
| `_DELETIONS_VECTORS_RANGES` | ARRAY of DeletionVectorMeta, nullable | Mapping from data files to deletion vectors. |
| `_EXTERNAL_PATH` | STRING, nullable | External index file path. |
| `_GLOBAL_INDEX` | GlobalIndexMeta, nullable | Row-range and field metadata for a global index. |

`_DELETIONS_VECTORS_RANGES` is the serialized field name, including its historical spelling.

#### DeletionVectorMeta

| Field | Type | Meaning |
| --- | --- | --- |
| `f0` | STRING | Data file name. |
| `f1` | INT | Starting offset of the deletion vector in the index file. |
| `f2` | INT | Stored vector length. See [Deletion Vectors](./tableindex#deletion-vectors) for the 32-bit and 64-bit length conventions. |
| `_CARDINALITY` | BIGINT, nullable | Number of deleted rows. |

#### GlobalIndexMeta

| Field | Type | Meaning |
| --- | --- | --- |
| `_ROW_RANGE_START`, `_ROW_RANGE_END` | BIGINT | Inclusive row-ID range covered by the index. |
| `_INDEX_FIELD_ID` | INT | Field ID indexed by this file. |
| `_EXTRA_FIELD_IDS` | ARRAY of INT, nullable | Additional field IDs recorded by the index. |
| `_INDEX_META` | BYTES, nullable | Metadata specific to the index implementation. |
| `_SOURCE_META` | BYTES, nullable | Source metadata recorded by the index implementation. |

See [Table Index](./tableindex) for dynamic bucket indexes and deletion vectors, and the
[global index guide](../../primary-key-table/global-index) for query-index behavior.

## Appendix

### SimpleStats

SimpleStats is a nested record. Decoding its minimum and maximum values requires the corresponding
field types.

| Field | Type | Meaning |
| --- | --- | --- |
| `_MIN_VALUES` | BYTES | Serialized BinaryRow of minimum values. |
| `_MAX_VALUES` | BYTES | Serialized BinaryRow of maximum values. |
| `_NULL_COUNTS` | ARRAY of nullable BIGINT, nullable | Null counts for the corresponding columns. An individual count can be null when unknown. |

### BinaryRow

BinaryRow stores values in a fixed-length region and a variable-length region. The fixed region
contains a one-byte header, a null bitmap aligned to 8-byte words, and 8-byte field slots.
Slots hold fixed-width values or the information needed to locate larger variable-width values.
Some short variable-width values can be stored inline.

BinaryRow is used inside manifest metadata. It is distinct from the compact row encoding in the
[Row file format](./rowformat#row-serialization-format).
