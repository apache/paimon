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

`ManifestSidecar` provides a binary sidecar for selecting complete Avro manifest blocks
using independent partition, row-ID and bucket coverage. A sidecar uses the
`<manifest-file-name>.avro.sidecar` naming convention. Readers find it through an explicit
`.avro.sidecar` reference in the manifest metadata's `_EXTRA_FILES`, without probing a
derived file name. The Avro schemas and `_VERSION` identifiers remain unchanged.

The utility includes construction, validation, block selection and optional caching. Java table
writers generate sidecars when `manifest.sidecar.write` is enabled; when unset, it inherits
`manifest-sort.enabled`. Both ordinary writes and raw manifest rewrites build the sidecar from
the completed output manifest and publish its `_EXTRA_FILES` reference only after both files
close successfully. Failed writes and aborted writers clean up their own manifest/sidecar pairs.
Scans do not yet invoke sidecar pruning automatically. Callers remain responsible for applying
entry filters and reconciling ADD/DELETE entries after block selection. The low-level `build`
method returns sidecar bytes without writing or publishing another file.

Callers decide whether to invoke `build` and `read`; these utilities have no read/write switches.
`build` and `Builder` accept `rowIdEnabled` and `bucketEnabled` arguments for independent
payload generation. Partition generation is always enabled,
including the empty partition tuple for unpartitioned tables. Missing or invalid
metadata makes only the affected block's dimension unavailable. There is no sidecar byte budget:
construction keeps complete coverage and `read` consumes the entire file once it is opened.

`read` returns null for an absent sidecar reference or an `IOException`, allowing the caller
to fall back to the manifest. If the thread is interrupted, the I/O failure is propagated as
`UncheckedIOException`. Other exceptions and errors propagate unchanged. `select` validates
supplied bytes directly and reports invalid containers with `IOException`.

Version 1 uses the following layout. Counts, lengths, offsets and the version use canonical
nonnegative unsigned LEB128 varints. Counts and payload lengths are bounded by `Integer.MAX_VALUE`;
block offsets, lengths and record counts are bounded by `Long.MAX_VALUE`. Row-ID envelope
endpoints remain fixed-width, eight-byte big-endian longs. Encoding IDs are unsigned bytes
with separate namespaces. The existing serialized partition tuple bytes are unchanged.

```text
magic : 4 bytes                         // ASCII PMSC
formatVersion : varint                 // 1
avroHeaderLength : varint
avroHeader : bytes                      // original schema, codec and sync marker
partitionCount : varint
partitionDictionary[]
  partitionByteLength : varint
  partitionBytes : bytes                // existing manifest BinaryRow serialization
blockCount : varint
blocks[]                               // original physical order
  offset : varint
  length : varint                       // complete encoded block, including sync marker
  recordCount : varint
  partitionEncoding : byte
  if partitionEncoding != 0:
    partitionPayloadLength : varint
    partitionPayload : bytes
  rowIdEncoding : byte
  if rowIdEncoding != 0:
    rowIdPayloadLength : varint
    rowIdPayload : bytes
  bucketEncoding : byte
  if bucketEncoding != 0:
    bucketPayloadLength : varint
    bucketPayload : bytes
checksum : 4 bytes                      // big-endian CRC32 of all preceding bytes
```

The block ID is its position. Its first entry ordinal is the sum of preceding record counts
and is not stored. Each complete partition tuple appears once in the dictionary, including
all its fields and nulls. The scan's partition type interprets the existing serialized tuple.
Partition predicates are evaluated once per dictionary entry.

| Dimension | Encoding | Payload |
| --- | --- | --- |
| Any | `0` | Unavailable; only the encoding byte is present. |
| Partition | `1` | `intsDeltaPayload` of sorted unique dictionary IDs. |
| Row ID | `1` | Minimum, maximum, and `intsDeltaPayload` of sorted interior interval endpoints. |
| Bucket | `1` | Two paired `intsDeltaPayload` sequences: sorted bucket IDs and their recorded total bucket counts. |
| Any | Other nonzero ID | Skip the declared payload length; treat only this dimension as unavailable. |

Only nonzero encodings are followed by a length and payload. Payload lengths exclude the
encoding and length fields, but include the count and other fields within the payload.
Partition IDs and bucket pairs have positive counts no greater than the block's record count.
Row-ID coverage contains one or more intervals; its interior endpoint count can be zero for
a single interval. Encoding 0 represents unavailable coverage, not an empty known set.

#### Integer Delta Payload

Partition, row-ID and both bucket sequences share this structure:

```text
intsDeltaPayload
  count : varint
  deltas[count] : varint
```

The count is in `[0, Integer.MAX_VALUE]`. Nonnegative deltas use unsigned LEB128 varints,
occupying one to nine bytes for values from 0 through
`Long.MAX_VALUE`. Seven value bits are stored per byte, least significant group first; the
high bit indicates another byte follows.
Encodings use the shortest representation without padding.

A nondecreasing sequence is delta-encoded from a specified base. Each value contributes one
unsigned varint containing its difference from the preceding value. The first difference
is relative to the base:

```text
deltas[] : varint
value[0] = base + deltas[0]
value[i] = value[i - 1] + deltas[i]
```

The shared `DeltaVarintCodec` utility writes the count and then each delta immediately,
and reads values on demand using `VarLengthIntUtils`. A reader consumes exactly the declared
number of values, leaving any following sequence available in the buffer. Callers check
their enclosing payload boundaries. Counts, overflow and value bounds are checked without
materializing arrays. Reads may stop early.

Only the `totalBuckets` sequence uses signed differences, because totals need not increase
when pairs are sorted by bucket. Its differences use ZigZag before unsigned varint encoding:
`encoded = (delta << 1) ^ (delta >> 63)` and
`delta = (encoded >>> 1) ^ -(encoded & 1)`. Values are nonnegative ints, so encoded deltas
are at most `2 * Integer.MAX_VALUE` and require at most five bytes. The field defines this
signed mode; no additional mode byte is stored. Other sequences use nonnegative differences.

#### Partition Payload

When `partitionEncoding == 1`, the block stores IDs of all distinct partition tuples
represented by its entries:

```text
partitionPayload
  intsDeltaPayload                    // dictionary IDs, base = 0
```

An ID is the zero-based position of a complete tuple in the sidecar's shared dictionary.
IDs satisfy `0 <= id < partitionCount` and are strictly increasing. Tuple bytes appear only
in the dictionary and are not repeated in each block. For IDs `[0, 1, 2, 3, 4]`, the deltas
are `[0, 1, 1, 1, 1]`. The payload contains a one-byte count of 5 followed by these five
varint bytes: 6 bytes, or 8 bytes including the encoding and length fields.

With a partition filter, a block matches if any referenced tuple matches. A tuple containing
a null partition value still has a dictionary ID. Unpartitioned tables record the empty
tuple. If any entry's entire partition tuple is unavailable, the block uses encoding 0,
so a dictionary miss cannot exclude that block. Later blocks can still use existing IDs.

#### Row-ID Payload

The writer merges overlapping and adjacent inclusive intervals contributed by entries.
An entry contributes `[firstRowId, firstRowId + rowCount - 1]`. The resulting intervals are
sorted and disjoint; they are never expanded into individual row IDs or coarsened to include gaps.

```text
rowIdPayload
  minRowId : long                      // first interval's start
  maxRowId : long                      // last interval's inclusive end
  intsDeltaPayload                    // stores longs: 2 * (N - 1) sorted interior endpoints, base = minRowId
```

The envelope satisfies `0 <= minRowId <= maxRowId <= Long.MAX_VALUE`. Flatten the intervals
as `[start0, end0, start1, end1, ...]`. The first start is supplied by `minRowId`, and the last
end by `maxRowId`; only the remaining `2 * (N - 1)` interior endpoints are delta/varint encoded.
Their count must be even, and the derived interval count `N = count / 2 + 1` must not exceed
the block's record count. There is no separate stored interval count.
Pairing the reconstructed endpoints recovers the intervals. Each pair satisfies
`0 <= start <= end <= Long.MAX_VALUE`; each following start must exceed the preceding end.

For `[(10, 19), (30, 39)]`, the minimum is 10 and maximum is 39. The interior
endpoints `[19, 30]` have deltas `[9, 11]` from base 10, each encoded as one varint byte.
The payload starts with an eight-byte minimum of 10 and an eight-byte maximum of 39,
followed by `intsDeltaPayload` bytes `[2, 9, 11]`: 19 bytes, or 21 bytes with framing.
For a single interval, the two eight-byte endpoints and a one-byte zero count define the
interval: 17 payload bytes and no deltas.

The reader first tests the envelope without decoding any deltas. A query for row ID 25
passes the example's envelope check but matches neither interval. Unknown or invalid row-ID
metadata makes that block's row-ID payload unavailable; partition and bucket coverage remain usable.

#### Bucket Payload

When `bucketEncoding == 1`, the block stores distinct bucket/count pairs:

```text
bucketPayload
  buckets : intsDeltaPayload           // N > 0, base = 0, nonnegative deltas
  totalBuckets : intsDeltaPayload      // N values, base = 0, ZigZag signed deltas
```

Pairs are sorted first by bucket, then by total bucket count, with duplicates removed.
Each pair satisfies `0 <= bucket < totalBuckets <= Integer.MAX_VALUE`. Both sequences have
the same count, and values at the same position form one pair. The totals must not be sorted
independently. The same bucket may occur with different totals after rescaling.

For `[(1, 4), (1, 8), (3, 4)]`, bucket values `[1, 1, 3]` have deltas `[1, 0, 2]`.
Paired totals `[4, 8, 4]` have signed deltas `[4, 4, -4]`, ZigZag-encoded as `[8, 8, 7]`.
The two payloads are `[3, 1, 0, 2]` and `[3, 8, 8, 7]`: 8 bytes total, or 10 bytes with framing.

Missing, invalid or negative/synthetic bucket metadata makes the block's bucket coverage
unavailable. A caller can supply a predicate on `(bucket, totalBuckets)` which conservatively
retains every potentially matching pair. Filters requiring an entry's partition belong at
the entry-filtering stage; omit the bucket predicate if no safe check is available.

#### Validation and Reading

Readers validate the checksum, container fields, payload lengths and leading count headers,
and the complete physical block directory regardless of the query. Byte spans must cover
the whole original manifest after its header; record counts must sum to the manifest entry
count. Manifest length and entry count come from the supplied `ManifestFileMeta` rather
than being duplicated in the sidecar. Unknown nonzero encodings skip their declared bytes
without interpreting a count.

Compressed contents are decoded only for dimensions needed by the filters. A row-ID envelope
rejection skips all its deltas; a matching interval or partition ID skips remaining values.
Bucket filtering first walks the bucket sequence to locate the paired totals without
allocating arrays, then decodes pairs until a match. Unused totals may be skipped.
Invalid varints, value counts, overflows,
out-of-range values or ordering encountered while decoding invalidate the container. Delta
contents skipped by short-circuiting are not individually validated.

For conjunctive filters a block is retained only if every dimension is unavailable or matches.
Matching tests row ID, partition, then bucket coverage. Absent filters are skipped, and a
rejection skips the remaining dimensions. Matches in different dimensions can come from
different entries, so entry filtering and ADD/DELETE reconciliation remain necessary.

The entire sidecar is read in chunks of at most 1 MiB, including payloads unused by a query.
There is no size-based fallback or payload dropping. Payload lengths save decoding work,
not sidecar storage I/O. Selected compressed Avro blocks are read by byte range with adjacent
spans coalesced and individual read requests bounded to 4 MiB. Building a sidecar does not
modify the original manifest.

`read` and `openManifest` accept an optional caller-supplied `SegmentsCache<Object>`. Complete
sidecar bytes are keyed by their explicit `Path`. Only successful reads and selections
populate the cache; query-specific selections are not cached. Cache entry-size limits affect
admission only: larger sidecars are still fully read, validated and used.

Selected Avro blocks also share this cache. Each entry contains one complete compressed block,
keyed by the manifest's full path, original offset and encoded length, separately from whole-file
keys. Different selections reuse the same blocks. Only complete reads populate the cache;
oversized blocks stream through the read buffer. Adjacent uncached blocks fitting the buffer
are read together and cached individually. Fully cached selections do not open the manifest.
The cache retains its configured memory budget, entry-size limit, expiration and eviction policy.

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
