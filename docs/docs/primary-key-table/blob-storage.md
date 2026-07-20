---
title: "BLOB Storage"
sidebar_position: 11
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

# BLOB Storage

Primary-key tables can store top-level `BLOB` and `ARRAY<BLOB>` payloads in table-managed files. Unlike the positional
BLOB files used by append tables, managed BLOB payloads have stable descriptors. MergeTree sorting, deduplication, and
compaction can therefore reorder or remove rows without rewriting the surviving payload bytes.

This mode stores:

- a serialized `BlobDescriptor` for each scalar value or non-null array element;
- the payload in an immutable `.managed.blob` pack; and
- one `.blobref` sidecar for every data file, containing the exact managed packs referenced by that file.

For general BLOB concepts and read options, see [BLOB Storage](../multimodal-table/blob).

## Create a Table

Use `blob-field` to mark scalar or array fields whose payloads should be stored in managed BLOB files.
`blob-descriptor-field` and `blob-view-field` are inline forms: their serialized descriptor or view metadata stays in
the normal data file and is not materialized into a managed BLOB file.

The following example accepts both a scalar value and an ordered array of values:

```sql
CREATE TABLE media (
    id BIGINT,
    name STRING,
    content BYTES COMMENT '__BLOB_FIELD; media content',
    attachments ARRAY<BYTES> COMMENT '__BLOB_FIELD; related files',
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'none',
    'blob.target-file-size' = '128 mb'
);

INSERT INTO media VALUES
    (1, 'logo', X'89504E470D0A1A0A', ARRAY[X'25504446', NULL]);
```

For a primary-key table, every non-null `Blob` value in a `blob-field` is externalized before it enters the MergeTree
sort buffer. Its payload is copied into a new table-managed BLOB pack regardless of the value's backing representation.
Reads return the payload bytes by default; the existing `blob-as-descriptor` read option can expose descriptors instead.
A `blob-descriptor-field` is written inline to the normal data file and does not participate in managed storage or its
reference sidecars.

When descriptor-backed BLOBs are copied to another table, the target normally rebuilds a `FileIO` from its catalog
context. For a managed `blob-field`, this is a copy flow: the target writes the payload into its own BLOB storage and
does not retain the source descriptor. If the source table uses table-scoped credentials, configure
`blob-descriptor.source-table` on the target so that the source table's `FileIO` is used to materialize the payload:

```sql
ALTER TABLE media_copy SET TBLPROPERTIES (
    'blob-descriptor.source-table' = 'db.media$branch_rt'
);
```

Use `blob-descriptor-field` to retain literal descriptors, or `blob-view-field` to retain a logical, no-copy reference
to an upstream row. Other `blob-descriptor.*` filesystem options remain sufficient when the source storage can be
accessed with static configuration; `source-table` is for table-scoped `FileIO` credentials.

The source table must belong to the same catalog. A branch suffix is supported. Target tables without a catalog loader,
including external tables in REST catalogs, are not supported. When this option is set, it takes precedence over other
`blob-descriptor.*` options; remove it before switching back to descriptor-specific filesystem configuration.

`ARRAY<BLOB>` is externalized element by element. Every non-null `Blob` element is copied into managed storage, while
array order, a null array, and null elements are preserved. An empty array writes no payload. `ARRAY<BLOB>` uses
`blob-field`; `blob-descriptor-field` and `blob-view-field` remain scalar-only declarations.

`blob.target-file-size` controls when a writer rolls to a new managed payload pack. A pack can contain payloads from
multiple rows, and a row descriptor records its URI, offset, and length.

:::note

On append tables, `blob-descriptor-field` is descriptor-only storage and writes must provide a descriptor. Append-table
`blob-field` storage still requires row tracking and data evolution. The managed raw-byte externalization described here
is specific to supported primary-key tables.

:::

## Requirements and Limitations

Primary-key managed BLOB storage has the following requirements:

| Item | Requirement |
|------|-------------|
| Managed BLOB declaration | Scalar `BLOB` and `ARRAY<BLOB>` use `blob-field`; `blob-descriptor-field` remains inline |
| Merge engine | `deduplicate` only |
| Changelog producer | `none` only |
| Key usage | A managed BLOB column cannot be a primary, partition, bucket, or sequence key |
| External data paths | `data-file.external-paths` is not supported |
| PK clustering override | `pk-clustering-override` is not supported |

`row-tracking.enabled` and `data-evolution.enabled` are not required for this primary-key mode.

## Update, Delete, and Compaction

An update writes a new descriptor and managed payload when a scalar value or array element changes. A delete record does
not write a new payload. Deduplication determines the final rows of each data file, and its `.blobref` sidecar contains
only the managed packs referenced by those rows.

Compaction preserves descriptors for surviving values and creates new `.blobref` sidecars from the compacted output.
It does not copy the referenced payload bytes into new `.managed.blob` packs. This keeps ordinary compaction cost
proportional to row metadata instead of BLOB size.

The `.blobref` file is owned by its data file through `DataFileMeta.extraFiles`, so it follows the data file through
snapshot, tag, branch, rollback, expiration, and deletion lifecycles. Shared `.managed.blob` packs are deliberately not
extra files because more than one retained data file can reference the same pack.

## Garbage Collection

Garbage collection of unreferenced `.managed.blob` packs is not implemented yet. Updates, deletes, compaction, or an
ambiguous writer failure can therefore leave payload packs that are no longer reachable from current rows.

The ordinary orphan-file cleaner intentionally preserves all `.managed.blob` files. This fail-safe behavior prevents it
from deleting a payload that is still reachable from a snapshot, tag, branch, or another retained root, but it also
means unused BLOB storage can grow until a root-aware BLOB garbage collector is available.

A future collector must compute reachability across all retained roots and treat a missing, corrupt, or unsupported
`.blobref` sidecar as unsafe to delete. An empty, valid sidecar is different from a missing sidecar: it explicitly states
that the data file references no managed payload pack.

## Reference Metadata

The BLOB reference set is recorded in a data-file-owned `.blobref` sidecar rather than `IndexManifest`. BLOB
reachability is an exact dependency of one data file, and `DataFileMeta.extraFiles` already provides the required
lifecycle for such metadata. Using `IndexManifest` would require a new snapshot-level index lifecycle and would be a
larger compatibility change.

Each sidecar is immutable, versioned, checksummed, and deterministic. It stores only managed payload identities;
descriptor and view fields remain inline and are not added to the managed reference set. Managed pack identity is
derived from the descriptor URI and its reserved `.managed.blob` suffix, so a valid reference is retained even when the
pack and the data file are in different directories.
