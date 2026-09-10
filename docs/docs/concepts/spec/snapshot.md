---
title: "Snapshot"
sidebar_position: 3
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

# Snapshot

A snapshot describes a committed table state and the metadata needed to read it. Its data-file
inventory is resolved through [manifest lists and manifests](./manifest); the snapshot itself
does not contain the records.

## Snapshot ID and Publication

Snapshot IDs begin at `1`. The **`id`** identifies a snapshot; the **`version`** identifies the
snapshot JSON format, currently version `3`. These are independent values.

Writers publish a snapshot through the table's configured atomic commit mechanism. A write
transaction can produce separate append and compaction snapshots, and an empty commit can be
skipped. See [Concurrency Control](../concurrency-control) for publication and conflict handling.

## File Layout

For filesystem-managed snapshots, the default layout is:

```text
my_table/
└── snapshot/
    ├── EARLIEST
    ├── LATEST
    ├── snapshot-1
    ├── snapshot-2
    └── snapshot-3
```

`EARLIEST` and `LATEST` are lookup hints, not the snapshot contents. They can be stale; the
filesystem reader can fall back to discovering snapshot files. Catalog-managed snapshot lookup
uses the catalog's version-management APIs. Snapshot expiration can remove older snapshots, so
a table's retained history need not start at ID `1`.

## JSON Fields

The fields are grouped by purpose below. Optional fields can be absent in older snapshots or
when the corresponding feature was not used.

### Identity and Commit

| Field | Type | Meaning |
| --- | --- | --- |
| `version` | Integer | Snapshot JSON format version, currently `3`. |
| `id` | Long | Snapshot ID. |
| `uuid` | String, optional | UUID identifying the immutable snapshot; absent in older snapshots. |
| `schemaId` | Long | Schema ID associated with this snapshot. Data files also carry their own schema IDs. |
| `commitUser` | String | Writer identity used for commit recovery and deduplication. |
| `commitIdentifier` | Long | Writer-supplied commit identifier; a write transaction can produce different commit kinds. |
| `commitKind` | String | `APPEND`, `COMPACT`, `OVERWRITE`, or `ANALYZE`. |
| `timeMillis` | Long | Commit time in milliseconds since the Unix epoch. |
| `writerVersion` | String, optional | Paimon version of the writer that created the snapshot. |
| `operation` | String, optional | Logical operation, such as `WRITE`, `DELETE`, `UPDATE`, or `MERGE`. |

`commitKind` describes the physical commit category. `operation`, when present, records the
logical operation that produced it; the two fields are not interchangeable.

### Manifest References

| Field | Type | Meaning |
| --- | --- | --- |
| `baseManifestList` | String | Manifest list carrying the data-file state inherited from earlier commits. |
| `deltaManifestList` | String | Manifest list carrying the data-file changes of this snapshot. |
| `baseManifestListSize` | Long, optional | Base manifest-list size in bytes. |
| `deltaManifestListSize` | Long, optional | Delta manifest-list size in bytes. |
| `changelogManifestList` | String, optional | Manifest list for changelog files produced by this snapshot. |
| `changelogManifestListSize` | Long, optional | Changelog manifest-list size in bytes. |
| `indexManifest` | String, optional | Manifest describing the snapshot's table index files. |

Combining the base and delta manifests identifies the snapshot's live data files. A reader does
not need to replay every earlier snapshot to obtain that file set. Changelog and index
references serve separate purposes; see the [file relationship diagram](./#metadata-and-file-relationships).

### Counts and Additional Metadata

| Field | Type | Meaning |
| --- | --- | --- |
| `totalRecordCount` | Long | Unmerged record count across all live data files. |
| `deltaRecordCount` | Long | Net change in unmerged records from added and deleted data files. |
| `changelogRecordCount` | Long, optional | Number of records in this snapshot's changelog files. |
| `watermark` | Long, optional | Input watermark. It can be absent when neither the commit nor earlier state supplies one. |
| `statistics` | String, optional | Table statistics file name. |
| `properties` | Map of strings to strings, optional | Additional snapshot properties. |
| `nextRowId` | Long, optional | Next row ID used by row tracking. |

## Interpreting Record Counts

The record counts are calculated per data file and are not logical row counts. For example, a
Dedicated Format table stores the regular columns and each dedicated BLOB column in separate data
files. Appending `N` logical rows to a table with one dedicated BLOB column therefore increases the
`deltaRecordCount` by `2 * N`. Use `COUNT(*)` when you need the logical row count.

## Inspect and Retain Snapshots

Use the [snapshots system table](../system-tables#snapshots-table) to inspect commit history with
SQL. See [Manage Snapshots](../../maintenance/manage-snapshots) for expiration and rollback, and
[Manage Tags](../../maintenance/manage-tags) to retain selected versions.
