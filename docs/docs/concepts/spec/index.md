---
title: "Storage Specification"
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

<a id="spec-overview"></a>

# Storage Specification

This section describes the files and metadata that make up a Paimon table. Use it when inspecting
storage or implementing a reader, writer, or integration. For an introduction to the concepts,
start with [Basic Concepts](../basic-concepts).

<a id="terms"></a>

## Metadata and File Relationships

A snapshot is the entry point to a committed table state. Its base and delta manifest lists
identify the manifests needed to resolve live data files. Optional references describe changelog
files and table indexes.

[![Snapshot metadata links to schema, data and changelog manifests, and an index manifest; manifests identify the files used by the table.](/img/concepts-file-layout.svg)](/img/concepts-file-layout.svg)

| Layer | Reference | What it describes |
| --- | --- | --- |
| Table definition | [Schema](./schema) | Fields, field IDs, partition keys, primary keys, and options |
| Committed state | [Snapshot](./snapshot) | Schema and manifest references, commit information, and record counts |
| File inventory | [Manifest](./manifest) | File additions and deletions, partition and file statistics, and index metadata |
| Data organization | [Data Files](./datafile) | Partition and bucket paths, primary-key records, and changelog files |
| Physical encoding | [File Format](./fileformat), [Row Format](./rowformat) | Format-specific type mappings and the Paimon row-format binary layout |
| Table indexes | [Table Index](./tableindex) | Dynamic bucket indexes and deletion vectors |
| Per-file indexes | [File Index](./fileindex) | Index headers and encodings for column indexes within a data file |

Table indexes and per-file indexes have different roles and metadata. See the
[global index guide](../../primary-key-table/global-index) for global query-index behavior and
its compatibility requirements.

## Example Table Directory

The following Flink SQL creates a primary-key table with one fixed bucket and Parquet data files:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '/your/path'
);
USE CATALOG my_catalog;

CREATE TABLE my_table (
    k INT PRIMARY KEY NOT ENFORCED,
    f0 INT,
    f1 STRING
) WITH (
    'bucket' = '1',
    'file.format' = 'parquet'
);

INSERT INTO my_table VALUES (1, 11, '111');
```

A simplified directory after the first commit looks like this. UUIDs and file counters are
abbreviated; the exact number of manifests depends on the write.

```text
warehouse/
└── default.db/
    └── my_table/
        ├── bucket-0/
        │   └── data-<uuid>-0.parquet
        ├── manifest/
        │   ├── manifest-<uuid>-0
        │   ├── manifest-list-<uuid>-0
        │   └── manifest-list-<uuid>-1
        ├── schema/
        │   └── schema-0
        └── snapshot/
            ├── EARLIEST
            ├── LATEST
            └── snapshot-1
```

Partitioned tables add partition directories above the bucket directories. Features such as
dynamic buckets and deletion vectors add index files under `index/` and an index manifest under
`manifest/`. External data paths can place data files outside the table directory.

`EARLIEST` and `LATEST` are snapshot lookup hints. The selected snapshot's metadata determines
which files to read; readers must not treat all files present in a directory as live table data.
