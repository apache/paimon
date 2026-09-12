---
title: "Data Files"
sidebar_position: 5
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

<a id="datafile"></a>

# Data Files

This page describes the default partition and bucket layout, followed by the records stored in
data and changelog files. For the metadata that identifies live files, see [Manifest](./manifest).
For physical encodings and type mappings, see [File Format](./fileformat).

## Partition

Consider a Partition table via Flink SQL:

```sql
CREATE TABLE part_t (
    f0 INT,
    f1 STRING,
    dt STRING
) PARTITIONED BY (dt) WITH ('file.format' = 'parquet');

INSERT INTO part_t VALUES (1, '11', '20240514');
```

A simplified directory after the write is shown below. File identifiers are illustrative:

```shell
part_t
├── dt=20240514
│   └── bucket-0
│       └── data-ca1c3c38-dc8d-4533-949b-82e195b41bd4-0.parquet
├── manifest
│   ├── manifest-08995fe5-c2ac-4f54-9a5f-d3af1fcde41d-0
│   ├── manifest-list-51c16f7b-421c-4bc0-80a0-17677f343358-0
│   └── manifest-list-51c16f7b-421c-4bc0-80a0-17677f343358-1
├── schema
│   └── schema-0
└── snapshot
    ├── EARLIEST
    ├── LATEST
    └── snapshot-1
```

Paimon adopts the same partitioning concept as Apache Hive to separate data. The files of the partition will be placed
in a separate partition directory.

## Bucket

In the default layout, data files are stored in bucket directories beneath their partitions.
External data paths can place them outside the table directory. Common bucket configurations are
listed below; see [Data Distribution](../../primary-key-table/data-distribution) for all
primary-key bucket modes and their constraints.

| Table | `bucket` | Layout and assignment |
| --- | --- | --- |
| Primary-key | `-1` (default) | Dynamic buckets; the index records key-hash assignments to buckets. |
| Primary-key | Positive count | Fixed buckets; records are hashed by the configured bucket key. |
| Append | `-1` (default) | Bucket-unaware writes use `bucket-0` without a fixed writer-parallelism limit. |
| Append | Positive count | Fixed buckets with an explicitly configured `bucket-key`. |

For primary-key tables, the default bucket key is the primary key with partition fields removed.
The logical bucket assignment is separate from any configured external file location.

## Data File

Data files normally use names of the form `data-${uuid}-${id}.${format}`. The file's schema
depends on the table mode and enabled features. The examples below use ordinary table layouts;
[row tracking](../../append-table/row-tracking) and
[data evolution](../../multimodal-table/data-evolution) can add metadata or split columns across files.

### Table with Primary key Data File

For the standard primary-key layout, fields appear in this order:

| Position | Fields | Purpose |
| --- | --- | --- |
| 1 | Key fields with the `_KEY_` prefix | Store the merge key separately from the values; omitted in thin mode. |
| 2 | `_SEQUENCE_NUMBER` (BIGINT) | Orders record versions during merging. |
| 3 | `_VALUE_KIND` (TINYINT) | Encodes the record's row kind. |
| 4 | Value fields | Store the table's declared columns. |

For example:

```sql
CREATE TABLE T (
    a INT PRIMARY KEY NOT ENFORCED,
    b INT,
    c INT
);
```

With the default `data-file.thin-mode = false`, the file contains six columns:

```text
_KEY_a | _SEQUENCE_NUMBER | _VALUE_KIND | a | b | c
```

With `data-file.thin-mode = true`, readers obtain the key from the value fields and the file
contains five columns:

```text
_SEQUENCE_NUMBER | _VALUE_KIND | a | b | c
```

### Table w/o Primary key Data File

An ordinary append table stores its declared value columns without the primary-key merge fields:

```sql
CREATE TABLE T (
    a INT,
    b INT,
    c INT
);
```

Its file contains `a`, `b`, and `c`. Row-tracking tables can also store `_ROW_ID` and
`_SEQUENCE_NUMBER`; see [Row Tracking](../../append-table/row-tracking) for physical fields and
metadata-based fallback values.

## Changelog File

For primary-key tables, changelog files use the key/value record structure to represent changes
for incremental readers. The configured [changelog producer](../../primary-key-table/changelog-producer)
determines which changes are emitted. A changelog file's role is identified by the snapshot's
changelog manifest references, separately from the live data-file inventory.
