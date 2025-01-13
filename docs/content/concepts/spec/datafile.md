---
title: "DataFile"
weight: 5
type: docs
aliases:
- /concepts/spec/datafile.html
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

# DataFile

## Partition

Consider a Partition table via Flink SQL:

```sql
CREATE TABLE part_t (
    f0 INT,
    f1 STRING,
    dt STRING
) PARTITIONED BY (dt);

INSERT INTO part_t VALUES (1, '11', '20240514');
```

The file system will be:

```shell
part_t
├── dt=20240514
│   └── bucket-0
│       └── data-ca1c3c38-dc8d-4533-949b-82e195b41bd4-0.orc
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

The storage of all Paimon tables relies on buckets, and data files are stored in the bucket directory. The
relationship between various table types and buckets in Paimon:

1. Primary Key Table:
   1. bucket = -1: Default mode, the dynamic bucket mode records which bucket the key corresponds to through the index
      files. The index records the correspondence between the hash value of the primary-key and the bucket.
   2. bucket = 10: The data is distributed to the corresponding buckets according to the hash value of bucket key (
      default is primary key).
2. Append Table:
   1. bucket = -1: Default mode, ignoring bucket concept, although all data is written to bucket-0, the parallelism of
      reads and writes is unrestricted.
   2. bucket = 10: You need to define bucket-key too, the data is distributed to the corresponding buckets according to
      the hash value of bucket key.

## Data File

The name of data file is `data-${uuid}-${id}.${format}`. For the append table, the file stores the data of the table
without adding any new columns. But for the primary key table, each row of data stores additional system columns:

## Table with Primary key Data File

1. Primary key columns, `_KEY_` prefix to key columns, this is to avoid conflicts with columns of the table. It's optional,
   Paimon version 1.0 and above will retrieve the primary key fields from value_columns.
2. `_VALUE_KIND`: TINYINT, row is deleted or added. Similar to RocksDB, each row of data can be deleted or added, which will be
   used for updating the primary key table.
3. `_SEQUENCE_NUMBER`: BIGINT, this number is used for comparison during updates, determining which data came first and which
   data came later.
4. Value columns. All columns declared in the table.

For example, data file for table:

```sql
CREATE TABLE T (
    a INT PRIMARY KEY NOT ENFORCED,
    b INT,
    c INT
);
```

Its file has 6 columns: `_KEY_a`, `_VALUE_KIND`, `_SEQUENCE_NUMBER`, `a`, `b`, `c`.

When `data-file.thin-mode` enabled, its file has 5 columns: `_VALUE_KIND`, `_SEQUENCE_NUMBER`, `a`, `b`, `c`.

## Table w/o Primary key Data File

- Value columns. All columns declared in the table.

For example, data file for table:

```sql
CREATE TABLE T (
    a INT,
    b INT,
    c INT
);
```

Its file has 3 columns: `a`, `b`, `c`.

## Changelog File

Changelog file and Data file are exactly the same, it only takes effect on the primary key table. It is similar to the
Binlog in a database, recording changes to the data in the table.
