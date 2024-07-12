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

1. `_VALUE_KIND`: row is deleted or added. Similar to RocksDB, each row of data can be deleted or added, which will be
   used for updating the primary key table.
2. `_SEQUENCE_NUMBER`: this number is used for comparison during updates, determining which data came first and which
   data came later.
3. `_KEY_` prefix to key columns, this is to avoid conflicts with columns of the table.

## Changelog File

Changelog file and Data file are exactly the same, it only takes effect on the primary key table. It is similar to the
Binlog in a database, recording changes to the data in the table.
