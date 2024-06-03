---
title: "Understand Files"
weight: 1
type: docs
aliases:
- /learn-paimon/understand-files.html
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

# Understand Files

This article is specifically designed to clarify
the impact that various file operations have on files.

This page provides concrete examples and practical tips for
effectively managing them. Furthermore, through an in-depth
exploration of operations such as commit and compact,
we aim to offer insights into the creation and updates of files.

## Prerequisite

Before delving further into this page, please ensure that you have read through the
following sections:

1. [Basic Concepts]({{< ref "concepts/basic-concepts" >}}),
2. [Primary Key Table]({{< ref "primary-key-table/overview" >}}) and
3. How to use Paimon in [Flink]({{< ref "flink" >}}).

## Understand File Operations

### Create Catalog

Start Flink SQL client via `./sql-client.sh` and execute the following
statements one by one to create a Paimon catalog.
```sql
CREATE CATALOG paimon WITH (
'type' = 'paimon',
'warehouse' = 'file:///tmp/paimon'
);

USE CATALOG paimon;
```

This will only create a directory at given path `file:///tmp/paimon`.

### Create Table

Execute the following create table statement will create a Paimon table with 3 fields:

```sql
CREATE TABLE T (
  id BIGINT,
  a INT,
  b STRING,
  dt STRING COMMENT 'timestamp string in format yyyyMMdd',
  PRIMARY KEY(id, dt) NOT ENFORCED
) PARTITIONED BY (dt);
```

This will create Paimon table `T` under the path `/tmp/paimon/default.db/T`,
with its schema stored in `/tmp/paimon/default.db/T/schema/schema-0`


### Insert Records Into Table

Run the following insert statement in Flink SQL:

```sql
INSERT INTO T VALUES (1, 10001, 'varchar00001', '20230501');
```

Once the Flink job is completed, the records are written to the Paimon table through a successful `commit`.
Users can verify the visibility of these records by executing the query `SELECT * FROM T` which will return a single row.
The commit process creates a snapshot located at the path `/tmp/paimon/default.db/T/snapshot/snapshot-1`.
The resulting file layout at snapshot-1 is as described below:

{{< img src="/img/file-operations-0.png">}}

The content of snapshot-1 contains metadata of the snapshot, such as manifest list and schema id:
```json
{
  "version" : 3,
  "id" : 1,
  "schemaId" : 0,
  "baseManifestList" : "manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-0",
  "deltaManifestList" : "manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-1",
  "changelogManifestList" : null,
  "commitUser" : "7d758485-981d-4b1a-a0c6-d34c3eb254bf",
  "commitIdentifier" : 9223372036854775807,
  "commitKind" : "APPEND",
  "timeMillis" : 1684155393354,
  "logOffsets" : { },
  "totalRecordCount" : 1,
  "deltaRecordCount" : 1,
  "changelogRecordCount" : 0,
  "watermark" : -9223372036854775808
}
```

Remind that a manifest list contains all changes of the snapshot, `baseManifestList` is the base
file upon which the changes in `deltaManifestList` is applied.
The first commit will result in 1 manifest file, and 2 manifest lists are
created (the file names might differ from those in your experiment):

```bash
./T/manifest:
manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-1	
manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-0
manifest-2b833ea4-d7dc-4de0-ae0d-ad76eced75cc-0
```
`manifest-2b833ea4-d7dc-4de0-ae0d-ad76eced75cc-0` is the manifest
file (manifest-1-0 in the above graph), which stores the information about the data files in the snapshot.

`manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-0` is the
baseManifestList (manifest-list-1-base in the above graph), which is effectively empty.

`manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-1` is the
deltaManifestList (manifest-list-1-delta in the above graph), which
contains a list of manifest entries that perform operations on data
files, which, in this case, is `manifest-1-0`.


Now let's insert a batch of records across different partitions and
see what happens. In Flink SQL, execute the following statement:

```sql
INSERT INTO T VALUES 
(2, 10002, 'varchar00002', '20230502'),
(3, 10003, 'varchar00003', '20230503'),
(4, 10004, 'varchar00004', '20230504'),
(5, 10005, 'varchar00005', '20230505'),
(6, 10006, 'varchar00006', '20230506'),
(7, 10007, 'varchar00007', '20230507'),
(8, 10008, 'varchar00008', '20230508'),
(9, 10009, 'varchar00009', '20230509'),
(10, 10010, 'varchar00010', '20230510');
```

The second `commit` takes place and executing `SELECT * FROM T` will return
10 rows. A new snapshot, namely `snapshot-2`, is created and gives us the
following physical file layout:
```bash
 % ls -1tR . 
./T:
dt=20230501
dt=20230502	
dt=20230503	
dt=20230504	
dt=20230505	
dt=20230506	
dt=20230507	
dt=20230508	
dt=20230509	
dt=20230510	
snapshot
schema
manifest

./T/snapshot:
LATEST
snapshot-2
EARLIEST
snapshot-1

./T/manifest:
manifest-list-9ac2-5e79-4978-a3bc-86c25f1a303f-1 # delta manifest list for snapshot-2
manifest-list-9ac2-5e79-4978-a3bc-86c25f1a303f-0 # base manifest list for snapshot-2	
manifest-f1267033-e246-4470-a54c-5c27fdbdd074-0	 # manifest file for snapshot-2

manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-1 # delta manifest list for snapshot-1 
manifest-list-4ccc-c07f-4090-958c-cfe3ce3889e5-0 # base manifest list for snapshot-1
manifest-2b833ea4-d7dc-4de0-ae0d-ad76eced75cc-0  # manifest file for snapshot-1

./T/dt=20230501/bucket-0:
data-b75b7381-7c8b-430f-b7e5-a204cb65843c-0.orc

...
# each partition has the data written to bucket-0
...

./T/schema:
schema-0
```
The new file layout as of snapshot-2 looks like
{{< img src="/img/file-operations-1.png">}}

### Delete Records From Table

Now let's delete records that meet the condition `dt>=20230503`.
In Flink SQL, execute the following statement:

{{< label Batch >}}
```sql
DELETE FROM T WHERE dt >= '20230503';
```
The third `commit` takes place and it gives us `snapshot-3`. Now, listing the files
under the table and your will find out no partition is dropped. Instead, a new data
file is created for partition `20230503` to `20230510`:

```bash
./T/dt=20230510/bucket-0:
data-b93f468c-b56f-4a93-adc4-b250b3aa3462-0.orc # newer data file created by the delete statement 
data-0fcacc70-a0cb-4976-8c88-73e92769a762-0.orc # older data file created by the insert statement
```

This make sense since we insert a record in the second commit (represented by
`+I[10, 10010, 'varchar00010', '20230510']`) and then delete
the record in the third commit. Executing `SELECT * FROM T` will return 2 rows, namely:
```
+I[1, 10001, 'varchar00001', '20230501']
+I[2, 10002, 'varchar00002', '20230502']
```

The new file layout as of snapshot-3 looks like
{{< img src="/img/file-operations-2.png">}}

Note that `manifest-3-0` contains 8 manifest entries of `ADD` operation type,
corresponding to 8 newly written data files.



### Compact Table

As you may have noticed, the number of small files will augment over successive
snapshots, which may lead to decreased read performance. Therefore, a full-compaction
is needed in order to reduce the number of small files.

Let's trigger the full-compaction now, and run a dedicated compaction job through `flink run`:

{{< label Batch >}}
```bash  
<FLINK_HOME>/bin/flink run \
    -D execution.runtime-mode=batch \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    [--partition <partition-name>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-dynamic-conf> [--table_conf <paimon-table-dynamic-conf>] ...]
```

an example would be (suppose you're already in Flink home)

```bash
./bin/flink run \
    ./lib/paimon-flink-action-{{< version >}}.jar \
    compact \
    --path file:///tmp/paimon/default.db/T
```

All current table files will be compacted and a new snapshot, namely `snapshot-4`, is
made and contains the following information:

```json
{
  "version" : 3,
  "id" : 4,
  "schemaId" : 0,
  "baseManifestList" : "manifest-list-9be16-82e7-4941-8b0a-7ce1c1d0fa6d-0",
  "deltaManifestList" : "manifest-list-9be16-82e7-4941-8b0a-7ce1c1d0fa6d-1",
  "changelogManifestList" : null,
  "commitUser" : "a3d951d5-aa0e-4071-a5d4-4c72a4233d48",
  "commitIdentifier" : 9223372036854775807,
  "commitKind" : "COMPACT",
  "timeMillis" : 1684163217960,
  "logOffsets" : { },
  "totalRecordCount" : 38,
  "deltaRecordCount" : 20,
  "changelogRecordCount" : 0,
  "watermark" : -9223372036854775808
}
```

The new file layout as of snapshot-4 looks like
{{< img src="/img/file-operations-3.png">}}

Note that `manifest-4-0` contains 20 manifest entries (18 `DELETE` operations and 2 `ADD` operations)
1. For partition `20230503` to `20230510`, two `DELETE` operations for two data files
2. For partition `20230501` to `20230502`, one `DELETE` operation and one `ADD` operation
   for the same data file.


### Alter Table
Execute the following statement to configure full-compaction:
```sql
ALTER TABLE T SET ('full-compaction.delta-commits' = '1');
```

It will create a new schema for Paimon table, namely `schema-1`, but no snapshot
has actually used this schema yet until the next commit.

### Expire Snapshots

Remind that the marked data files are not truly deleted until the snapshot expires and
no consumer depends on the snapshot. For more information, see [Expiring Snapshots]({{< ref "maintenance/manage-snapshots#expiring-snapshots" >}}).

During the process of snapshot expiration, the range of snapshots is initially determined, and then data files within these snapshots are marked for deletion.
A data file is `marked` for deletion only when there is a manifest entry of kind `DELETE` that references that specific data file.
This marking ensures that the file will not be utilized by subsequent snapshots and can be safely removed.


Let's say all 4 snapshots in the above diagram are about to expire. The expire process is as follows:

1. It first deletes all marked data files, and records any changed buckets.

2. It then deletes any changelog files and associated manifests.

3. Finally, it deletes the snapshots themselves and writes the earliest hint file.

If any directories are left empty after the deletion process, they will be deleted as well.


Let's say another snapshot, `snapshot-5` is created and snapshot expiration is triggered. `snapshot-1` to `snapshot-4` are  
to be deleted. For simplicity, we will only focus on files from previous snapshots, the final layout after snapshot
expiration looks like:

{{< img src="/img/file-operations-4.png">}}

As a result, partition `20230503` to `20230510` are physically deleted.

### Flink Stream Write

Finally, we will examine Flink Stream Write by utilizing the example
of CDC ingestion. This section will address the capturing and writing of
change data into Paimon, as well as the mechanisms behind asynchronous compact
and snapshot commit and expiration.

To begin, let's take a closer look at the CDC ingestion workflow and
the unique roles played by each component involved.

{{< img src="/img/cdc-ingestion-topology.png">}}

1. `MySQL CDC Source` uniformly reads snapshot and incremental data, with `SnapshotReader` reading snapshot data
   and `BinlogReader` reading incremental data, respectively.
2. `Paimon Sink` writes data into Paimon table in bucket level. The `CompactManager` within it will trigger compaction
   asynchronously.
3. `Committer Operator` is a singleton responsible for committing and expiring snapshots.

Next, we will go over end-to-end data flow.


{{< img src="/img/cdc-ingestion-source.png">}}

`MySQL Cdc Source` read snapshot and incremental data and emit them to downstream after normalization.

{{< img src="/img/cdc-ingestion-write.png">}}


`Paimon Sink` first buffers new records in a heap-based LSM tree, and flushes them to disk when
the memory buffer is full. Note that each data file written is a sorted run. At this point, no manifest file and snapshot
is created. Right before Flink checkpoint takes places, `Paimon Sink` will flush all buffered records and send committable message
to downstream, which is read and committed by `Committer Operator` during checkpoint.

{{< img src="/img/cdc-ingestion-commit.png">}}

During checkpoint, `Committer Operator` will create a new snapshot and associate it with manifest lists so that the snapshot  
contains information about all data files in the table.

{{< img src="/img/cdc-ingestion-compact.png">}}

At later point asynchronous compaction might take place, and the committable produced by `CompactManager` contains information
about previous files and merged files so that `Committer Operator` can construct corresponding manifest entries. In this case
`Committer Operator` might produce two snapshot during Flink checkpoint, one for data written (snapshot of kind `Append`) and the
other for compact (snapshot of kind `Compact`). If no data file is written during checkpoint interval, only snapshot of kind `Compact`
will be created. `Committer Operator` will check against snapshot expiration and perform
physical deletion of marked data files.

## Understand Small Files

Many users are concerned about small files, which can lead to:
1. Stability issue: Too many small files in HDFS, NameNode will be overstressed.
2. Cost issue: A small file in HDFS will temporarily use the size of a minimum of one Block, for example 128 MB.
3. Query efficiency: The efficiency of querying too many small files will be affected.

### Understand Checkpoints

Assuming you are using Flink Writer, each checkpoint generates 1-2 snapshots, and the checkpoint forces the files to be
generated on DFS, so the smaller the checkpoint interval the more small files will be generated.

1. So first thing is increase checkpoint interval.

By default, not only checkpoint will cause the file to be generated, but writer's memory (write-buffer-size) exhaustion
will also flush data to DFS and generate the corresponding file. You can enable `write-buffer-spillable` to generate
spilled files in writer to generate bigger files in DFS.

2. So second thing is increase `write-buffer-size` or enable `write-buffer-spillable`.

### Understand Snapshots

{{< img src="/img/file-operations-3.png">}}

Paimon maintains multiple versions of files, compaction and deletion of files are logical and do not actually
delete files. Files are only really deleted when Snapshot is expired, so the first way to reduce files is to
reduce the time it takes for snapshot to be expired. Flink writer will automatically expire snapshots.

See [Expire Snapshots]({{< ref "/maintenance/manage-snapshots#expire-snapshots" >}}).

### Understand Partitions and Buckets

Paimon files are organized in a layered style. The following image illustrates the file layout. Starting
from a snapshot file, Paimon readers can recursively access all records from the table.

{{< img src="/img/file-layout.png">}}

For example, the following table:

```sql
CREATE TABLE MyTable (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh) WITH (
    'bucket' = '10'
);
```

The table data will be physically sliced into different partitions, and different buckets inside, so if the overall
data volume is too small, there is at least one file in a single bucket, I suggest you configure a smaller number
of buckets, otherwise there will be quite a few small files as well.

### Understand LSM for Primary Table

LSM tree organizes files into several sorted runs. A sorted run consists of one or multiple data files and each data
file belongs to exactly one sorted run.

{{< img src="/img/sorted-runs.png">}}

By default, sorted runs number depends on `num-sorted-run.compaction-trigger`, see [Compaction for Primary Key Table]({{< ref "/maintenance/write-performance#compaction" >}}),
this means that there are at least 5 files in a bucket. If you want to reduce this number, you can keep fewer files, but write performance may suffer.

### Understand Files for Bucketed Append Table

By default, Append also does automatic compaction to reduce the number of small files.

However, for Bucketed Append table, it will only compact the files within the Bucket for sequential
purposes, which may keep more small files. See [Bucketed Append]({{< ref "append-table/bucketed-append" >}}).

### Understand Full-Compaction

Maybe you think the 5 files for the primary key table are actually okay, but the Append table (bucket)
may have 50 small files in a single bucket, which is very difficult to accept. Worse still, partitions that
are no longer active also keep so many small files.

Configure ‘full-compaction.delta-commits’ perform full-compaction periodically in Flink writing. And it can ensure
that partitions are full compacted before writing ends.
