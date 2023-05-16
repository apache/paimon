---
title: "File Operations"
weight: 2
type: docs
aliases:
- /maintenance/read-performance.html
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
# File Operations

This article is specifically designed to clarify 
the impact that various file operations have on files. 
In order to make user better understand the origins of the 
multitude of small files that result from these operations, 
we will provide concrete examples and practical tips for 
effectively managing them. Furthermore, through an in-depth 
exploration of operations such as commit and compact, 
we aim to offer insights into the creation and updates of 
these small files.


## Prerequisite

Before delving further into this page, please ensure that you have read through the
following sections:

1. [Basic Concepts]({{< ref "concepts/basic-concepts" >}}), 
2. [File Layouts]({{< ref "concepts/file-layouts" >}}) and 
3. how to use Paimon in [Flink]({{< ref "engines/flink" >}}).

{{< img src="/img/file-layout.png">}}


## Create Paimon Catalog
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

## Create Paimon Table

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


## Insert Records Into Paimon Table

Run the following insert statement in Flink SQL:

```sql
INSERT INTO T VALUES (1, 10001, 'varchar00001', '20230501');
```

After the Flink job finishes, the records are written into Paimon table, which 
is done by a successful `commit`. The records are visible to user
as can be verified by `SELECT * FROM T` which return a single row. 
The commit creates a snapshot under path `/tmp/paimon/default.db/T/snapshot/snapshot-1`. 
The resulting file layout as of snapshot-1 is as follows:

{{< img src="/img/file-operations-0.png">}}

The content of snapshot-1 contains metadata of the snapshot, such as manifest list and schema id:
```json
{
  "version" : 3,
  "id" : 1,
  "schemaId" : 0,
  "baseManifestList" : "manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-0",
  "deltaManifestList" : "manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-1",
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
manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-1	
manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-0
manifest-2b833ea4-d7dc-4de0-ae0d-ad76eced75cc-0
```
`manifest-2b833ea4-d7dc-4de0-ae0d-ad76eced75cc-0` is the manifest 
file (manifest-1-0 in the above graph), which stores the information about the data files in the snapshot.

`manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-0` is the 
baseManifestList (manifest-list-1-base in the above graph), which is effectively empty.

`manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-1` is the 
deltaManifestList (manifest-list-1-delta in the above graph), which 
contains a list of manifest entries that perform operations on data 
files, which, in this case, is `manifest-B-0`.


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
 % ls -atR . 
T	.	..

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
manifest-96739ac2-5e79-4978-a3bc-86c25f1a303f-1	 # delta manifest list for snapshot-2
manifest-96739ac2-5e79-4978-a3bc-86c25f1a303f-0  # base manifest list for snapshot-2	
manifest-f1267033-e246-4470-a54c-5c27fdbdd074-0	 # manifest file for snapshot-2

manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-1	 # delta manifest list for snapshot-1 
manifest-09184ccc-c07f-4090-958c-cfe3ce3889e5-0  # base manifest list for snapshot-1
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

## Delete Records From Paimon Table

Now let's delete records that meet the condition `dt>=20230503`. 
In Flink SQL, execute the following statement:

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

## Alter Paimon Table
As you may have noticed, the number of small files will augment over successive 
snapshots, which may lead to decreased read performance. Therefore, a full-compaction 
is needed in order to reduce the number of small files.

Execute the following statement to configure full-compaction:
```sql
ALTER TABLE T SET ('full-compaction.delta-commits' = '1');
```

It will create a new schema for Paimon table, namely `schema-1`, but no snapshot 
has actually used this schema yet until the next commit.

This configuration will ensure that partitions are full compacted before writing 
ends, and since we haven't done any compaction yet, the next commit will produce 
two snapshots, one for data written and one for full-compaction. However, we will 
not use this configuration since Flink does not support running compaction in Flink SQL.

Let's trigger the full-compaction now. Make sure you have set execution mode to `batch` 
(add an entry `execution.runtime-mode: batch`  in `flink-conf.yaml`) and run a 
dedicated compaction job through `flink run`:

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    [--partition <partition-name>] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
```

an example would be (suppose you're already in Flink home)

```bash
./bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    ./lib/paimon-flink-1.17-0.5-SNAPSHOT.jar \
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
  "baseManifestList" : "manifest-aa28be16-82e7-4941-8b0a-7ce1c1d0fa6d-0",
  "deltaManifestList" : "manifest-aa28be16-82e7-4941-8b0a-7ce1c1d0fa6d-1",
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

## When Data Files are Deleted

Remind that the marked data files are not truly deleted until the snapshot expires and 
no consumer depends on the snapshot.
