---
title: "Querying Tables"
weight: 5
type: docs
aliases:
- /how-to/querying-tables.html
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

# Querying Tables

Just like all other tables, Paimon tables can be queried with `SELECT` statement.

## Scan Mode

By specifying the `scan.mode` table property, users can specify where and how Paimon sources should produce records.

<table class="table table-bordered">
<thead>
<tr>
<th>Scan Mode</th>
<th>Batch Source Behavior</th>
<th>Streaming Source Behavior</th>
</tr>
</thead>
<tbody>
<tr>
<td>default</td>
<td colspan="2">
The default scan mode. Determines actual scan mode according to other table properties. If "scan.timestamp-millis" is set the actual scan mode will be "from-timestamp", and if "scan.snapshot-id" is set the actual startup mode will be "from-snapshot". Otherwise the actual scan mode will be "latest-full".
</td>
</tr>
<tr>
<td>latest-full</td>
<td>
Produces the latest snapshot of table.
</td>
<td>
Produces the latest snapshot on the table upon first startup, and continues to read the following changes.
</td>
</tr>
<tr>
<td>compacted-full</td>
<td>
Produces the snapshot after the latest <a href="{{< ref "concepts/file-layouts#compaction" >}}">compaction</a>.
</td>
<td>
Produces the snapshot after the latest compaction on the table upon first startup, and continues to read the following changes.
</td>
</tr>
<tr>
<td>latest</td>
<td>Same as "latest-full"</td>
<td>Continuously reads latest changes without producing a snapshot at the beginning.</td>
</tr>
<tr>
<td>from-timestamp</td>
<td>Produces a snapshot earlier than or equals to the timestamp specified by "scan.timestamp-millis".</td>
<td>Continuously reads changes starting from timestamp specified by "scan.timestamp-millis", without producing a snapshot at the beginning.</td>
</tr>
<tr>
<td>from-snapshot</td>
<td>Produces a snapshot specified by "scan.snapshot-id".</td>
<td>Continuously reads changes starting from a snapshot specified by "scan.snapshot-id", without producing a snapshot at the beginning.</td>
</tr>
<tr>
<td>from-snapshot-full</td>
<td>Produces a snapshot specified by "scan.snapshot-id".</td>
<td>Produces from snapshot specified by "scan.snapshot-id" on the table upon first startup, and continuously reads changes.</td>
</tr>
</tbody>
</table>

Users can also adjust `changelog-producer` table property to specify the pattern of produced changes. See [changelog producer]({{< ref "concepts/primary-key-table#changelog-producers" >}}) for details.

{{< img src="/img/scan-mode.png">}}

## Time Travel

Currently, Paimon supports time travel for Flink and Spark 3 (requires Spark 3.3+).

{{< tabs "time-travel-example" >}}

{{< tab "Flink" >}}
****
you can use [dynamic table options](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/hints/#dynamic-table-options) to specify scan mode and from where to start:

```sql
-- travel to snapshot with id 1L with 'scan.mode'='from-snapshot' by default
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- travel to snapshot with id 1L with 'scan.mode'='from-snapshot-full'
SELECT * FROM t /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '1') */;

-- travel to specified timestamp with a long value in milliseconds
SELECT * FROM t /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */;
```
{{< /tab >}}

{{< tab "Spark3" >}}

you can use `VERSION AS OF` and `TIMESTAMP AS OF` in query to do time travel:

```sql
-- travel to snapshot with id 1L (use snapshot id as version)
SELECT * FROM t VERSION AS OF 1;

-- travel to specified timestamp 
SELECT * FROM t TIMESTAMP AS OF '2023-06-01 00:00:00.123';

-- you can also use a long value in seconds as timestamp
SELECT * FROM t TIMESTAMP AS OF 1678883047;
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
-- travel to specified timestamp with a long value in milliseconds
SET SESSION paimon.scan_timestamp_millis=1679486589444;
SELECT * FROM t;
```

{{< /tab >}}

{{< /tabs >}}

## Incremental

Currently, Paimon supports read data incrementally for Flink and Spark.

{{< tabs "incremental-example" >}}

{{< tab "Flink" >}}
```sql
-- Read incremental changes between start snapshot (exclusive) and end snapshot
-- For example, '12,20' means changes between snapshot 12 and snapshot 20.
SELECT * FROM t /*+ OPTIONS('incremental-between' = '12,20') */;
```
{{< /tab >}}

{{< tab "Spark" >}}

```java
// Read incremental changes between start snapshot (exclusive) and end snapshot
// For example, '12,20' means changes between snapshot 12 and snapshot 20.
spark.read()
  .format("paimon")
  .option("incremental-between", "12,20")
  .load("path/to/table")
```

{{< /tab >}}

{{< /tabs >}}

## Consumer ID

{{< hint info >}}
This is an experimental feature.
{{< /hint >}}

You can specify the `consumer-id` when streaming read table:
```sql
SELECT * FROM t /*+ OPTIONS('consumer-id' = 'myid') */;
```

When stream read Paimon tables, the next snapshot id to be recorded into the file system. This has several advantages:

1. When previous job is stopped, the newly started job can continue to consume from the previous progress without
   resuming from the state. The newly reading will start reading from next snapshot id found in consumer files.
2. When deciding whether a snapshot has expired, Paimon looks at all the consumers of the table in the file system,
   and if there are consumers that still depend on this snapshot, then this snapshot will not be deleted by expiration.
3. When there is no watermark definition, the Paimon table will pass the watermark in the snapshot to the downstream
   Paimon table, which means you can track the progress of the watermark for the entire pipeline.

{{< hint info >}}
NOTE: The consumer will prevent expiration of the snapshot. You can specify 'consumer.expiration-time' to manage the 
lifetime of consumers.
{{< /hint >}}

## System Tables

System tables contain metadata and information about each table, such as the snapshots created and the options in use. Users can access system tables with batch queries.

Currently, Flink, Spark and Trino supports querying system tables.

In some cases, the table name needs to be enclosed with back quotes to avoid syntax parsing conflicts, for example triple access mode:
```sql
SELECT * FROM my_catalog.my_db.`MyTable$snapshots`;
```

### Snapshots Table

You can query the snapshot history information of the table through snapshots table, including the record count occurred in the snapshot.

```sql
SELECT * FROM MyTable$snapshots;

/*
+--------------+------------+-----------------+-------------------+--------------+-------------------------+---------------------+---------------------+-------------------------+
|  snapshot_id |  schema_id |     commit_user | commit_identifier |  commit_kind |             commit_time |  total_record_count |  delta_record_count |  changelog_record_count |
+--------------+------------+-----------------+-------------------+--------------+-------------------------+---------------------+---------------------+-------------------------+
|            2 |          0 | 7ca4cd28-98e... |                 2 |       APPEND | 2022-10-26 11:44:15.600 |                   2 |                   2 |                       0 |
|            1 |          0 | 870062aa-3e9... |                 1 |       APPEND | 2022-10-26 11:44:15.148 |                   1 |                   1 |                       0 |
+--------------+------------+-----------------+-------------------+--------------+-------------------------+---------------------+---------------------+-------------------------+
2 rows in set
*/
```

By querying the snapshots table, you can know the commit and expiration information about that table and time travel through the data.

### Schemas Table

You can query the historical schemas of the table through schemas table.

```sql
SELECT * FROM MyTable$schemas;

/*
+-----------+--------------------------------+----------------+--------------+---------+---------+
| schema_id |                         fields | partition_keys | primary_keys | options | comment |
+-----------+--------------------------------+----------------+--------------+---------+---------+
|         0 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
|         1 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
|         2 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
+-----------+--------------------------------+----------------+--------------+---------+---------+
3 rows in set
*/
```

You can join the snapshots table and schemas table to get the fields of given snapshots.

```sql
SELECT s.snapshot_id, t.schema_id, t.fields 
    FROM MyTable$snapshots s JOIN MyTable$schemas t 
    ON s.schema_id=t.schema_id where s.snapshot_id=100;
```

### Options Table

You can query the table's option information which is specified from the DDL through options table. The options not shown will be the default value. You can take reference to  [Configuration].

```sql
SELECT * FROM MyTable$options;

/*
+------------------------+--------------------+
|         key            |        value       |
+------------------------+--------------------+
| snapshot.time-retained |         5 h        |
+------------------------+--------------------+
1 rows in set
*/
```

### Audit log Table

If you need to audit the changelog of the table, you can use the `audit_log` system table. Through `audit_log` table, you can get the `rowkind` column when you get the incremental data of the table. You can use this column for
filtering and other operations to complete the audit.

There are four values for `rowkind`:

- `+I`: Insertion operation.
- `-U`: Update operation with the previous content of the updated row.
- `+U`: Update operation with new content of the updated row.
- `-D`: Deletion operation.

```sql
SELECT * FROM MyTable$audit_log;

/*
+------------------+-----------------+-----------------+
|     rowkind      |     column_0    |     column_1    |
+------------------+-----------------+-----------------+
|        +I        |      ...        |      ...        |
+------------------+-----------------+-----------------+
|        -U        |      ...        |      ...        |
+------------------+-----------------+-----------------+
|        +U        |      ...        |      ...        |
+------------------+-----------------+-----------------+
3 rows in set
*/
```

### Files Table
You can query the files of the table with specific snapshot.

```
-- Query the files of latest snapshot
SELECT * FROM MyTable$files;
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
| partition | bucket |                      file_path | file_format | schema_id | level | record_count | file_size_in_bytes | min_key | max_key |      null_value_counts |         min_value_stats |         max_value_stats |         creation_time |
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
|       [3] |      0 | data-8f64af95-29cc-4342-adc... |         orc |         0 |     0 |            1 |                593 |     [c] |     [c] | {cnt=0, val=0, word=0} | {cnt=3, val=33, word=c} | {cnt=3, val=33, word=c} |2023-02-24T16:06:21.166|
|       [2] |      0 | data-8b369068-0d37-4011-aa5... |         orc |         0 |     0 |            1 |                593 |     [b] |     [b] | {cnt=0, val=0, word=0} | {cnt=2, val=22, word=b} | {cnt=2, val=22, word=b} |2023-02-24T16:06:21.166|
|       [2] |      0 | data-83aa7973-060b-40b6-8c8... |         orc |         0 |     0 |            1 |                605 |     [d] |     [d] | {cnt=0, val=0, word=0} | {cnt=2, val=32, word=d} | {cnt=2, val=32, word=d} |2023-02-24T16:06:21.166|
|       [5] |      0 | data-3d304f4a-bcea-44dc-a13... |         orc |         0 |     0 |            1 |                593 |     [c] |     [c] | {cnt=0, val=0, word=0} | {cnt=5, val=51, word=c} | {cnt=5, val=51, word=c} |2023-02-24T16:06:21.166|
|       [1] |      0 | data-10abb5bc-0170-43ae-b6a... |         orc |         0 |     0 |            1 |                595 |     [a] |     [a] | {cnt=0, val=0, word=0} | {cnt=1, val=11, word=a} | {cnt=1, val=11, word=a} |2023-02-24T16:06:21.166|
|       [4] |      0 | data-2c9b7095-65b7-4013-a7a... |         orc |         0 |     0 |            1 |                593 |     [a] |     [a] | {cnt=0, val=0, word=0} | {cnt=4, val=12, word=a} | {cnt=4, val=12, word=a} |2023-02-24T16:06:21.166|
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
6 rows in set

-- You can also query the files with specific snapshot
SELECT * FROM MyTable$files /*+ OPTIONS('scan.snapshot-id'='1') */;
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
| partition | bucket |                      file_path | file_format | schema_id | level | record_count | file_size_in_bytes | min_key | max_key |      null_value_counts |         min_value_stats |         max_value_stats |         creation_time |
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
|       [3] |      0 | data-8f64af95-29cc-4342-adc... |         orc |         0 |     0 |            1 |                593 |     [c] |     [c] | {cnt=0, val=0, word=0} | {cnt=3, val=33, word=c} | {cnt=3, val=33, word=c} |2023-02-24T16:06:21.166|
|       [2] |      0 | data-8b369068-0d37-4011-aa5... |         orc |         0 |     0 |            1 |                593 |     [b] |     [b] | {cnt=0, val=0, word=0} | {cnt=2, val=22, word=b} | {cnt=2, val=22, word=b} |2023-02-24T16:06:21.166|
|       [1] |      0 | data-10abb5bc-0170-43ae-b6a... |         orc |         0 |     0 |            1 |                595 |     [a] |     [a] | {cnt=0, val=0, word=0} | {cnt=1, val=11, word=a} | {cnt=1, val=11, word=a} |2023-02-24T16:06:21.166|
+-----------+--------+--------------------------------+-------------+-----------+-------+--------------+--------------------+---------+---------+------------------------+-------------------------+-------------------------+-----------------------+
3 rows in set
```