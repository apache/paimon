---
title: "Append Table"
weight: 1
type: docs
aliases:
- /append-table/append-table.html
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

# Append Table

If a table does not have a primary key defined, it is an append table by default.

You can only insert a complete record into the table in streaming. This type of table is suitable for use cases that
do not require streaming updates (such as log data synchronization).

{{< tabs "create-append-table" >}}
{{< tab "Flink" >}}
```sql
CREATE TABLE my_table (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    'file.compression' = 'zstd'
);
```
{{< /tab >}}
{{< /tabs >}}

The recommended compression for the Append table is `'zstd'`.

## Automatic small file merging

In streaming writing job, without bucket definition, there is no compaction in writer, instead, will use
`Compact Coordinator` to scan the small files and pass compaction task to `Compact Worker`. In streaming mode, if you
run insert sql in flink, the topology will be like this:

{{< img src="/img/unaware-bucket-topo.png">}}

Do not worry about backpressure, compaction never backpressure.

If you set `write-only` to true, the `Compact Coordinator` and `Compact Worker` will be removed in the topology.

The auto compaction is only supported in Flink engine streaming mode. You can also start a compaction job in flink by
flink action in paimon and disable all the other compaction by set `write-only`.

## Streaming Query

You can stream the Append table and use it like a Message Queue. As with primary key tables, there are two options
for streaming reads:
1. By default, Streaming read produces the latest snapshot on the table upon first startup, and continue to read the
   latest incremental records.
2. You can specify `scan.mode` or `scan.snapshot-id` or `scan.timestamp-millis` or `scan.file-creation-time-millis` to
   streaming read incremental only. 

Similar to flink-kafka, order is not guaranteed by default, if your data has some sort of order requirement, you also
need to consider defining a `bucket-key`, see [Bucketed Append]({{< ref "append-table/bucketed-append" >}}).

## OLAP Query 

### Data Skipping By Order

Paimon by default records the maximum and minimum values of each field in the manifest file.

In the query, according to the `WHERE` condition of the query, according to the statistics in the manifest do files
filtering, if the filtering effect is good, the query would have been minutes of the query will be accelerated to
milliseconds to complete the execution.

Often the data distribution is not always effective filtering, so if we can sort the data by the field in `WHERE` condition?
You can take a look to [Flink COMPACT Action]({{< ref "maintenance/dedicated-compaction#sort-compact" >}}) or
[Flink COMPACT Procedure]({{< ref "flink/procedures" >}}) or [Spark COMPACT Procedure]({{< ref "spark/procedures" >}}).

### Data Skipping By File Index

You can use file index too, it filters files by index on the read side.

```sql
CREATE TABLE <PAIMON_TABLE> (<COLUMN> <COLUMN_TYPE> , ...) WITH (
    'file-index.bloom-filter.columns' = 'c1,c2',
    'file-index.bloom-filter.c1.items' = '200'
);
```

## DELETE & UPDATE

Now, only Spark SQL supports DELETE & UPDATE, you can take a look to [Spark Write]({{< ref "spark/sql-write" >}}).