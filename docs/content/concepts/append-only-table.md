---
title: "Append Only Table"
weight: 7
type: docs
aliases:
- /concepts/append-only-table.html
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

# Append Only Table

If a table does not have a primary key defined, it is an append-only table by default. Separated by the definition of bucket,
we have two different append-only mode: "Append For Queue" and "Append For Scalable Table".

## Append For Queue

You can only insert a complete record into the table. No delete or update is supported, and you cannot define primary keys.
This type of table is suitable for use cases that do not require updates (such as log data synchronization).

### Definition

In this mode, you can regard append-only table as a queue separated by bucket. Every record in the same bucket is ordered strictly,
streaming read will transfer the record to down-stream exactly in the order of writing. To use this mode, you do not need 
to config special configurations, all the data will go into one bucket as a queue. You can also define the `bucket` and
`bucket-key` to enable larger parallelism and disperse data (see [Example]({{< ref "#example" >}})).

{{< img src="/img/for-queue.png">}}


### Compaction

By default, the sink node will automatically perform compaction to control the number of files. The following options
control the strategy of compaction:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>write-only</h5></td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>If set to true, compactions and snapshot expiration will be skipped. This option is used along with dedicated compact jobs.</td>
        </tr>
        <tr>
            <td><h5>compaction.min.file-num</h5></td>
            <td style="word-wrap: break-word;">5</td>
            <td>Integer</td>
            <td>For file set [f_0,...,f_N], the minimum file number which satisfies sum(size(f_i)) &gt;= targetFileSize to trigger a compaction for append-only table. This value avoids almost-full-file to be compacted, which is not cost-effective.</td>
        </tr>
        <tr>
            <td><h5>compaction.max.file-num</h5></td>
            <td style="word-wrap: break-word;">50</td>
            <td>Integer</td>
            <td>For file set [f_0,...,f_N], the maximum file number to trigger a compaction for append-only table, even if sum(size(f_i)) &lt; targetFileSize. This value avoids pending too much small files, which slows down the performance.</td>
        </tr>
        <tr>
            <td><h5>full-compaction.delta-commits</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>Full compaction will be constantly triggered after delta commits.</td>
        </tr>
    </tbody>
</table>

### Streaming Source

Streaming source behavior is only supported in Flink engine at present.

#### Streaming Read Order

For streaming reads, records are produced in the following order:

* For any two records from two different partitions
  * If `scan.plan-sort-partition` is set to true, the record with a smaller partition value will be produced first.
  * Otherwise, the record with an earlier partition creation time will be produced first.
* For any two records from the same partition and the same bucket, the first written record will be produced first.
* For any two records from the same partition but two different buckets, different buckets are processed by different tasks, there is no order guarantee between them.

#### Watermark Definition

You can define watermark for reading Paimon tables:

```sql
CREATE TABLE T (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (...);

-- launch a bounded streaming job to read paimon_table
SELECT window_start, window_end, COUNT(`user`) FROM TABLE(
 TUMBLE(TABLE T, DESCRIPTOR(order_time), INTERVAL '10' MINUTES)) GROUP BY window_start, window_end;
```

You can also enable [Flink Watermark alignment](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/#watermark-alignment-_beta_),
which will make sure no sources/splits/shards/partitions increase their watermarks too far ahead of the rest:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>scan.watermark.alignment.group</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>A group of sources to align watermarks.</td>
        </tr>
        <tr>
            <td><h5>scan.watermark.alignment.max-drift</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Duration</td>
            <td>Maximal drift to align watermarks, before we pause consuming from the source/task/partition.</td>
        </tr>
    </tbody>
</table>

#### Bounded Stream

Streaming Source can also be bounded, you can specify 'scan.bounded.watermark' to define the end condition for bounded streaming mode, stream reading will end until a larger watermark snapshot is encountered.

Watermark in snapshot is generated by writer, for example, you can specify a kafka source and declare the definition of watermark.
When using this kafka source to write to Paimon table, the snapshots of Paimon table will generate the corresponding watermark,
so that you can use the feature of bounded watermark when streaming reads of this Paimon table.

```sql
CREATE TABLE kafka_table (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka'...);

-- launch a streaming insert job
INSERT INTO paimon_table SELECT * FROM kakfa_table;

-- launch a bounded streaming job to read paimon_table
SELECT * FROM paimon_table /*+ OPTIONS('scan.bounded.watermark'='...') */;
```

### Example

The following is an example of creating the Append-Only table and specifying the bucket key.

{{< tabs "create-append-only-table" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    'bucket' = '8',
    'bucket-key' = 'product_id'
);
```
{{< /tab >}}

{{< /tabs >}}



## Append For Scalable Table

{{< hint info >}}
This is an experimental feature.
{{< /hint >}}

### Definition

By defining `'bucket' = '-1'` in table properties, you can assign a special mode (we call it "unaware-bucket mode") to this 
table (see [Example]({{< ref "#example-1" >}})). In this mode, all the things are different. We don't have
the concept of bucket anymore, and we don't guarantee the order of streaming read. We regard this table as a batch off-line table (
although we can stream read and write still). All the records will go into one directory (for compatibility, we put them in bucket-0),
and we do not maintain the order anymore. As we don't have the concept of bucket, we will not shuffle the input records by bucket anymore,
which will speed up the inserting.

Using this mode, you can replace your Hive table to lake table.

{{< img src="/img/for-scalable.png">}}

### Compaction

In unaware-bucket mode, we don't do compaction in writer, instead, we use `Compact Coordinator` to scan the small files and submit compaction task
to `Compact Worker`. By this, we can easily do compaction for one simple data directory in parallel. In streaming mode, if you run insert sql in flink,
the topology will be like this:

{{< img src="/img/unaware-bucket-topo.png">}}

It will do its best to compact small files, but when a single small file in one partition remains long time 
and no new file added to the partition, the `Compact Coordinator` will remove it from memory to reduce memory usage. 
After you restart the job, it will scan the small files and add it to memory again. The options to control the compact
behavior is exactly the same as [Append For Qeueue]({{< ref "#compaction" >}}). If you set `write-only` to true, the 
`Compact Coordinator` and `Compact Worker` will be removed in the topology.

The auto compaction is only supported in Flink engine streaming mode. You can also start a compaction job in flink by flink action in paimon
and disable all the other compaction by set `write-only`.

### Sort Compact

The data in a per-partition out of order will lead a slow select, compaction may slow down the inserting. It is a good choice for you to set 
write-only for inserting job, and after per-partition data done, trigger a partition `Sort Compact` action. 

You can trigger action by shell script:
```shell
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table <tableName> \
    --table-conf <key>=<value> \
    --order-strategy <orderType> \
    --order-by <col1,col2,...>
```

{{< generated/sort-compact >}}

The sort parallelism is the same as the sink parallelism, you can dynamically specify it by add conf --table-conf sink.parallelism=<value>.
Other config is the same as [Compact Table]({{< ref "concepts/file-operations#compact-table" >}})

### Streaming Source

Unaware-bucket mode append-only table supported streaming read and write, but no longer guarantee order anymore. You cannot regard it 
as a queue, instead, as a lake with storage bins. Every commit will generate a new record bin, we can read the 
increase by reading the new record bin, but records in one bin are flowing to anywhere they want, and we fetch them in any possible order.
While in the `Append For Queue` mode, records are not stored in bins, but in record pipe. We can see the difference below.


### Example

The following is an example of creating the Append-Only table and specifying the bucket key.

{{< tabs "create-append-only-table-unaware-bucket" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    'bucket' = '-1'
);
```
{{< /tab >}}

{{< /tabs >}}

## Multiple Partitions Write

While writing multiple partitions in a single insert job, we may get an out-of-memory error if
too many records arrived between two checkpoint.

You can `write-buffer-for-append` option for append-only table. Setting this parameter to true, writer will cache
the records use Segment Pool to avoid OOM.

You can also set `write-buffer-spillable` to true, writer can spill the records to disk. This can reduce small
files as much as possible.
