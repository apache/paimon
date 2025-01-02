---
title: "Bucketed"
weight: 5
type: docs
aliases:
- /append-table/bucketed.html
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

# Bucketed Append

You can define the `bucket` and `bucket-key` to get a bucketed append table.

Example to create bucketed append table:

{{< tabs "create-bucketed-append" >}}
{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (
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

## Streaming

An ordinary Append table has no strict ordering guarantees for its streaming writes and reads, but there are some cases
where you need to define a key similar to Kafka's.

Every record in the same bucket is ordered strictly, streaming read will transfer the record to down-stream exactly in
the order of writing. To use this mode, you do not need to config special configurations, all the data will go into one
bucket as a queue.

{{< img src="/img/for-queue.png">}}

### Compaction in Bucket

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
            <td>For file set [f_0,...,f_N], the minimum file number which satisfies sum(size(f_i)) &gt;= targetFileSize to trigger a compaction for append table. This value avoids almost-full-file to be compacted, which is not cost-effective.</td>
        </tr>
        <tr>
            <td><h5>compaction.max.file-num</h5></td>
            <td style="word-wrap: break-word;">5</td>
            <td>Integer</td>
            <td>For file set [f_0,...,f_N], the maximum file number to trigger a compaction for append table, even if sum(size(f_i)) &lt; targetFileSize. This value avoids pending too much small files, which slows down the performance.</td>
        </tr>
        <tr>
            <td><h5>full-compaction.delta-commits</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>Full compaction will be constantly triggered after delta commits.</td>
        </tr>
    </tbody>
</table>

### Streaming Read Order

For streaming reads, records are produced in the following order:

* For any two records from two different partitions
   * If `scan.plan-sort-partition` is set to true, the record with a smaller partition value will be produced first.
   * Otherwise, the record with an earlier partition creation time will be produced first.
* For any two records from the same partition and the same bucket, the first written record will be produced first.
* For any two records from the same partition but two different buckets, different buckets are processed by different tasks, there is no order guarantee between them.

### Watermark Definition

You can define watermark for reading Paimon tables:

```sql
CREATE TABLE t (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (...);

-- launch a bounded streaming job to read paimon_table
SELECT window_start, window_end, COUNT(`user`) FROM TABLE(
 TUMBLE(TABLE t, DESCRIPTOR(order_time), INTERVAL '10' MINUTES)) GROUP BY window_start, window_end;
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

### Bounded Stream

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

## Batch

Bucketed table can be used to avoid shuffle if necessary in batch query, for example, you can use the following Spark
SQL to read a Paimon table:

```sql
SET spark.sql.sources.v2.bucketing.enabled = true;

CREATE TABLE FACT_TABLE (order_id INT, f1 STRING) TBLPROPERTIES ('bucket'='10', 'bucket-key' = 'order_id');

CREATE TABLE DIM_TABLE (order_id INT, f2 STRING) TBLPROPERTIES ('bucket'='10', 'primary-key' = 'order_id');

SELECT * FROM FACT_TABLE JOIN DIM_TABLE on t1.order_id = t4.order_id;
```

The `spark.sql.sources.v2.bucketing.enabled` config is used to enable bucketing for V2 data sources. When turned on,
Spark will recognize the specific distribution reported by a V2 data source through SupportsReportPartitioning, and
will try to avoid shuffle if necessary.

The costly join shuffle will be avoided if two tables have the same bucketing strategy and same number of buckets.
