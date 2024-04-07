---
title: "Append Scalable Table"
weight: 2
type: docs
aliases:
- /append-table/append-scalable-table.html
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

# Append Scalable Table

## Definition

By defining `'bucket' = '-1'` in table properties, you can assign a special mode (we call it "unaware-bucket mode") to this
table (see [Example]({{< ref "#example" >}})). In this mode, all the things are different. We don't have
the concept of bucket anymore, and we don't guarantee the order of streaming read. We regard this table as a batch off-line table (
although we can stream read and write still). All the records will go into one directory (for compatibility, we put them in bucket-0),
and we do not maintain the order anymore. As we don't have the concept of bucket, we will not shuffle the input records by bucket anymore,
which will speed up the inserting.

Using this mode, you can replace your Hive table to lake table.

{{< img src="/img/for-scalable.png">}}

## Compaction

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

## Sort Compact

The data in a per-partition out of order will lead a slow select, compaction may slow down the inserting. It is a good choice for you to set
`write-only` for inserting job, and after per-partition data done, trigger a partition `Sort Compact` action. See [Sort Compact]({{< ref "maintenance/dedicated-compaction#sort-compact" >}}).

## Streaming Source

Unaware-bucket mode append table supported streaming read and write, but no longer guarantee order anymore. You cannot regard it
as a queue, instead, as a lake with storage bins. Every commit will generate a new record bin, we can read the
increase by reading the new record bin, but records in one bin are flowing to anywhere they want, and we fetch them in any possible order.
While in the `Append For Queue` mode, records are not stored in bins, but in record pipe. We can see the difference below.

## Streaming Multiple Partitions Write

Since the number of write tasks that Paimon-sink needs to handle is: the number of partitions to which the data is written * the number of buckets per partition.
Therefore, we need to try to control the number of write tasks per paimon-sink task as much as possible,so that it is distributed within a reasonable range.
If each sink-task handles too many write tasks, not only will it cause problems with too many small files, but it may also lead to out-of-memory errors.

In addition, write failures introduce orphan files, which undoubtedly adds to the cost of maintaining paimon. We need to avoid this problem as much as possible.

For flink-jobs with auto-merge enabled, we recommend trying to follow the following formula to adjust the parallelism of paimon-sink(This doesn't just apply to append-tables, it actually applies to most scenarios):
```
(N*B)/P < 100   (This value needs to be adjusted according to the actual situation)
N(the number of partitions to which the data is written)
B(bucket number)
P(parallelism of paimon-sink)
100 (This is an empirically derived threshold,For flink-jobs with auto-merge disabled, this value can be reduced.
However, please note that you are only transferring part of the work to the user-compaction-job, you still have to deal with the problem in essence,
the amount of work you have to deal with has not been reduced, and the user-compaction-job still needs to be adjusted according to the above formula.)
```
You can also set `write-buffer-spillable` to true, writer can spill the records to disk. This can reduce small
files as much as possible.To use this option, you need to have a certain size of local disks for your flink cluster. This is especially important for those using flink on k8s.

For append-table, You can set `write-buffer-for-append` option for append table. Setting this parameter to true, writer will cache
the records use Segment Pool to avoid OOM.

## Example

The following is an example of creating the Append table and specifying the bucket key.

{{< tabs "create-append-table-unaware-bucket" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    'bucket' = '-1'
);
```
{{< /tab >}}

{{< /tabs >}}
