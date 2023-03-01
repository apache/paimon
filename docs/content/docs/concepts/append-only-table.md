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

By specifying `'write-mode' = 'append-only'` when creating the table, user creates an append-only table.

You can only insert a complete record into the table. No delete or update is supported and you cannot define primary keys.
This type of table is suitable for use cases that do not require updates (such as log data synchronization).

## Bucketing

You can also define bucket number for Append-only table, see [Bucket]({{< ref "docs/concepts/basic-concepts#bucket" >}}).

It is recommended that you set the `bucket-key` field. Otherwise, the data will be hashed according to the whole row,
and the performance will be poor.

## Streaming Read Order

For streaming reads, records are produced in the following order:

* For any two records from two different partitions
  * If `scan.plan-sort-partition` is set to true, the record with a smaller partition value will be produced first.
  * Otherwise, the record with an earlier partition creation time will be produced first.
* For any two records from the same partition and the same bucket, the first written record will be produced first.
* For any two records from the same partition but two different buckets, different buckets are processed by different tasks, there is no order guarantee between them.

## Compaction

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
            <td><h5>compaction.early-max.file-num</h5></td>
            <td style="word-wrap: break-word;">50</td>
            <td>Integer</td>
            <td>For file set [f_0,...,f_N], the maximum file number to trigger a compaction for append-only table, even if sum(size(f_i)) &lt; targetFileSize. This value avoids pending too much small files, which slows down the performance.</td>
        </tr>
    </tbody>
</table>

## Example

The following is an example of creating the Append-Only table and specifying the bucket key.

{{< tabs "create-append-only-table" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    'write-mode' = 'append-only',
    'bucket' = '8',
    'bucket-key' = 'product_id'
);
```

{{< /tab >}}
