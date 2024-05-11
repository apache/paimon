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
);
```
{{< /tab >}}
{{< /tabs >}}

## Data Distribution

By default, append table has no bucket concept. It acts just like a Hive Table. The data files are placed under
partitions where they can be reorganized and reordered to speed up queries.

## Automatic small file merging

In streaming writing job, without bucket definition, there is no compaction in writer, instead, will use
`Compact Coordinator` to scan the small files and pass compaction task to `Compact Worker`. In streaming mode, if you
run insert sql in flink, the topology will be like this:

{{< img src="/img/unaware-bucket-topo.png">}}

Do not worry about backpressure, compaction never backpressure.

If you set `write-only` to true, the `Compact Coordinator` and `Compact Worker` will be removed in the topology.

The auto compaction is only supported in Flink engine streaming mode. You can also start a compaction job in flink by
flink action in paimon and disable all the other compaction by set `write-only`.