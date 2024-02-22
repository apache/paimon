---
title: "Manage Partition"
weight: 5
type: docs
aliases:
- /maintenance/manage-partition.html
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

## Expiring Partitions

You can set `partition.expiration-time` when creating a partitioned table. Paimon will periodically check
the status of partitions and delete expired partitions according to time.

How to determine whether a partition has expired: compare the time extracted from the partition with the current
time to see if survival time has exceeded the `partition.expiration-time`.

{{< hint info >}}
__Note:__ After the partition expires, it is logically deleted and the latest snapshot cannot query its data. But the
files in the file system are not immediately physically deleted, it depends on when the corresponding snapshot expires.
See [Expire Snapshots]({{< ref "/maintenance/manage-snapshots#expire-snapshots" >}}).
{{< /hint >}}

An example for single partition field:
```sql
CREATE TABLE t (...) PARTITIONED BY (dt) WITH (
    'partition.expiration-time' = '7 d',
    'partition.expiration-check-interval' = '1 d',
    'partition.timestamp-formatter' = 'yyyyMMdd'
);
```

An example for multiple partition fields:
```sql
CREATE TABLE t (...) PARTITIONED BY (other_key, dt) WITH (
    'partition.expiration-time' = '7 d',
    'partition.expiration-check-interval' = '1 d',
    'partition.timestamp-formatter' = 'yyyyMMdd',
    'partition.timestamp-pattern' = '$dt'
);
```

More options:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>partition.expiration-check-interval</h5></td>
            <td style="word-wrap: break-word;">1 h</td>
            <td>Duration</td>
            <td>The check interval of partition expiration.</td>
        </tr>
        <tr>
            <td><h5>partition.expiration-time</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Duration</td>
            <td>The expiration interval of a partition. A partition will be expired if it‘s lifetime is over this value. Partition time is extracted from the partition value.</td>
        </tr>
        <tr>
            <td><h5>partition.timestamp-formatter</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The formatter to format timestamp from string. It can be used with 'partition.timestamp-pattern' to create a formatter using the specified value.<ul><li>Default formatter is 'yyyy-MM-dd HH:mm:ss' and 'yyyy-MM-dd'.</li><li>Supports multiple partition fields like '$year-$month-$day $hour:00:00'.</li><li>The timestamp-formatter is compatible with Java's DateTimeFormatter.</li></ul></td>
        </tr>
        <tr>
            <td><h5>partition.timestamp-pattern</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>You can specify a pattern to get a timestamp from partitions. The formatter pattern is defined by 'partition.timestamp-formatter'.<ul><li>By default, read from the first field.</li><li>If the timestamp in the partition is a single field called 'dt', you can use '$dt'.</li><li>If it is spread across multiple fields for year, month, day, and hour, you can use '$year-$month-$day $hour:00:00'.</li><li>If the timestamp is in fields dt and hour, you can use '$dt $hour:00:00'.</li></ul></td>
        </tr>
    </tbody>
</table>
