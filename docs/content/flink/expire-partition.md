---
title: "Expire Partition"
weight: 96
type: docs
aliases:
- /flink/expire-partition.html
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

You can set `partition.expiration-time` when creating a partitioned table. Paimon streaming sink will periodically check
the status of partitions and delete expired partitions according to time.

How to determine whether a partition has expired: you can set `partition.expiration-strategy` when creating a partitioned table,
this strategy determines how to extract the partition time and compare it with the current time to see if survival time
has exceeded the `partition.expiration-time`. Expiration strategy supported values are:

- `values-time` : The strategy compares the time extracted from the partition value with the current time,
this strategy as the default.
- `update-time` : The strategy compares the last update time of the partition with the current time. 
What is the scenario for this strategy:
   - Your partition value is non-date formatted.
   - You only want to keep data that has been updated in the last n days/months/years.
   - Data initialization imports a large amount of historical data.

{{< hint info >}}
__Note:__ After the partition expires, it is logically deleted and the latest snapshot cannot query its data. But the
files in the file system are not immediately physically deleted, it depends on when the corresponding snapshot expires.
See [Expire Snapshots]({{< ref "/maintenance/manage-snapshots#expire-snapshots" >}}).
{{< /hint >}}

An example for single partition field:

`values-time` strategy.
```sql
CREATE TABLE t (...) PARTITIONED BY (dt) WITH (
    'partition.expiration-time' = '7 d',
    'partition.expiration-check-interval' = '1 d',
    'partition.timestamp-formatter' = 'yyyyMMdd'   -- this is required in `values-time` strategy.
);
-- Let's say now the date is 2024-07-09，so before the date of 2024-07-02 will expire.
insert into t values('pk', '2024-07-01');

-- An example for multiple partition fields
CREATE TABLE t (...) PARTITIONED BY (other_key, dt) WITH (
    'partition.expiration-time' = '7 d',
    'partition.expiration-check-interval' = '1 d',
    'partition.timestamp-formatter' = 'yyyyMMdd',
    'partition.timestamp-pattern' = '$dt'
);
```

`update-time` strategy.
```sql
CREATE TABLE t (...) PARTITIONED BY (dt) WITH (
    'partition.expiration-time' = '7 d',
    'partition.expiration-check-interval' = '1 d',
    'partition.expiration-strategy' = 'update-time'
);

-- The last update time of the partition is now, so it will not expire.
insert into t values('pk', '2024-01-01');
-- Support non-date formatted partition.
insert into t values('pk', 'par-1'); 

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
            <td><h5>partition.expiration-strategy</h5></td>
            <td style="word-wrap: break-word;">values-time</td>
            <td>String</td>
            <td>
                Specifies the expiration strategy for partition expiration. 
                Possible values:
                <li>values-time: The strategy compares the time extracted from the partition value with the current time.</li>
                <li>update-time: The strategy compares the last update time of the partition with the current time.</li>
            </td>
        </tr>
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
