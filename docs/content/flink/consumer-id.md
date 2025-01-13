---
title: "Consumer ID"
weight: 5
type: docs
aliases:
- /flink/consumer-id.html
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

# Consumer ID

Consumer id can help you accomplish the following two things:

1. Safe consumption: When deciding whether a snapshot has expired, Paimon looks at all the consumers of the table in
   the file system, and if there are consumers that still depend on this snapshot, then this snapshot will not be
   deleted by expiration.
2. Resume from breakpoint: When previous job is stopped, the newly started job can continue to consume from the previous
   progress without resuming from the state.

## Usage

You can specify the `consumer-id` when streaming read table.

The consumer will prevent expiration of the snapshot. In order to prevent too many snapshots caused by mistakes,
you need to specify `'consumer.expiration-time'` to manage the lifetime of consumers.

```sql
ALTER TABLE t SET ('consumer.expiration-time' = '1 d');
```

Then, restart streaming write job of this table, expiration of consumers will be triggered in writing job.

```sql
SELECT * FROM t /*+ OPTIONS('consumer-id' = 'myid', 'consumer.mode' = 'at-least-once') */;
```

## Ignore Progress

Sometimes, you only want the feature of 'Safe Consumption'. You want to get a new snapshot progress when restarting the
stream consumption job , you can enable the `'consumer.ignore-progress'` option.

```sql
SELECT * FROM t /*+ OPTIONS('consumer-id' = 'myid', 'consumer.ignore-progress' = 'true') */;
```

The startup of this job will retrieve the snapshot that should be read again.

## Consumer Mode

By default, the consumption of snapshots is strictly aligned within the checkpoint to make 'Resume from breakpoint'
feature exactly-once.

But in some scenarios where you don't need 'Resume from breakpoint', or you don't need strict 'Resume from breakpoint',
you can consider enabling `'consumer.mode' = 'at-least-once'` mode. This mode:
1. Allow readers consume snapshots at different rates and record the slowest snapshot-id among all readers into the
   consumer. It doesn't affect the checkpoint time and have good performance.
2. This mode can provide more capabilities, such as watermark alignment.

{{< hint >}}
About `'consumer.mode'`, since the implementation of `exactly-once` mode and `at-least-once` mode are completely
different, the state of flink is incompatible and cannot be restored from the state when switching modes.
{{< /hint >}}

## Reset Consumer

You can reset or delete a consumer with a given consumer ID and next snapshot ID and delete a consumer with a given
consumer ID. First, you need to stop the streaming task using this consumer ID, and then execute the reset consumer
action job.

Run the following command:

{{< tabs "reset_consumer" >}}

{{< tab "Flink SQL" >}}

```sql
CALL sys.reset_consumer(
   `table` => 'database_name.table_name', 
   consumer_id => 'consumer_id', 
   next_snapshot_id => <snapshot_id>
);
-- No next_snapshot_id if you want to delete the consumer
```
{{< /tab >}}

{{< tab "Flink Action" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    reset-consumer \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --consumer_id <consumer-id> \
    [--next_snapshot <next-snapshot-id>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

## No next_snapshot if you want to delete the consumer
```
{{< /tab >}}

{{< /tabs >}}
