---
title: "Manage Snapshots"
weight: 4
type: docs
aliases:
- /maintenance/manage-snapshots.html
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

# Manage Snapshots

This section will describe the management and behavior related to snapshots.

## Expire Snapshots

Paimon writers generate one or two [snapshot]({{< ref "concepts/basic-concepts#snapshot" >}}) per commit. Each snapshot may add some new data files or mark some old data files as deleted. However, the marked data files are not truly deleted because Paimon also supports time traveling to an earlier snapshot. They are only deleted when the snapshot expires.

Currently, expiration is automatically performed by Paimon writers when committing new changes. By expiring old snapshots, old data files and metadata files that are no longer used can be deleted to release disk space.

Snapshot expiration is controlled by the following table properties.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Required</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>snapshot.time-retained</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">1 h</td>
      <td>Duration</td>
      <td>The maximum time of completed snapshots to retain.</td>
    </tr>
    <tr>
      <td><h5>snapshot.num-retained.min</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The minimum number of completed snapshots to retain. Should be greater than or equal to 1.</td>
    </tr>
    <tr>
      <td><h5>snapshot.num-retained.max</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">Integer.MAX_VALUE</td>
      <td>Integer</td>
      <td>The maximum number of completed snapshots to retain. Should be greater than or equal to the minimum number.</td>
    </tr>
    <tr>
      <td><h5>snapshot.expire.execution-mode</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">sync</td>
      <td>Enum</td>
      <td>Specifies the execution mode of expire.</td>
    </tr>
    <tr>
      <td><h5>snapshot.expire.limit</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of snapshots allowed to expire at a time.</td>
    </tr>
    </tbody>
</table>

When the number of snapshots is less than `snapshot.num-retained.min`, no snapshots will be expired(even the condition `snapshot.time-retained` meet), after which `snapshot.num-retained.max` and `snapshot.time-retained` will be used to control the snapshot expiration until the remaining snapshot meets the condition.

The following example show more details(`snapshot.num-retained.min` is 2, `snapshot.time-retained` is 1h, `snapshot.num-retained.max` is 5):

> snapshot item is described using tuple (snapshotId, corresponding time)

<table>
<thead>
    <tr>
      <th class="text-left" style="width: 30%">New Snapshots</th>
      <th class="text-left" style="width: 40%">All snapshots after expiration check</th>
      <th class="text-left" style="width: 30%">explanation</th>
    </tr>
    </thead>
    <tbody>
      <tr>
        <td>
         (snapshots-1, 2023-07-06 10:00)
        </td>
        <td>
         (snapshots-1, 2023-07-06 10:00)
        </td>
        <td>
          No snapshot expired
        </td>
      </tr>
      <tr>
        <td>
         (snapshots-2, 2023-07-06 10:20)
        </td>
        <td>
         (snapshots-1, 2023-07-06 10:00) <br>
         (snapshots-2, 2023-07-06 10:20)
        </td>
        <td>
          No snapshot expired
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-3, 2023-07-06 10:40)
        </td>
        <td>
           (snapshots-1, 2023-07-06 10:00) <br>
           (snapshots-2, 2023-07-06 10:20) <br>
           (snapshots-3, 2023-07-06 10:40)
        </td>
        <td>
           No snapshot expired
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-4, 2023-07-06 11:00)
        </td>
        <td>
          (snapshots-1, 2023-07-06 10:00) <br>
          (snapshots-2, 2023-07-06 10:20) <br>
          (snapshots-3, 2023-07-06 10:40) <br>
          (snapshots-4, 2023-07-06 11:00) <br>
        </td>
        <td>
           No snapshot expired
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-5, 2023-07-06 11:20)
        </td>
        <td>
          (snapshots-2, 2023-07-06 10:20) <br>
          (snapshots-3, 2023-07-06 10:40) <br>
          (snapshots-4, 2023-07-06 11:00) <br>
          (snapshots-5, 2023-07-06 11:20)
        </td>
        <td>
           snapshot-1 was expired because the condition `snapshot.time-retained` is not met
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-6, 2023-07-06 11:30)
        </td>
        <td>
        (snapshots-3, 2023-07-06 10:40) <br>
        (snapshots-4, 2023-07-06 11:00) <br>
        (snapshots-5, 2023-07-06 11:20) <br>
        (snapshots-6, 2023-07-06 11:30)
        </td>
        <td>
        snapshot-2 was expired because the condition `snapshot.time-retained` is not met
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-7, 2023-07-06 11:35)
        </td>
        <td>
        (snapshots-3, 2023-07-06 10:40) <br>
        (snapshots-4, 2023-07-06 11:00) <br>
        (snapshots-5, 2023-07-06 11:20) <br>
        (snapshots-6, 2023-07-06 11:30) <br>
        (snapshots-7, 2023-07-06 11:35)
        </td>
        <td>
        No snapshot expired
        </td>
      </tr>
      <tr>
        <td>
          (snapshots-8, 2023-07-06 11:36)
        </td>
        <td>
        (snapshots-4, 2023-07-06 11:00) <br>
        (snapshots-5, 2023-07-06 11:20) <br>
        (snapshots-6, 2023-07-06 11:30) <br>
        (snapshots-7, 2023-07-06 11:35) <br>
        (snapshots-8, 2023-07-06 11:36)
        </td>
        <td>
         snapshot-3 was expired because the condition `snapshot.num-retained.max` is not met
        </td>
      </tr>
    </tbody>
</table>

Please note that too short retain time or too small retain number may result in:

- Batch queries cannot find the file. For example, the table is relatively large and
  the batch query takes 10 minutes to read, but the snapshot from 10 minutes ago
  expires, at which point the batch query will read a deleted snapshot.
- Streaming reading jobs on table files fail to restart.
  When the job restarts, the snapshot it recorded may have expired. (You can use
  [Consumer Id]({{< ref "flink/sql-query#consumer-id" >}}) to protect streaming reading
  in a small retain time of snapshot expiration).

By default, paimon will delete expired snapshots synchronously. When there are too 
many files that need to be deleted, they may not be deleted quickly and back-pressured 
to the upstream operator. To avoid this situation, users can use asynchronous expiration 
mode by setting `snapshot.expire.execution-mode` to `async`.

## Rollback to Snapshot

Rollback a table to a specific snapshot ID.

{{< tabs "rollback-to" >}}

{{< tab "Flink SQL" >}}

Run the following command:

```sql
CALL sys.rollback_to(`table` => 'database_name.table_name', snapshot_id => <snasphot-id>);
```

{{< /tab >}}

{{< tab "Flink Action" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    rollback_to \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --version <snapshot-id> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class RollbackTo {

    public static void main(String[] args) {
        // before rollback:
        // snapshot-3
        // snapshot-4
        // snapshot-5
        // snapshot-6
        // snapshot-7
      
        table.rollbackTo(5);
        
        // after rollback:
        // snapshot-3
        // snapshot-4
        // snapshot-5
    }
}
```

{{< /tab >}}

{{< tab "Spark" >}}

Run the following sql:

```sql
CALL rollback(table => 'test.T', version => '2');
```

{{< /tab >}}

{{< /tabs >}}

## Remove Orphan Files

Paimon files are deleted physically only when expiring snapshots. However, it is possible that some unexpected errors occurred
when deleting files, so that there may exist files that are not used by Paimon snapshots (so-called "orphan files"). You can
submit a `remove_orphan_files` job to clean them:

{{< tabs "remove_orphan_files" >}}

{{< tab "Spark SQL/Flink SQL" >}}
```sql
CALL sys.remove_orphan_files(`table` => "my_db.my_table", [older_than => "2023-10-31 12:00:00"])

CALL sys.remove_orphan_files(`table` => "my_db.*", [older_than => "2023-10-31 12:00:00"])
```
{{< /tab >}}

{{< tab "Flink Action" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    remove_orphan_files \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    [--older_than <timestamp>] \
    [--dry_run <false/true>] \
    [--parallelism <parallelism>]
```

To avoid deleting files that are newly added by other writing jobs, this action only deletes orphan files older than
1 day by default. The interval can be modified by `--older_than`. For example:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    remove_orphan_files \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table T \
    --older_than '2023-10-31 12:00:00'
```

The table can be `*` to clean all tables in the database.

{{< /tab >}}

{{< /tabs >}}