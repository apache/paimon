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

Paimon writers generates one or two [snapshots]({{< ref "concepts/basic-concepts#snapshots" >}}) per commit. Each snapshot may add some new data files or mark some old data files as deleted. However, the marked data files are not truly deleted because Paimon also supports time traveling to an earlier snapshot. They are only deleted when the snapshot expires.

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
      <td>The maximum number of completed snapshots to retain. Should be greater than the minimum number.</td>
    </tr>
    </tbody>
</table>

Please note that too short retain time or too small retain number may result in:

- Batch queries cannot find the file. For example, the table is relatively large and
  the batch query takes 10 minutes to read, but the snapshot from 10 minutes ago
  expires, at which point the batch query will read a deleted snapshot.
- Streaming reading jobs on table files (without the external log system) fail to restart.
  When the job restarts, the snapshot it recorded may have expired. (You can use
  [Consumer Id]({{< ref "how-to/querying-tables#consumer-id" >}}) to protect streaming reading
  in a small retain time of snapshot expiration).

## Rollback to Snapshot

Rollback a table to a specific snapshot ID.

{{< tabs "rollback-to" >}}

{{< tab "Flink" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    rollback-to \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --snapshot <snapshot-id> \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]
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

{{< /tabs >}}