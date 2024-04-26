---
title: "Manage Tags"
weight: 8
type: docs
aliases:
- /maintenance/manage-tags.html
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

# Manage Tags

Paimon's snapshots can provide an easy way to query historical data. But in most scenarios, a job will generate too many
snapshots and table will expire old snapshots according to table configuration. Snapshot expiration will also delete old
data files, and the historical data of expired snapshots cannot be queried anymore.

To solve this problem, you can create a tag based on a snapshot. The tag will maintain the manifests and data files of the
snapshot. A typical usage is creating tags daily, then you can maintain the historical data of each day for batch reading.

## Automatic Creation

Paimon supports automatic creation of tags in writing job.

**Step 1: Choose Creation Mode**

You can set creation mode by table option `'tag.automatic-creation'`. Supported values are:
- `process-time`: Create TAG based on the time of the machine.
- `watermark`: Create TAG based on the watermark of the Sink input.
- `batch`: In a batch processing scenario, a tag is generated after the current task is completed.

{{< hint info >}}
If you choose Watermark, you may need to specify the time zone of watermark, if watermark is not in the
UTC time zone, please configure `'sink.watermark-time-zone'`.
{{< /hint >}}

**Step 2: Choose Creation Period**

What frequency is used to generate tags. You can choose `'daily'`, `'hourly'` and `'two-hours'` for `'tag.creation-period'`.

If you need to wait for late data, you can configure a delay time: `'tag.creation-delay'`.

**Step 3: Automatic deletion of tags**

You can configure `'tag.num-retained-max'` to delete tags automatically.

Example, configure table to create a tag at 0:10 every day, with a maximum retention time of 3 months:

```sql
-- Flink SQL
CREATE TABLE t (
    k INT PRIMARY KEY NOT ENFORCED,
    f0 INT,
    ...
) WITH (
    'tag.automatic-creation' = 'process-time',
    'tag.creation-period' = 'daily',
    'tag.creation-delay' = '10 m',
    'tag.num-retained-max' = '90'
);

INSERT INTO t SELECT ...;

-- Spark SQL

-- Read latest snapshot
SELECT * FROM t;

-- Read Tag snapshot
SELECT * FROM t VERSION AS OF '2023-07-26';

-- Read Incremental between Tags
SELECT * FROM paimon_incremental_query('t', '2023-07-25', '2023-07-26');
```

See [Query Tables]({{< ref "spark/sql-query" >}}) to see more query for Spark.

## Create Tags

You can create a tag with given name and snapshot ID.

{{< tabs "create-tag" >}}

{{< tab "Flink" >}}

 Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    create_tag \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --tag_name <tag-name> \
    [--snapshot <snapshot_id>] \
    [--time_retained <time-retained>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

If `snapshot` unset, snapshot_id defaults to the latest.

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class CreateTag {

    public static void main(String[] args) {
        Table table = ...;
        table.createTag("my-tag", 1);
        table.createTag("my-tag-retained-12-hours", 1, Duration.ofHours(12));
    }
}
```

{{< /tab >}}

{{< tab "Spark" >}}
Run the following sql:
```sql
CALL create_tag(table => 'test.t', tag => 'test_tag', snapshot => 2);
```

To create a tag with retained 1 day, run the following sql:
```sql
CALL create_tag(table => 'test.t', tag => 'test_tag', snapshot => 2, time_retained => '1 d');
```

To create a tag based on the latest snapshot id, run the following sql:
```sql
CALL create_tag(table => 'test.t', tag => 'test_tag');
```

{{< /tab >}}

{{< /tabs >}}

## Delete Tags

You can delete a tag by its name.

{{< tabs "delete-tag" >}}

{{< tab "Flink" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    delete_tag \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --tag_name <tag-name> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class DeleteTag {

    public static void main(String[] args) {
        Table table = ...;
        table.deleteTag("my-tag");
    }
}
```

{{< /tab >}}


{{< tab "Spark" >}}
Run the following sql:
```sql
CALL delete_tag(table => 'test.t', tag => 'test_tag');
```

{{< /tab >}}

{{< /tabs >}}

## Rollback to Tag

Rollback table to a specific tag. All snapshots and tags whose snapshot id is larger than the tag will be deleted (and 
the data will be deleted too).

{{< tabs "rollback-to" >}}

{{< tab "Flink" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    rollback_to \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --version <tag-name> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class RollbackTo {

    public static void main(String[] args) {
        // before rollback:
        // snapshot-3 [expired] -> tag3
        // snapshot-4 [expired]
        // snapshot-5 -> tag5
        // snapshot-6
        // snapshot-7
      
        table.rollbackTo("tag3");
        
        // after rollback:
        // snapshot-3 -> tag3
    }
}
```

{{< /tab >}}

{{< tab "Spark" >}}

Run the following sql:

```sql
CALL rollback(table => 'test.t', version => '2');
```

{{< /tab >}}

{{< /tabs >}}
