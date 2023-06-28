---
title: "Manage Tags"
weight: 7
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

Paimon's snapshots can provide a easy way to query historical data. But in most scenarios, a job will generate too many
snapshots and table will expire old snapshots according to table configuration. Snapshot expiration will also delete old
data files, and the historical data of expired snapshots cannot be queried anymore.

To solve this problem, you can create a tag based on a snapshot. The tag will maintain the manifests and data files of the
snapshot. A typical usage is creating tags daily, then you can maintain the historical data of each day for batch reading.

## Create Tags

You can create a tag with given name (cannot be number) and snapshot ID.

{{< tabs "create-tag" >}}

{{< tab "Flink" >}}

 Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    create-tag \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --tag-name <tag-name> \
    --snapshot <snapshot-id> \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< tab "Java API" >}}

```java
import org.apache.paimon.table.Table;

public class CreateTag {

    public static void main(String[] args) {
        Table table = ...;
        table.createTag("my-tag", 1);
    }
}
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
    delete-tag \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --tag-name <tag-name> \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]
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
    rollback-to \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --vesion <tag-name> \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]
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

{{< /tabs >}}