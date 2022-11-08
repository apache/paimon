---
title: "Quick Start"
weight: 1
type: docs
aliases:
- /try-table-store/quick-start.html
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

# Quick Start

This document provides a quick introduction to using Flink Table Store. Readers of this
document will be guided to create a simple dynamic table to read and write it.

## Step 1: Downloading Flink

{{< hint info >}}
__Note:__ Table Store is only supported since Flink 1.14.
{{< /hint >}}

[Download Flink 1.16](https://flink.apache.org/downloads.html),
then extract the archive:

```bash
tar -xzf flink-*.tgz
```

## Step 2: Copy Table Store Bundle Jar

{{< stable >}}
Download [flink-table-store-dist-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-dist-{{< version >}}.jar).
{{< /stable >}}
{{< unstable >}}
You are using an unreleased version of Table Store, you need to manually [Build Bundled Jar]({{< ref "docs/engines/build" >}}) from the source code.
{{< /unstable >}}

Copy table store bundle jar to flink/lib:

```bash
cp flink-table-store-dist-*.jar FLINK_HOME/lib/
```

## Step 3: Copy Hadoop Bundle Jar

[Download](https://flink.apache.org/downloads.html) Pre-bundled Hadoop.

```bash
cp flink-shaded-hadoop-2-uber-*.jar FLINK_HOME/lib/
```

## Step 4: Start Flink Local Cluster

In order to run multiple jobs, you need to modify the cluster configuration:

```bash
vi ./conf/flink-conf.yaml

taskmanager.numberOfTaskSlots: 2
```

To start a local cluster, run the bash script that comes with Flink:

```bash
./bin/start-cluster.sh
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081) to view
the Flink dashboard and see that the cluster is up and running.

Start the SQL Client CLI:

```bash
./bin/sql-client.sh embedded
```

## Step 5: Create Table

```sql
CREATE CATALOG my_catalog WITH (
  'type'='table-store',
  'warehouse'='file:/tmp/table_store'
);

USE CATALOG my_catalog;

-- create a word count table
CREATE TABLE word_count (
    word STRING PRIMARY KEY NOT ENFORCED,
    cnt BIGINT
);
```

## Step 6: Write Data

```sql
-- create a word data generator table
CREATE TEMPORARY TABLE word_table (
    word STRING
) WITH (
    'connector' = 'datagen',
    'fields.word.length' = '1'
);

-- table store requires checkpoint interval in streaming mode
SET 'execution.checkpointing.interval' = '10 s';

-- write streaming data to dynamic table
INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;
```

## Step 7: OLAP Query

```sql
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';

-- switch to batch mode
RESET 'execution.checkpointing.interval';
SET 'execution.runtime-mode' = 'batch';

-- olap query the table
SELECT * FROM word_count;
```

You can execute the query multiple times and observe the changes in the results.

## Step 8: Streaming Query

```sql
-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';

-- track the changes of table and calculate the count interval statistics
SELECT `interval`, COUNT(*) AS interval_cnt FROM
  (SELECT cnt / 10000 AS `interval` FROM word_count) GROUP BY `interval`;
```

With the streaming mode, you can get the change log of the dynamic table,
and perform new stream computations.

## Step 9: Exit

Cancel streaming job in [localhost:8081](http://localhost:8081).

```sql
-- drop the dynamic table, clear the files
DROP TABLE word_count;

-- exit sql-client
EXIT;
```

Stop the Flink local cluster:

```bash
./bin/stop-cluster.sh
```

## Congratulation!

You have completed the Flink Table Store Quick Start.
