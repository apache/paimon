---
title: "Flink"
weight: 2
type: docs
aliases:
- /engines/flink.html
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

# Flink

This documentation is a guide for using Paimon in Flink.

## Preparing Paimon Jar File

Paimon currently supports Flink 1.16, 1.15 and 1.14. We recommend the latest Flink version for a better experience.

Download the jar file with corresponding version.

{{< stable >}}

| Version    | Jar                                                                                                                                               |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.17 | [paimon-flink-1.17-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-flink-1.17-{{< version >}}.jar) |
| Flink 1.16 | [paimon-flink-1.16-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-flink-1.16-{{< version >}}.jar) |
| Flink 1.15 | [paimon-flink-1.15-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-flink-1.15-{{< version >}}.jar) |
| Flink 1.14 | [paimon-flink-1.14-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-flink-1.14-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

| Version    | Jar                                                                                                                                   |
|------------|---------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.17 | [paimon-flink-1.17-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/{{< version >}}/) |
| Flink 1.16 | [paimon-flink-1.16-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.16/{{< version >}}/) |
| Flink 1.15 | [paimon-flink-1.15-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.15/{{< version >}}/) |
| Flink 1.14 | [paimon-flink-1.14-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.14/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.
- `mvn clean install -DskipTests`

You can find the bundled jar in `./paimon-flink/paimon-flink-<flink-version>/target/paimon-flink-<flink-version>-{{< version >}}.jar`.

## Quick Start

**Step 1: Download Flink**

If you haven't downloaded Flink, you can [download Flink 1.16](https://flink.apache.org/downloads.html), then extract the archive with the following command.

```bash
tar -xzf flink-*.tgz
```

**Step 2: Copy Paimon Bundled Jar**

Copy paimon bundled jar to the `lib` directory of your Flink home.

```bash
cp paimon-flink-*.jar <FLINK_HOME>/lib/
```

**Step 3: Copy Hadoop Bundled Jar**

{{< hint info >}}
If the machine is in a hadoop environment, please ensure the value of the environment variable `HADOOP_CLASSPATH`, you do not need to use the following pre-bundled Hadoop jar.
{{< /hint >}}

[Download](https://flink.apache.org/downloads.html) Pre-bundled Hadoop jar and copy the jar file to the `lib` directory of your Flink home.

```bash
cp flink-shaded-hadoop-2-uber-*.jar <FLINK_HOME>/lib/
```

**Step 4: Start a Flink Local Cluster**

In order to run multiple Flink jobs at the same time, you need to modify the cluster configuration in `<FLINK_HOME>/conf/flink-conf.yaml`.

```yaml
taskmanager.numberOfTaskSlots: 2
```

To start a local cluster, run the bash script that comes with Flink:

```bash
<FLINK_HOME>/bin/start-cluster.sh
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081) to view
the Flink dashboard and see that the cluster is up and running.

You can now start Flink SQL client to execute SQL scripts.

```bash
<FLINK_HOME>/bin/sql-client.sh
```

**Step 5: Create a Catalog and a Table**

```sql
-- if you're trying out Paimon in a distributed environment,
-- warehouse path should be set to a shared file system, such as HDFS or OSS
CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

-- create a word count table
CREATE TABLE word_count (
    word STRING PRIMARY KEY NOT ENFORCED,
    cnt BIGINT
);
```

**Step 6: Write Data**

```sql
-- create a word data generator table
CREATE TEMPORARY TABLE word_table (
    word STRING
) WITH (
    'connector' = 'datagen',
    'fields.word.length' = '1'
);

-- paimon requires checkpoint interval in streaming mode
SET 'execution.checkpointing.interval' = '10 s';

-- write streaming data to dynamic table
INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word;
```

**Step 7: OLAP Query**

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

**Step 8: Streaming Query**

```sql
-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';

-- track the changes of table and calculate the count interval statistics
SELECT `interval`, COUNT(*) AS interval_cnt FROM
    (SELECT cnt / 10000 AS `interval` FROM word_count) GROUP BY `interval`;
```

**Step 9: Exit**

Cancel streaming job in [localhost:8081](http://localhost:8081), then execute the following SQL script to exit Flink SQL client.

```sql
-- uncomment the following line if you want to drop the dynamic table and clear the files
-- DROP TABLE word_count;

-- exit sql-client
EXIT;
```

Stop the Flink local cluster.

```bash
./bin/stop-cluster.sh
```

## Supported Flink Data Type

See [Flink Data Types](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/types/).

All Flink data types are supported, except that

* `MULTISET` is not supported.
* `MAP` is not supported as primary keys.
