---
title: "Quick Start"
weight: 1
type: docs
aliases:
- /flink/quick-start.html
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

This documentation is a guide for using Paimon in Flink.

## Jars

Paimon currently supports Flink 1.20, 1.19, 1.18, 1.17, 1.16, 1.15. We recommend the latest Flink version for a better experience.

Download the jar file with corresponding version.

> Currently, paimon provides two types jar: one of which(the bundled jar) is used for read/write data, and the other(action jar) for operations such as manually compaction,
{{< stable >}}

| Version      | Type        | Jar                                                                                                                                                                           |
|--------------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.20   | Bundled Jar | [paimon-flink-1.20-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/{{< version >}}/paimon-flink-1.20-{{< version >}}.jar)       |
| Flink 1.19   | Bundled Jar | [paimon-flink-1.19-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.19/{{< version >}}/paimon-flink-1.19-{{< version >}}.jar)       |
| Flink 1.18   | Bundled Jar | [paimon-flink-1.18-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/{{< version >}}/paimon-flink-1.18-{{< version >}}.jar)       |
| Flink 1.17   | Bundled Jar | [paimon-flink-1.17-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.17/{{< version >}}/paimon-flink-1.17-{{< version >}}.jar)       |
| Flink 1.16   | Bundled Jar | [paimon-flink-1.16-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.16/{{< version >}}/paimon-flink-1.16-{{< version >}}.jar)       |
| Flink 1.15   | Bundled Jar | [paimon-flink-1.15-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.15/{{< version >}}/paimon-flink-1.15-{{< version >}}.jar)       |
| Flink Action | Action Jar  | [paimon-flink-action-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-action/{{< version >}}/paimon-flink-action-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

| Version      | Type        | Jar                                                                                                                                       |
|--------------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.20   | Bundled Jar | [paimon-flink-1.20-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.20/{{< version >}}/)     |
| Flink 1.19   | Bundled Jar | [paimon-flink-1.19-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.19/{{< version >}}/)     |
| Flink 1.18   | Bundled Jar | [paimon-flink-1.18-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.18/{{< version >}}/)     |
| Flink 1.17   | Bundled Jar | [paimon-flink-1.17-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/{{< version >}}/)     |
| Flink 1.16   | Bundled Jar | [paimon-flink-1.16-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.16/{{< version >}}/)     |
| Flink 1.15   | Bundled Jar | [paimon-flink-1.15-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.15/{{< version >}}/)     |
| Flink Action | Action Jar  | [paimon-flink-action-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-action/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.
- `mvn clean install -DskipTests`

You can find the bundled jar in `./paimon-flink/paimon-flink-<flink-version>/target/paimon-flink-<flink-version>-{{< version >}}.jar`, and the action jar in `./paimon-flink/paimon-flink-action/target/paimon-flink-action-{{< version >}}.jar`.

## Start

**Step 1: Download Flink**

If you haven't downloaded Flink, you can [download Flink](https://flink.apache.org/downloads.html), then extract the archive with the following command.

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
If the machine is in a hadoop environment, please ensure the value of the environment variable `HADOOP_CLASSPATH` include path to the common Hadoop libraries, you do not need to use the following pre-bundled Hadoop jar.
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

{{< tabs "Create Flink Catalog" >}}

{{< tab "Catalog" >}}

```sql
-- if you're trying out Paimon in a distributed environment,
-- the warehouse path should be set to a shared file system, such as HDFS or OSS
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

{{< /tab >}}

{{< tab "Generic-Catalog" >}}

Using FlinkGenericCatalog, you need to use Hive metastore. Then, you can use all the tables from Paimon, Hive, and
Flink Generic Tables (Kafka and other tables)!

In this mode, you should use 'connector' option for creating tables.

{{< hint info >}}
Paimon will use `hive.metastore.warehouse.dir` in your `hive-site.xml`, please use path with scheme.
For example, `hdfs://...`. Otherwise, Paimon will use the local path.
{{< /hint >}}

```sql
CREATE CATALOG my_catalog WITH (
    'type'='paimon-generic',
    'hive-conf-dir'='...',
    'hadoop-conf-dir'='...'
);

USE CATALOG my_catalog;

-- create a word count table
CREATE TABLE word_count (
    word STRING PRIMARY KEY NOT ENFORCED,
    cnt BIGINT
) WITH (
    'connector'='paimon'
);
```

{{< /tab >}}

{{< /tabs >}}

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

## Use Flink Managed Memory

Paimon tasks can create memory pools based on executor memory which will be managed by Flink executor, such as managed memory in Flink task manager. It will improve the stability and performance of sinks by managing writer buffers for multiple tasks through executor.

The following properties can be set if using Flink managed memory:

| Option    | Default | Description                                                                                                                                                                    |
|------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sink.use-managed-memory-allocator | false | If true, flink sink will use managed memory for merge tree; otherwise, it will create an independent memory allocator, which means each task allocates and manages its own memory pool (heap memory), if there are too many tasks in one Executor, it may cause performance issues and even OOM. |
| sink.managed.writer-buffer-memory | 256M  | Weight of writer buffer in managed memory, Flink will compute the memory size, for writer according to the weight, the actual memory used depends on the running environment. Now the memory size defined in this property are equals to the exact memory allocated to write buffer in runtime. |

**Use In SQL**
Users can set memory weight in SQL for Flink Managed Memory, then Flink sink operator will get the memory pool size and create allocator for Paimon writer.

```sql
INSERT INTO paimon_table /*+ OPTIONS('sink.use-managed-memory-allocator'='true', 'sink.managed.writer-buffer-memory'='256M') */
SELECT * FROM ....;
```

## Setting dynamic options

When interacting with the Paimon table, table options can be tuned without changing the options in the catalog. Paimon will extract job-level dynamic options and take effect in the current session.
The dynamic option's key format is `paimon.${catalogName}.${dbName}.${tableName}.${config_key}`. The catalogName/dbName/tableName can be `*`, which means matching all the specific parts.

For example:

```sql
-- set scan.timestamp-millis=1697018249000 for the table mycatalog.default.T
SET 'paimon.mycatalog.default.T.scan.timestamp-millis' = '1697018249000';
SELECT * FROM T;

-- set scan.timestamp-millis=1697018249000 for the table default.T in any catalog
SET 'paimon.*.default.T.scan.timestamp-millis' = '1697018249000';
SELECT * FROM T;
```
