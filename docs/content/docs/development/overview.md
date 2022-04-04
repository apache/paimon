---
title: "Overview"
weight: 1
type: docs
aliases:
- /development/overview.html
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

# Overview

Flink Table Store is a unified streaming and batch store for building dynamic
tables on Apache Flink. Flink Table Store serves as the storage engine behind
Flink SQL Managed Table.

## Setup Table Store

{{< hint info >}}
__Note:__ Table Store is only supported since Flink 1.15.
{{< /hint >}}

You can get the bundle jar for the Table Store in one of the following ways:
- [Download the latest bundle jar](https://flink.apache.org/downloads.html) of
  Flink Table Store.
- Build bundle jar under submodule `flink-table-store-dist` from source code.

Flink Table Store has shaded all the dependencies in the package, so you don't have
to worry about conflicts with other connector dependencies.

The steps to set up are:
- Copy the Table Store bundle jar to `flink/lib`.
- Setting the HADOOP_CLASSPATH environment variable or copy the
  [Pre-bundled Hadoop Jar](https://flink.apache.org/downloads.html) to `flink/lib`.

## Managed Table

The typical usage of Flink SQL DDL is to specify the 'connector' and fill in
the complex connection information in 'with'. The DDL just establishes an implicit
relationship with the external system. We call such Table as external table.

```sql
-- an external table ddl
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
```

The managed table is different, the connection information is already
filled in the session environment, the user only needs to focus on the
business logic when writing the table creation DDL. The DDL is no longer
just an implicit relationship; creating a table will create the corresponding
physical storage, and dropping a table will delete the corresponding
physical storage.

```sql
-- a managed table ddl
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
);
```

## Unify Streaming and Batch

There are three types of connectors in Flink SQL.
- Message queue, such as Apache Kafka, it is used in both source and 
  intermediate stages in this pipeline, to guarantee the latency stay
  within seconds.
- OLAP system, such as Clickhouse, it receives processed data in
  streaming fashion and serving user’s ad-hoc queries. 
- Batch storage, such as Apache Hive, it supports various operations
  of the traditional batch processing, including `INSERT OVERWRITE`.

Flink Table Store provides table abstraction. It is used in a way that
does not differ from the traditional database:
- In Flink `batch` execution mode, it acts like a Hive table and
  supports various operations of Batch SQL. Query it to see the
  latest snapshot.
- In Flink `streaming` execution mode, it acts like a message queue.
  Query it acts like querying a stream changelog from a message queue
  where historical data never expires.

Different `log.scan` mode will result in different consuming behavior under streaming mode.
<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scan Mode</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>FULL</h5></td>
      <td>Yes</td>
      <td>FULL scan mode performs a hybrid reading with a snapshot scan and the continuous incremental scan.</td>
    </tr>
    <tr>
      <td><h5>LATEST</h5></td>
      <td>No</td>
      <td>LATEST scan mode only reads incremental data from the latest offset.</td>
    </tr>
    </tbody>
</table>

## Architecture

Flink Table Store consists of two parts, LogStore and FileStore. The
LogStore would serve the need of message systems, while FileStore will
play the role of file systems with columnar formats. At each point in time,
LogStore and FileStore will store exactly the same data for the latest
written data (LogStore has TTL), but with different physical layouts.
Flink Table Store aims to bridge the storage layout gap between the
batch table and streaming changelog, to provide a unified experience
as Flink SQL:
- LogStore: Store the latest data, support second level streaming incremental
consumption, use Kafka by default.
- FileStore: Store latest data + historical data, provide batch Ad-Hoc analysis.

{{< img src="/img/architecture.svg" alt="Flink Table Store Architecture" >}}

The manifest file is used to record changes to the SST file, and multiple
manifest files make up a snapshot.

The data in the FileStore is divided into buckets, each bucket is a
separate LSM (log structured merge tree).

The file inside LSM is called SST (Sorted Strings Table). By default, files
are stored in columnar format (Apache ORC) for high performance of analysis
and compression of storage.

