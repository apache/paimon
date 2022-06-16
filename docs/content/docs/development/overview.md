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

Flink Table Store is a unified storage to build dynamic tables for both streaming and
batch processing in Flink, supporting high speed data ingestion and timely data query.

## Architecture

<center>
<img src="/img/architecture.png" width="100%"/>
</center>

As shown in the architecture above:

* Users can use Flink to insert data into the Table Store, either by streaming the change log
  captured from databases, or by loading the data in batches from the other stores like data warehouses.
* Users can use Flink to query the table store in different ways, including streaming queries and
  Batch/OLAP queries. It is also worth noting that users can use other engines such as Apache Hive to
  query from the table store as well.
* Under the hood, table Store uses a hybrid storage architecture, using a Lake Store to store historical data
  and a Queue system (Apache Kafka integration is currently supported) to store incremental data. It provides
  incremental snapshots for hybrid streaming reads.
* Table Store's Lake Store stores data as columnar files on file system / object store, and uses the LSM Structure
  to support a large amount of data updates and high-performance queries.

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

## Unified Table

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
