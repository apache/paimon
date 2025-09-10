---
title: "Structured Streaming"
weight: 11
type: docs
aliases:
- /spark/structured-streaming.html
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

# Structured Streaming

Paimon supports streaming data processing with [Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html), enabling both streaming write and streaming query.

## Streaming Write

{{< hint info >}}

Paimon Structured Streaming only supports the two `append` and `complete` modes.

{{< /hint >}}

```scala
// Create a paimon table if not exists.
spark.sql(s"""
           |CREATE TABLE T (k INT, v STRING)
           |TBLPROPERTIES ('primary-key'='k', 'bucket'='3')
           |""".stripMargin)

// Here we use MemoryStream to fake a streaming source.
val inputData = MemoryStream[(Int, String)]
val df = inputData.toDS().toDF("k", "v")

// Streaming Write to paimon table.
val stream = df
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "/path/to/checkpoint")
  .format("paimon")
  .start("/path/to/paimon/sink/table")
```

Streaming write also supports [Write merge schema]({{< ref "spark/sql-write#write-merge-schema" >}}).

## Streaming Query

{{< hint info >}}

Paimon currently supports Spark 3.3+ for streaming read.

{{< /hint >}}

Paimon supports rich scan mode for streaming read. There is a list:
<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Scan Mode</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>latest</h5></td>
            <td>For streaming sources, continuously reads latest changes without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>latest-full</h5></td>
            <td>For streaming sources, produces the latest snapshot on the table upon first startup, and continue to read the latest changes.</td>
        </tr>
        <tr>
            <td><h5>from-timestamp</h5></td>
            <td>For streaming sources, continuously reads changes starting from timestamp specified by "scan.timestamp-millis", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot</h5></td>
            <td>For streaming sources, continuously reads changes starting from snapshot specified by "scan.snapshot-id", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot-full</h5></td>
            <td>For streaming sources, produces from snapshot specified by "scan.snapshot-id" on the table upon first startup, and continuously reads changes.</td>
        </tr>
        <tr>
            <td><h5>default</h5></td>
            <td>It is equivalent to from-snapshot if "scan.snapshot-id" is specified. It is equivalent to from-timestamp if "timestamp-millis" is specified. Or, It is equivalent to latest-full.</td>
        </tr>
    </tbody>
</table>

A simple example with default scan mode:

```scala
// no any scan-related configs are provided, that will use latest-full scan mode.
val query = spark.readStream
  .format("paimon")
  // by table name
  .table("table_name") 
  // or by location
  // .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()
```

Paimon Structured Streaming also supports a variety of streaming read modes, it can support many triggers and many read limits.

These read limits are supported:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>read.stream.maxFilesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>The maximum number of files returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxBytesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of bytes returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of rows returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.minRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The minimum number of rows returned in a single batch, which used to create MinRowsReadLimit with read.stream.maxTriggerDelayMs together.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxTriggerDelayMs</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum delay between two adjacent batches, which used to create MinRowsReadLimit with read.stream.minRowsPerTrigger together.</td>
        </tr>
    </tbody>
</table>

**Example: One**

Use `org.apache.spark.sql.streaming.Trigger.AvailableNow()` and `maxBytesPerTrigger` defined by paimon.

```scala
// Trigger.AvailableNow()) processes all available data at the start
// of the query in one or multiple batches, then terminates the query.
// That set read.stream.maxBytesPerTrigger to 128M means that each
// batch processes a maximum of 128 MB of data.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.maxBytesPerTrigger", "134217728")
  .table("table_name")
  .writeStream
  .format("console")
  .trigger(Trigger.AvailableNow())
  .start()
```

**Example: Two**

Use `org.apache.spark.sql.connector.read.streaming.ReadMinRows`.

```scala
// It will not trigger a batch until there are more than 5,000 pieces of data,
// unless the interval between the two batches is more than 300 seconds.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.minRowsPerTrigger", "5000")
  .option("read.stream.maxTriggerDelayMs", "300000")
  .table("table_name")
  .writeStream
  .format("console")
  .start()
```

Paimon Structured Streaming supports read row in the form of changelog (add rowkind column in row to represent its
change type) in two ways:

- Direct streaming read with the system audit_log table
- Set `read.changelog` to true (default is false), then streaming read with table location

**Example:**

```scala
// Option 1
val query1 = spark.readStream
  .format("paimon")
  .table("`table_name$audit_log`")
  .writeStream
  .format("console")
  .start()

// Option 2
val query2 = spark.readStream
  .format("paimon")
  .option("read.changelog", "true")
  .table("table_name")
  .writeStream
  .format("console")
  .start()

/*
+I   1  Hi
+I   2  Hello
*/
```
