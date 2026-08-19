---
title: "Structured Streaming"
sidebar_position: 11
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

:::info

Paimon Structured Streaming only supports the two `append` and `complete` modes.

:::

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

Streaming write also supports [Write merge schema](./sql-write#write-merge-schema).

## Streaming Query

:::info

Paimon currently supports Spark 3.3+ for streaming read.

:::

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

### Consumer progress

You can assign a Paimon Consumer to a streaming query. The Consumer records a
table-side, snapshot-level recovery position and acts as a fence during normal
snapshot expiration. For tables with a decoupled changelog lifecycle, enabling
`consumer.changelog-only` makes the Consumer protect long-lived changelogs
instead of snapshots.

Configure the Consumer lifetime as a table property before starting the query:

```sql
ALTER TABLE table_name SET TBLPROPERTIES (
  'consumer.expiration-time' = '1 d'
);
```

```scala
val query = spark.readStream
  .format("paimon")
  .option("consumer-id", "my-consumer")
  .table("table_name")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/spark/checkpoint")
  .start()
```

Spark checkpoint and Paimon Consumer progress have different granularities:

- When the Spark checkpoint is available, Spark uses it for precise micro-batch
  recovery.
- A new query without that checkpoint starts from the Consumer position. This
  recovery is conservative and may replay a completed snapshot if the query
  failed after processing it but before updating the Consumer.

Paimon updates the Consumer only from Spark's successful micro-batch commit
callback. When a micro-batch ends partway through a delta snapshot, the Consumer
may be created or refreshed at that snapshot so that the whole snapshot remains
protected and can be replayed. The Consumer advances past a snapshot only after
the micro-batch containing its last split is committed. A Consumer update failure
is propagated, fails the running query, and leaves the Consumer at its previous
conservative position.

Spark never advances Consumer progress past an incomplete snapshot. An incomplete
initial full snapshot does not create a Consumer because Consumer recovery from
that snapshot would use a delta scan. `consumer.mode` does not select a different
Spark source implementation.

Offsets restored from an older Spark checkpoint do not contain snapshot
completion metadata. Spark logs a warning and leaves the Consumer unchanged for
those offsets rather than advancing it without proof that the snapshot finished.

If the Consumer does not exist, the configured startup options are used. For
example, the default `latest-full` mode first reads the full snapshot and creates
the Consumer only after that snapshot is completely committed. If the Consumer
already exists, its next snapshot takes precedence over startup options and is
read incrementally, unless `consumer.ignore-progress` is enabled.

:::note

Spark can invoke the source commit callback while constructing a later
micro-batch. Therefore the Consumer may temporarily lag behind the latest
successful batch, especially while a query is idle or after its final available
batch. This is safe and can only cause conservative replay.

:::

Consumer files are scoped to the branch being read. Use one active Spark query
for each `(table, branch, consumer-id)` combination. Concurrent queries sharing
the same Consumer are unsupported because a faster query could move the shared
position past data still needed by a slower query.

Configure `consumer.expiration-time` according to the longest expected snapshot
processing time, failure recovery time, and idle interval. Eligible Spark source
commit callbacks refresh the Consumer file, but there is no separate timer-based
Spark heartbeat. Expired Consumer files are cleaned by non-write-only table
commits. A write-only writer does not perform Consumer expiration, so stale
Consumer IDs must be managed explicitly or by a separate non-write-only
maintenance process.

An initial full scan has no Consumer retention fence until it completes, so
snapshot retention should be long enough for that scan. If its snapshot expires
first, a restart without a Spark checkpoint performs a new initial full scan
according to the startup options.

When expired data would force recovery of a logged Spark batch to switch between
a full and an incremental scan, Paimon fails the query instead of combining the
two plans. Start a new query with a new checkpoint to apply the startup options
again.

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

### Written Columns of a Micro-Batch

`foreachBatch` consumers can inspect which Paimon field IDs were written by the data files admitted to the current micro-batch. Call `PaimonSparkMicroBatchMetadata.writtenColumnIds` with the raw `Dataset` passed to `foreachBatch`. Paimon resolves the file metadata lazily when this method is called.

```scala
import org.apache.paimon.spark.PaimonSparkMicroBatchMetadata
import org.apache.spark.sql.{Dataset, Row}

val query = spark.readStream
  .format("paimon")
  .table("table_name")
  .writeStream
  .option("checkpointLocation", "/path/to/checkpoint")
  .foreachBatch { (batch: Dataset[Row], _: Long) =>
    val writtenColumnIds = PaimonSparkMicroBatchMetadata.writtenColumnIds(batch)
    if (!writtenColumnIds.isPresent) {
      // Metadata is unavailable; conservatively process all columns.
    } else {
      val fieldIds = writtenColumnIds.get()
      // Process the exact set of written Paimon field IDs.
    }
  }
  .start()
```

A present `Optional` contains the complete, immutable list of written field IDs in ascending order. The list may be empty; that is a known empty set, not unknown metadata.

An empty `Optional` means that metadata is unavailable, for example because a file or schema cannot be resolved, the micro-batch is empty, the `Dataset` is not the raw batch from a query with exactly one distinct Paimon streaming source, or its lineage is incomplete or ambiguous. An empty `Optional` does not mean that no columns were written; callers must fall back to processing all columns.

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
