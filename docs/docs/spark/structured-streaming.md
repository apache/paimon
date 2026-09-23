---
title: "Structured Streaming"
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

Read or write Paimon tables with Spark Structured Streaming. Configure the
[catalog and extensions](./quick-start#setup) first. Streaming reads require **Spark 3.3 or later**.
The examples use Scala and micro-batch execution.

## Streaming Write

The sink supports `append` and `complete` output modes. The following example uses Spark's
public `rate` source and writes to the location of a catalog table; run it in `spark-shell` configured as in
[Quick Start](./quick-start#setup).

```scala
import org.apache.spark.sql.functions.col

spark.sql("USE paimon.default")
spark.sql("""
  CREATE TABLE IF NOT EXISTS stream_events (id BIGINT, event_time TIMESTAMP)
  TBLPROPERTIES ('bucket' = '-1')
""")

val input = spark.readStream
  .format("rate")
  .option("rowsPerSecond", "10")
  .load()
  .select(col("value").as("id"), col("timestamp").as("event_time"))

val writer = input.writeStream
  .format("paimon")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/paimon-checkpoints/stream-events-writer")
  .start("file:/tmp/paimon/default.db/stream_events")
```

The sink uses `.start(tableLocation)` to access an existing table. `writeStream.toTable(...)` is
not supported for Paimon catalog tables. This path matches the local warehouse in Quick Start;
replace it with your table's actual location for another warehouse.

`append` applies each batch to the table. `complete` overwrites the table with each batch
and should be used only when the input represents the complete result.

Use a durable checkpoint location accessible to the cluster for deployed jobs. Give each query
its own checkpoint directory. Stop this example with `writer.stop()`.
Streaming writes also support [Schema Evolution on Write](./schema-evolution).

## Streaming Query

:::info

Paimon currently supports Spark 3.3+ for streaming read.

:::

Choose the startup scan mode for a new query. Once a Spark checkpoint or Consumer position
exists, recovery follows [Streaming Recovery](./streaming-recovery).

| `scan.mode` | Initial read | Then |
| --- | --- | --- |
| `latest` | No initial snapshot | Read new changes. |
| `latest-full` | Read the latest full snapshot | Read subsequent changes. |
| `from-timestamp` | Changes starting from `scan.timestamp-millis` | Continue reading changes. |
| `from-snapshot` | Changes starting from `scan.snapshot-id` | Continue reading changes. |
| `from-snapshot-full` | Full snapshot at `scan.snapshot-id` | Read subsequent changes. |
| `default` | Infer from `scan.snapshot-id`, `scan.timestamp-millis`, or `scan.timestamp`; otherwise use `latest-full` | Continue reading changes. |

A simple example with default scan mode:

```scala
// With no startup options or saved progress, use latest-full.
val query = spark.readStream
  .format("paimon")
  // by table name
  .table("paimon.default.stream_events")
  // or by location
  // .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/tmp/paimon-checkpoints/stream-events-reader")
  .start()
```

For primary key changes that include row kinds, see [Read Changelogs](#read-changelogs).

### Consumer progress

See [Streaming Recovery](./streaming-recovery) for checkpoints, Consumer positions, replay, and
retention. A Consumer is not a replacement for a Spark checkpoint.

## Triggers and Read Limits

Limit the amount of input admitted to each micro-batch with these source options. Admission
works on whole Paimon splits: a split can contain multiple files, and the byte or row threshold
can be exceeded by the last admitted split. These settings do not impose exact output row or
memory limits.

| Source option | Default | Meaning |
| --- | --- | --- |
| `read.stream.maxFilesPerTrigger` | Unset | Maximum admitted splits; the option name refers to files. |
| `read.stream.maxBytesPerTrigger` | Unset | Soft threshold on admitted file bytes. |
| `read.stream.maxRowsPerTrigger` | Unset | Soft threshold on admitted file row counts. |
| `read.stream.minRowsPerTrigger` | Unset | Row-count target for delaying admission; use with `read.stream.maxTriggerDelayMs`. |
| `read.stream.maxTriggerDelayMs` | Unset | Maximum waiting time in milliseconds when using the minimum-row limit. |

### Process Available Data and Stop

`Trigger.AvailableNow()` processes data available when the query starts, in one or more batches,
then stops. This example uses a 128 MiB admission threshold; a batch may exceed it by a split.

```scala
import org.apache.spark.sql.streaming.Trigger

val query = spark.readStream
  .format("paimon")
  .option("read.stream.maxBytesPerTrigger", "134217728")
  .table("table_name")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/checkpoints/available-now")
  .trigger(Trigger.AvailableNow())
  .start()
```

### Wait for More Rows

Use a 5,000-row target with a maximum delay of 300 seconds:

```scala
val query = spark.readStream
  .format("paimon")
  .option("read.stream.minRowsPerTrigger", "5000")
  .option("read.stream.maxTriggerDelayMs", "300000")
  .table("table_name")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/checkpoints/min-rows")
  .start()
```

## Written Columns of a Micro-Batch

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

## Read Changelogs

Expose a row-kind column to distinguish inserts, updates, and deletes. Use either the system
`$audit_log` table through the catalog, or `read.changelog=true` when reading by table location.
Choose one source form:

```scala
// Catalog table: use the audit_log system table.
val changesByName = spark.readStream
  .format("paimon")
  .table("`table_name$audit_log`")

// Table location: read.changelog is applied by the path-based source.
val changesByPath = spark.readStream
  .format("paimon")
  .option("read.changelog", "true")
  .load("/path/to/paimon/source/table")

val query = changesByName.writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/checkpoints/changelog")
  .start()
```

This exposes the available changes; it does not configure the table to produce a complete
changelog. See [Changelog Producers](../primary-key-table/changelog-producer) for production modes.
