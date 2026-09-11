---
title: "Java Reads"
sidebar_position: 3
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

# Java Reads

A read has two stages: a scan plans **splits**, and readers consume those splits. A standalone
application can do both. A distributed engine plans centrally and assigns each split to a reader task.

The examples use `CreateCatalog` and `my_db.my_table` from the [Java API setup](java-api).
Populate the table with the [batch write example](java-writing#batch-write) first.

![A coordinator plans splits with ReadBuilder; worker readers consume assigned splits using the same filter and projection.](/img/program-api-read-flow.svg)

## Batch Read

Build predicates against the table's full row type. Filter field indexes refer to that schema;
projection selects the columns returned by the reader. This example returns `f0` and `f1` for
records whose `f0` is not null and whose `f1` is at least 12.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import java.util.Arrays;
import java.util.List;

public class ReadTable {

    public static void main(String[] args) throws Exception {
        try (Catalog catalog = CreateCatalog.createFilesystemCatalog()) {
            Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
            PredicateBuilder predicates = new PredicateBuilder(table.rowType());
            ReadBuilder readBuilder = table.newReadBuilder()
                    .withFilter(Arrays.asList(
                            predicates.isNotNull(0), predicates.greaterOrEqual(1, 12)))
                    .withProjection(new int[] {0, 1});

            // Plan once in the coordinator; assign splits to workers if distributed.
            List<Split> splits = readBuilder.newScan().plan().splits();
            TableRead read = readBuilder.newRead().executeFilter();
            try (RecordReader<InternalRow> reader = read.createReader(splits)) {
                reader.forEachRemaining(row ->
                        System.out.println(row.getString(0) + ", " + row.getInt(1)));
            }
        }
    }
}
```

After the batch write example, the result contains `Alice, 12` and `Emily, 18`; row order is not
guaranteed. `withFilter` enables pruning, which can leave nonmatching rows in candidate files.
`executeFilter()` also evaluates the filter on individual records.

## Reader resources and row reuse

Close readers even if you stop before reaching the end. `forEachRemaining` consumes and releases
batches and closes the reader. If you use `readBatch()` directly, call `releaseBatch()` for every
returned batch and close the reader when done.

Readers can reuse rows and their backing memory. Consume values before advancing, or copy the
values you need to retain. See [Types and Predicates](java-types) for internal value representations.

## Stream Read

`newStreamScan()` plans successive snapshots. Startup options determine the initial scan;
subsequent calls discover new changes. See [streaming reads](../primary-key-table/table-mode)
and [scan configuration](../maintenance/configurations) when selecting a table's read mode.

This example is a single-process polling loop. It prints rows and their `RowKind`; it is not a
complete checkpointed pipeline.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;

import java.util.List;

public class StreamReadTable {

    public static void main(String[] args) throws Exception {
        try (Catalog catalog = CreateCatalog.createFilesystemCatalog()) {
            Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
            ReadBuilder readBuilder = table.newReadBuilder();
            StreamTableScan scan = readBuilder.newStreamScan();
            // On recovery, call scan.restore(savedNextSnapshotId) before planning.
            TableRead read = readBuilder.newRead();

            while (!Thread.currentThread().isInterrupted()) {
                List<Split> splits = scan.plan().splits();
                if (!splits.isEmpty()) {
                    try (RecordReader<InternalRow> reader = read.createReader(splits)) {
                        reader.forEachRemaining(row -> System.out.println(
                                row.getRowKind() + ": " + row.getString(0) + ", " + row.getInt(1)));
                    }
                }

                Long nextSnapshotId = scan.checkpoint();
                // Persist this position together with durable downstream progress.
                // Printing to stdout above is not a durable checkpoint.
                Thread.sleep(1000);
            }
        }
    }
}
```

### Checkpoint and restore

`scan.checkpoint()` returns the **next snapshot ID to plan**. It does not contain the planned
splits, reader offsets, or downstream state.

- In a sequential reader, finish the planned work and coordinate its durable downstream result
  with the saved scan position.
- In a distributed reader, checkpoint pending splits and reader progress as well as the scan
  position. Restoring only the scan position can skip work already planned but not yet read.
- Restore the saved position with `scan.restore(nextSnapshotId)` before planning again. Notify the
  scan with `notifyCheckpointComplete(nextSnapshotId)` only after the corresponding checkpoint
  has completed, when integrating with checkpoint/consumer tracking.

Choose [snapshot retention](../maintenance/manage-snapshots) that leaves enough time for consumers
to read and recover. The scan API alone does not provide end-to-end exactly-once processing.

## Adjust Read Batch Size at Runtime

Parquet and ORC readers can share a `ReadBatchSizer` to adjust the row count and vector
capacity of future physical batches without recreating readers. This fragment uses the
`readBuilder` and `splits` created in the batch read example:

```java
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.TableRead;

ReadBatchSizer sizer = new ReadBatchSizer();
TableRead read = readBuilder.newRead().withReadBatchSizer(sizer);
try (RecordReader<InternalRow> reader = read.createReader(splits)) {
    sizer.setBatchSize(256);
    // Consume batches with reader.readBatch() and release each batch after use.
}
```

Configure the sizer on `TableRead` before creating readers. A newly created sizer has no batch size,
so readers initially use their configured default. Every explicitly set batch size must be positive.
Call `clearBatchSize()` to make future physical batches use the configured default again.

A supporting reader snapshots the batch size before starting a physical batch. If the size has
changed, it replaces an idle reusable batch with vectors sized for the new value and then starts the
read. The allocation is reused until the batch size changes again. Consequently, lowering the
batch size reduces the vector capacity of future batches instead of only changing their logical
row count.

An update never mutates a batch that has already started or is still owned by a consumer.
Asynchronously prefetched batches may therefore retain the previous size. For a pooled ORC reader,
each idle pool entry adopts the current size the next time it is acquired, while in-flight entries
keep their old vectors until released. During such a transition, old and new vectors can coexist.

The sizer uses latest-value semantics: when updates happen faster than physical batches start,
readers may skip intermediate batch sizes. Engines should avoid changing the size too frequently
because each observed size change reallocates vectors and can add allocation and garbage-collection
overhead. A hysteresis interval or minimum adjustment period is recommended.

For concurrent scans, estimate memory using the selected batch size multiplied by the number of
active and prefetched batches. This allows an engine to reduce future batch capacities under memory
pressure and grow them again when more memory is available.

## Next steps

Use [Java Writes](java-writing) to publish data, [Types and Predicates](java-types) to construct
filters, and [Local Cache](file-cache) to reduce repeated file reads.
