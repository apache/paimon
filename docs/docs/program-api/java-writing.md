---
title: "Java Writes"
sidebar_position: 4
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

# Java Writes

Writing records creates files; committing makes the file changes visible through table snapshots.
A distributed job writes in worker tasks, gathers their `CommitMessage`s, and commits centrally.

The examples use `CreateCatalog` and the two-column primary-key table from the
[Java API setup](java-api). Java strings must be converted to `BinaryString` before writing.

![Workers prepare file changes for commit ID N. A coordinator collects all messages and commits with the same ID; recovery retries saved messages.](/img/program-api-write-flow.svg)

## Batch Write

Create a `BatchWriteBuilder`, write records, prepare messages, and commit the collected messages
once. This example upserts three rows into the sample primary-key table.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;

public class BatchWrite {

    public static void main(String[] args) throws Exception {
        try (Catalog catalog = CreateCatalog.createFilesystemCatalog()) {
            Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                write.write(GenericRow.of(BinaryString.fromString("Alice"), 12));
                write.write(GenericRow.of(BinaryString.fromString("Bob"), 5));
                write.write(GenericRow.of(BinaryString.fromString("Emily"), 18));

                List<CommitMessage> messages = write.prepareCommit();
                // In a distributed job, collect messages from every writer first.
                commit.commit(messages);
            }
        }
    }
}
```

Closing a writer does not publish its data. Call `prepareCommit()` and commit the resulting
messages before closing it. A batch committer is for one commit; use a new builder for another batch.

### Overwrite and abort

Use `table.newBatchWriteBuilder().withOverwrite()` only when you intend to replace existing data.
For a partitioned table, check the overwrite scope and table options before selecting this mode;
see [overwrite behavior](../flink/sql-write#overwriting-the-whole-table).

`BatchTableCommit.abort(messages)` deletes files belonging to an abandoned write. Use it only
for messages that are known not to have been committed and will not be retried. A commit exception
alone does not prove that publishing failed; do not delete files from a possibly successful commit.

## Stream Write

A `StreamTableWrite` and `StreamTableCommit` can be reused across commits. The application supplies
two identities:

| Identity | Rule |
| --- | --- |
| `commitUser` | Shared by writers and committer of one application, stable across recovery, different for independent applications. |
| `commitIdentifier` | The same value for prepare and commit, increasing for subsequent commits. A checkpoint ID is a common choice. |

The default commit user is random. Set it explicitly, or persist and restore the generated value,
when implementing recovery.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.util.List;

public class StreamWriteTable {

    public static void main(String[] args) throws Exception {
        // Supply a unique identity for a new application; retain it on recovery.
        String commitUser = args[0];
        try (Catalog catalog = CreateCatalog.createFilesystemCatalog()) {
            Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
            StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder()
                    .withCommitUser(commitUser);
            try (StreamTableWrite write = writeBuilder.newWrite();
                    StreamTableCommit commit = writeBuilder.newCommit()) {
                // This finite example starts a NEW application without recovered state.
                for (long commitIdentifier = 0; commitIdentifier < 3; commitIdentifier++) {
                    write.write(GenericRow.of(
                            BinaryString.fromString("Alice"), 12 + (int) commitIdentifier));
                    List<CommitMessage> messages =
                            write.prepareCommit(false, commitIdentifier);

                    // A recoverable pipeline saves messages and input progress durably here.
                    commit.commit(commitIdentifier, messages);
                    // The loop advances the ID only after this commit succeeds.
                }
            }
        }
    }
}
```

Pass an application identity as the first argument. This finite example demonstrates matching
prepare/commit IDs; it does not persist checkpoints. Do not restart it from ID 0 with a previously
used identity and treat it as recovery.

### Recover a pending commit

Save the commit user, commit IDs, prepared messages, and associated input progress in your
runtime's checkpoint state. After restoring the same commit user, retry saved messages with
`filterAndCommit` if the outcome of an earlier commit is uncertain:

```java
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// restoredCommitIdentifier and restoredMessages come from durable checkpoint state.
Map<Long, List<CommitMessage>> pending = new HashMap<>();
pending.put(restoredCommitIdentifier, restoredMessages);
try (StreamTableCommit commit = writeBuilder.newCommit()) {
    commit.filterAndCommit(pending);
}
```

`commit` does not check whether an ID was already committed. `filterAndCommit` filters previously
committed IDs and publishes the remaining messages. Resume new writes with the next ID after
reconciling pending commits. This protocol must be coordinated with source replay and writer
recovery to provide end-to-end exactly-once behavior.

## Distributed writer routing

Create one logical write builder for the operation and use it for writers and the committer.
For the fixed-bucket table used here, use `writeBuilder.newWriteSelector()` to determine which
downstream writer should receive a record. Records for the same bucket must be routed consistently.
Bucket-unaware and postpone modes return no selector. Dynamic-bucket modes do not support this
selector API: they require dedicated bucket assignment and `write(row, bucket)` instead. See
[data distribution](../primary-key-table/data-distribution) when selecting a different table layout.

For a Flink job, use [FlinkSinkBuilder](flink-api#write-to-table) to integrate routing, checkpoints,
and commits with the engine.
