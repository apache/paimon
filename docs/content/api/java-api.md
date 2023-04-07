---
title: "Java API"
weight: 1
type: docs
aliases:
- /api/java-api.html
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

# Java API

## Dependency

Maven dependency:

```xml
<dependency>
  <groupId>org.apache.paimon</groupId>
  <artifactId>paimon-bundle</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

Or download the jar file:
{{< stable >}}[Paimon Bundle](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-bundle-{{< version >}}.jar).{{< /stable >}}
{{< unstable >}}[Paimon Bundle](https://repository.apache.org/snapshots/org/apache/paimon/paimon-bundle/{{< version >}}/).{{< /unstable >}}

Paimon relies on Hadoop environment, you should add hadoop classpath or bundled jar.

## Create Catalog

Before coming into contact with the Table, you need to create a Catalog.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

public class CreateCatalog {

    public static void createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("..."));
        Catalog catalog = CatalogFactory.createCatalog(context);
    }

    public static void createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "...");
        options.set("metastore", "hive");
        options.set("uri", "...");
        options.set("hive-conf-dir", "...");
        CatalogContext context = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(context);
    }
}
```

## Create Table

You can use the catalog to create tables. The created tables are persistence in the file system.
Next time you can directly obtain these tables.

```java
import org.apache.paimon.fs.Path;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

public class CreateTable {

    public static void main(String[] args) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("...");
        schemaBuilder.partitionKeys("...");
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("my_db", "my_table");
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}
```

## Get Table

The `Table` interface provides access to the table metadata and tools to read and write table.

```java
import org.apache.paimon.fs.Path;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class GetTable {

    public static void main(String[] args) {
        Identifier identifier = Identifier.create("my_db", "my_table");
        try {
            Table table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // do something
        }
    }
}
```

Table metadata:

- `name` return a name string to identify this table.
- `rowType` return the current row type of this table containing a sequence of table's fields.
- `partitionKeys` returns the partition keys of this table.
- `parimaryKeys` returns the primary keys of this table.
- `options` returns the configuration of this table in a map of key-value.
- `comment` returns the optional comment of this table.
- `copy` return a new table by applying dynamic options to this table.

## Batch Read

For relatively small amounts of data, or for data that has undergone projection and filtering,
you can directly use a standalone program to read the table data.

But if the data volume of the table is relatively large, you can distribute splits to different tasks for reading.

The reading is divided into two stages:

1. Scan Plan: Generate plan splits in a global node ('Coordinator', or named 'Driver').
2. Read Split: Read split in distributed tasks.

```java
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import java.io.IOException;
import java.util.List;

public class ReadTable {

    public static void main(String[] args) {
        // 1. Create a ReadBuilder and push filter (`withFilter`)
        // and projection (`withProjection`) if necessary
        ReadBuilder readBuilder = table.newReadBuilder()
                .withProjection(projection)
                .withFilter(filter);

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        List<Split> splits = readBuilder.newScan().plan().splits();

        // 3. Distribute these splits to different tasks

        // 4. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(ReadTable::readRow);
    }
}
```

## Batch Write

The writing is divided into two stages:

1. Write records: Write records in distributed tasks, generate commit messages.
2. Commit: Collect all CommitMessages, commit them in a global node ('Coordinator', or named 'Driver', or named 'Committer').

```java
import java.util.List;

import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

public class WriteTable {

    public static void main(String[] args) {
        // 1. Create a WriteBuilder (Serializable)
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder()
                .withOverwrite(staticPartition);

        // 2. Write records in distributed tasks
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(record1);
        write.write(record2);
        write.write(record3);
        List<CommitMessage> messages = write.prepareCommit();

        // 3. Collect all CommitMessages to a global node and commit
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
    }
}
```

## Stream Read

The difference of Stream Read is that StreamTableScan can continuously scan and generate splits.

StreamTableScan provides the ability to checkpoint and restore, which can let you save the correct state
during stream reading.

```java
import java.io.IOException;
import java.util.List;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;

public class StreamReadTable {

    public static void main(String[] args) throws IOException {
        // 1. Create a ReadBuilder and push filter (`withFilter`)
        // and projection (`withProjection`) if necessary
        ReadBuilder readBuilder = table.newReadBuilder()
                .withProjection(projection)
                .withFilter(filter);

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        StreamTableScan scan = readBuilder.newStreamScan();
        while (true) {
            List<Split> splits = scan.plan().splits();
            // Distribute these splits to different tasks

            Long state = scan.checkpoint();
            // can be restored in scan.restore(state) after failover
        }

        // 3. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(row -> System.out.println(row));
    }
}
```

## Stream Write

The difference of Stream Write is that StreamTableCommit can continuously commit.

Key points to achieve exactly-once consistency:

- CommitUser represents a user. A user can commit multiple times. In distributed processing, you are
  expected to use the same commitUser.
- Different applications need to use different commitUsers.
- The commitIdentifier of `StreamTableWrite` and `StreamTableCommit` needs to be consistent, and the
  id needs to be incremented for the next committing.
- When a failure occurs, if you still have uncommitted `CommitMessage`s, please use `StreamTableCommit#filterCommitted`
  to exclude the committed messages by commitIdentifier.

```java
import java.util.List;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

public class StreamWriteTable {

    public static void main(String[] args) throws Exception {
        // 1. Create a WriteBuilder (Serializable)
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();

        // 2. Write records in distributed tasks
        StreamTableWrite write = writeBuilder.newWrite();
        // commitIdentifier like Flink checkpointId
        long commitIdentifier = 0;
        while (true) {
            write.write(record1);
            write.write(record2);
            write.write(record3);
            List<CommitMessage> messages = write.prepareCommit(
                    false, commitIdentifier);
            commitIdentifier++;
        }

        // 3. Collect all CommitMessages to a global node and commit
        StreamTableCommit commit = writeBuilder.newCommit();
        commit.commit(commitIdentifier, messages);

        // 4. When failover, you can use 'filterCommitted' to filter committed commits.
        commit.filterCommitted(committedIdentifiers);
    }
}
```
