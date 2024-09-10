---
title: "Java API"
weight: 2
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

{{< hint warning >}}
We do not recommend using the Paimon API naked, unless you are a professional downstream ecosystem developer, and even if you do, there will be significant difficulties.

If you are only using Paimon, we strongly recommend using computing engines such as Flink SQL or Spark SQL.

The following documents are not detailed and are for reference only.
{{< /hint >}}

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
{{< stable >}}[Paimon Bundle](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/{{< version >}}/paimon-bundle-{{< version >}}.jar).{{< /stable >}}
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

    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("..."));
        return CatalogFactory.createCatalog(context);
    }

    public static Catalog createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "...");
        options.set("metastore", "hive");
        options.set("uri", "...");
        options.set("hive-conf-dir", "...");
        options.set("hadoop-conf-dir", "...");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}
```

## Create Table

You can use the catalog to create tables. The created tables are persistence in the file system.
Next time you can directly obtain these tables.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

public class CreateTable {

    public static void main(String[] args) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("f0", "f1");
        schemaBuilder.partitionKeys("f1");
        schemaBuilder.column("f0", DataTypes.STRING());
        schemaBuilder.column("f1", DataTypes.INT());
        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("my_db", "my_table");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

public class GetTable {

    public static Table getTable() {
        Identifier identifier = Identifier.create("my_db", "my_table");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            return catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }
}
```

## Batch Read

For relatively small amounts of data, or for data that has undergone projection and filtering,
you can directly use a standalone program to read the table data.

But if the data volume of the table is relatively large, you can distribute splits to different tasks for reading.

The reading is divided into two stages:

1. Scan Plan: Generate plan splits in a global node ('Coordinator', or named 'Driver').
2. Read Split: Read split in distributed tasks.

```java
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.common.collect.Lists;

import java.util.List;

public class ReadTable {

    public static void main(String[] args) throws Exception {
        // 1. Create a ReadBuilder and push filter (`withFilter`)
        // and projection (`withProjection`) if necessary
        Table table = GetTable.getTable();

        PredicateBuilder builder =
            new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        Predicate notNull = builder.isNotNull(0);
        Predicate greaterOrEqual = builder.greaterOrEqual(1, 12);

        int[] projection = new int[] {0, 1};

        ReadBuilder readBuilder =
            table.newReadBuilder()
                .withProjection(projection)
                .withFilter(Lists.newArrayList(notNull, greaterOrEqual));

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        List<Split> splits = readBuilder.newScan().plan().splits();

        // 3. Distribute these splits to different tasks

        // 4. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(System.out::println);
    }
}
```

## Batch Write

The writing is divided into two stages:

1. Write records: Write records in distributed tasks, generate commit messages.
2. Commit/Abort: Collect all CommitMessages, commit them in a global node ('Coordinator', or named 'Driver', or named 'Committer').
   When the commit fails for certain reason, abort unsuccessful commit via CommitMessages.

```java
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
        // 1. Create a WriteBuilder (Serializable)
        Table table = GetTable.getTable();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();

        // 2. Write records in distributed tasks
        BatchTableWrite write = writeBuilder.newWrite();

        GenericRow record1 = GenericRow.of(BinaryString.fromString("Alice"), 12);
        GenericRow record2 = GenericRow.of(BinaryString.fromString("Bob"), 5);
        GenericRow record3 = GenericRow.of(BinaryString.fromString("Emily"), 18);

        // If this is a distributed write, you can use writeBuilder.newWriteSelector.
        // WriteSelector determines to which logical downstream writers a record should be written to.
        // If it returns empty, no data distribution is required.

        write.write(record1);
        write.write(record2);
        write.write(record3);

        List<CommitMessage> messages = write.prepareCommit();

        // 3. Collect all CommitMessages to a global node and commit
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);

        // Abort unsuccessful commit to delete data files
        // commit.abort(messages);
    }
}
```

## Stream Read

The difference of Stream Read is that StreamTableScan can continuously scan and generate splits.

StreamTableScan provides the ability to checkpoint and restore, which can let you save the correct state
during stream reading.

```java
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.common.collect.Lists;

import java.util.List;

public class StreamReadTable {

    public static void main(String[] args) throws Exception {
        // 1. Create a ReadBuilder and push filter (`withFilter`)
        // and projection (`withProjection`) if necessary
        Table table = GetTable.getTable();

        PredicateBuilder builder =
            new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        Predicate notNull = builder.isNotNull(0);
        Predicate greaterOrEqual = builder.greaterOrEqual(1, 12);

        int[] projection = new int[] {0, 1};

        ReadBuilder readBuilder =
            table.newReadBuilder()
                .withProjection(projection)
                .withFilter(Lists.newArrayList(notNull, greaterOrEqual));

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        StreamTableScan scan = readBuilder.newStreamScan();
        while (true) {
            List<Split> splits = scan.plan().splits();
            // Distribute these splits to different tasks

            Long state = scan.checkpoint();
            // can be restored in scan.restore(state) after fail over

            // 3. Read a split in task
            TableRead read = readBuilder.newRead();
            RecordReader<InternalRow> reader = read.createReader(splits);
            reader.forEachRemaining(System.out::println);

            Thread.sleep(1000);
        }
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
- When a failure occurs, if you still have uncommitted `CommitMessage`s, please use `StreamTableCommit#filterAndCommit`
  to exclude the committed messages by commitIdentifier.

```java
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
        // 1. Create a WriteBuilder (Serializable)
        Table table = GetTable.getTable();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();

        // 2. Write records in distributed tasks
        StreamTableWrite write = writeBuilder.newWrite();
        // commitIdentifier like Flink checkpointId
        long commitIdentifier = 0;

        while (true) {
            GenericRow record1 = GenericRow.of(BinaryString.fromString("Alice"), 12);
            GenericRow record2 = GenericRow.of(BinaryString.fromString("Bob"), 5);
            GenericRow record3 = GenericRow.of(BinaryString.fromString("Emily"), 18);

            // If this is a distributed write, you can use writeBuilder.newWriteSelector.
            // WriteSelector determines to which logical downstream writers a record should be written to.
            // If it returns empty, no data distribution is required.

            write.write(record1);
            write.write(record2);
            write.write(record3);
            List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
            commitIdentifier++;

            // 3. Collect all CommitMessages to a global node and commit
            StreamTableCommit commit = writeBuilder.newCommit();
            commit.commit(commitIdentifier, messages);

            // 4. When failure occurs and you're not sure if the commit process is successful,
            //    you can use `filterAndCommit` to retry the commit process.
            //    Succeeded commits will be automatically skipped.
            /*
            Map<Long, List<CommitMessage>> commitIdentifiersAndMessages = new HashMap<>();
            commitIdentifiersAndMessages.put(commitIdentifier, messages);
            commit.filterAndCommit(commitIdentifiersAndMessages);
            */

            Thread.sleep(1000);
        }
    }
}
```

## Data Types

| Java         | Paimon                               | 
|:-------------|:-------------------------------------|
| boolean      | boolean                              |
| byte         | byte                                 |
| short        | short                                |
| int          | int                                  |
| long         | long                                 |
| float        | float                                |
| double       | double                               |
| string       | org.apache.paimon.data.BinaryString  |
| decimal      | org.apache.paimon.data.Decimal       |
| timestamp    | org.apache.paimon.data.Timestamp     |
| byte[]       | byte[]                               |
| array        | org.apache.paimon.data.InternalArray |
| map          | org.apache.paimon.data.InternalMap   |
| InternalRow  | org.apache.paimon.data.InternalRow   |

## Predicate Types

| SQL Predicate| Paimon Predicate                                             | 
|:-------------|:-------------------------------------------------------------|
| and          | org.apache.paimon.predicate.PredicateBuilder.And             |
| or           | org.apache.paimon.predicate.PredicateBuilder.Or              |
| is null      | org.apache.paimon.predicate.PredicateBuilder.IsNull          |
| is not null  | org.apache.paimon.predicate.PredicateBuilder.IsNotNull       |
| in           | org.apache.paimon.predicate.PredicateBuilder.In              |
| not in       | org.apache.paimon.predicate.PredicateBuilder.NotIn           |
| =            | org.apache.paimon.predicate.PredicateBuilder.Equal           |
| <>           | org.apache.paimon.predicate.PredicateBuilder.NotEqual        |
| <            | org.apache.paimon.predicate.PredicateBuilder.LessThan        |
| <=           | org.apache.paimon.predicate.PredicateBuilder.LessOrEqual     |
| >            | org.apache.paimon.predicate.PredicateBuilder.GreaterThan     |
| >=           | org.apache.paimon.predicate.PredicateBuilder.GreaterOrEqual  |
