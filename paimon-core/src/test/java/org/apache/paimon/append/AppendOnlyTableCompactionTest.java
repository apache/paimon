/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Test for {@link org.apache.paimon.append.AppendOnlyTableCompactionCoordinator}. */
public class AppendOnlyTableCompactionTest {
    @TempDir Path tempDir;

    private AppendOnlyFileStoreTable appendOnlyFileStoreTable;
    private AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private AppendOnlyTableCompactionWorker compactionWorker;
    private String commitUser = UUID.randomUUID().toString();

    @Test
    public void noCompaction() throws Exception {
        List<CommitMessage> messages = writeCommit(10);

        messages.forEach(
                message ->
                        Assertions.assertTrue(
                                ((CommitMessageImpl) message).compactIncrement().isEmpty()));
    }

    @Test
    public void compactionTaskTest() throws Exception {
        List<CommitMessage> messages = writeCommit(10);
        compactionCoordinator.addAll(messages);
        List<CompactionTask> tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), 2);
        Assertions.assertTrue(tasks.get(0).isNeedCompact());
        Assertions.assertFalse(tasks.get(1).isNeedCompact());

        compactionWorker.accept(tasks);
        compactionWorker.doCompact();
        List<CommitMessage> result = compactionWorker.drainResult();
        Assertions.assertEquals(2, result.size());

        CommitMessageImpl impl0 = (CommitMessageImpl) result.get(0);
        Assertions.assertIterableEquals(
                impl0.newFilesIncrement().newFiles(), tasks.get(0).newFiles());
        Assertions.assertEquals(1, impl0.compactIncrement().compactAfter().size());

        CommitMessageImpl impl1 = (CommitMessageImpl) result.get(1);
        Assertions.assertTrue(impl1.compactIncrement().isEmpty());
    }

    @Test
    public void testCompactionCoordinator() throws Exception {
        Assertions.assertEquals(compactionCoordinator.currentSnapshotId(), null);

        List<CommitMessage> messages = writeCommit(10);
        commit(messages);

        compactionCoordinator.updateRestore();
        Assertions.assertEquals(compactionCoordinator.currentSnapshotId(), 1L);
        compactionCoordinator.addAll(writeCommit(10));
        commit(
                compactionWorker
                        .accept(compactionCoordinator.compactPlan())
                        .doCompact()
                        .drainResult());
        compactionCoordinator.updateRestore();
        Assertions.assertEquals(compactionCoordinator.currentSnapshotId(), 3L);
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option("compaction.min.file-num", "3");
        schemaBuilder.option("compaction.max.file-num", "6");
        schemaBuilder.option("bucket", "-1");
        return schemaBuilder.build();
    }

    private void commit(List<CommitMessage> messages) {
        appendOnlyFileStoreTable.newCommit(commitUser).commit(messages);
    }

    private List<CommitMessage> writeCommit(int number) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        StreamTableWrite writer = appendOnlyFileStoreTable.newStreamWriteBuilder().newWrite();
        for (int i = 0; i < number; i++) {
            writer.write(randomRow());
            messages.addAll(writer.prepareCommit(true, i));
        }
        return messages;
    }

    private InternalRow randomRow() {
        return GenericRow.of(
                random.nextInt(100),
                BinaryString.fromString("A" + random.nextInt(100)),
                BinaryString.fromString("B" + random.nextInt(100)),
                BinaryString.fromString("C" + random.nextInt(100)));
    }

    private static Random random = new Random();

    @BeforeEach
    public void createNegativeAppendOnlyTable() throws Exception {
        FileIO fileIO = new LocalFileIO();
        SchemaManager schemaManager =
                new SchemaManager(fileIO, new org.apache.paimon.fs.Path(tempDir.toString()));
        TableSchema tableSchema = schemaManager.createTable(schema());
        appendOnlyFileStoreTable =
                (AppendOnlyFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO,
                                new org.apache.paimon.fs.Path(tempDir.toString()),
                                tableSchema);
        compactionCoordinator = appendOnlyFileStoreTable.getCompactionCoordinator();
        compactionWorker = appendOnlyFileStoreTable.getCompactionWorker();
    }
}
