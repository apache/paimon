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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Test for {@link AppendOnlyTableCompactionCoordinator}. */
public class AppendOnlyTableCompactionITTest {

    @TempDir private Path tempDir;
    private AppendOnlyFileStoreTable appendOnlyFileStoreTable;
    private SnapshotManager snapshotManager;
    private AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private AppendOnlyFileStoreWrite write;
    private final String commitUser = UUID.randomUUID().toString();

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
        // commit 11 files
        List<CommitMessage> messages = writeCommit(11);
        commit(messages);

        // first compact, six files left after commit compact and update restored files
        // test run method invoke scan and compactPlan
        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.run();
        Assertions.assertEquals(tasks.size(), 1);
        AppendOnlyCompactionTask task = tasks.get(0);
        Assertions.assertEquals(task.compactBefore().size(), 6);
        List<CommitMessage> result = doCompact(tasks);
        Assertions.assertEquals(1, result.size());
        commit(result);
        compactionCoordinator.scan();
        Assertions.assertEquals(compactionCoordinator.listRestoredFiles().size(), 6);

        // second compact, only one file left after updateRestored
        tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), 1);
        // before update, zero file left
        Assertions.assertEquals(compactionCoordinator.listRestoredFiles().size(), 0);
        commit(doCompact(tasks));
        compactionCoordinator.scan();
        // one file is loaded from delta
        List<DataFileMeta> last = new ArrayList<>(compactionCoordinator.listRestoredFiles());
        Assertions.assertEquals(last.size(), 1);
        Assertions.assertEquals(last.get(0).rowCount(), 11);
    }

    @Test
    public void testCompactionLot() throws Exception {
        // test continuous compaction
        Assertions.assertNull(snapshotManager.latestSnapshotId());

        long count = 0;
        for (int i = 90; i < 100; i++) {
            count += i;
            commit(writeCommit(i));
            commit(doCompact(compactionCoordinator.run()));
            // scan the file generated itself
            Assertions.assertTrue(compactionCoordinator.scan());
            Assertions.assertEquals(
                    compactionCoordinator.listRestoredFiles().stream()
                            .map(DataFileMeta::rowCount)
                            .reduce(Long::sum)
                            .get(),
                    count);
        }

        Assertions.assertEquals(
                appendOnlyFileStoreTable.store().newScan().plan().files().size(),
                compactionCoordinator.listRestoredFiles().size());

        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.compactPlan();
        while (tasks.size() != 0) {
            commit(doCompact(tasks));
            tasks = compactionCoordinator.run();
        }

        int remainedSize = appendOnlyFileStoreTable.store().newScan().plan().files().size();
        Assertions.assertEquals(remainedSize, compactionCoordinator.listRestoredFiles().size());
        Assertions.assertEquals(remainedSize, 5);
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

    private List<CommitMessage> doCompact(List<AppendOnlyCompactionTask> tasks) throws Exception {
        List<CommitMessage> result = new ArrayList<>();
        for (AppendOnlyCompactionTask task : tasks) {
            result.add(task.doCompact(write));
        }
        return result;
    }

    private InternalRow randomRow() {
        return GenericRow.of(
                random.nextInt(100),
                BinaryString.fromString("A" + random.nextInt(100)),
                BinaryString.fromString("B" + random.nextInt(100)),
                BinaryString.fromString("C" + random.nextInt(100)));
    }

    private static final Random random = new Random();

    @BeforeEach
    public void createNegativeAppendOnlyTable() throws Exception {
        FileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        TableSchema tableSchema = schemaManager.createTable(schema());
        snapshotManager = new SnapshotManager(fileIO, path);
        appendOnlyFileStoreTable =
                (AppendOnlyFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO,
                                new org.apache.paimon.fs.Path(tempDir.toString()),
                                tableSchema);
        compactionCoordinator = new AppendOnlyTableCompactionCoordinator(appendOnlyFileStoreTable);
        write = appendOnlyFileStoreTable.store().newWrite(commitUser);
    }
}
