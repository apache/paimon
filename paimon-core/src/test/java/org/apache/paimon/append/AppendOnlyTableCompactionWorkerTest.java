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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
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

/** Tests for {@link AppendOnlyTableCompactionWorker}. */
public class AppendOnlyTableCompactionWorkerTest {

    private static final Random RANDOM = new Random();

    @TempDir Path tempDir;
    private AppendOnlyFileStoreTable appendOnlyFileStoreTable;
    private AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private AppendOnlyTableCompactionWorker compactionWorker;
    private BinaryRow partition;
    private final String commitUser = UUID.randomUUID().toString();

    @Test
    public void compactinoWorkerExecuteSinglePartitionTest() throws Exception {
        // single partition worker execute test
        int fileNumer = 100;
        int taskSize = fileNumer / 6;
        List<DataFileMeta> writtenFiles = writeCommit(fileNumer);
        compactionCoordinator.notifyNewFiles(partition, writtenFiles);
        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), taskSize);

        compactionWorker.accept(tasks);
        List<CommitMessage> lists = compactionWorker.doCompact();

        Assertions.assertEquals(lists.size(), taskSize);

        for (int i = 0; i < taskSize; i++) {
            AppendOnlyCompactionTask task = tasks.get(i);
            CommitMessageImpl commitMessage = (CommitMessageImpl) lists.get(i);
            Assertions.assertIterableEquals(
                    task.compactBefore(), commitMessage.compactIncrement().compactBefore());
            Assertions.assertEquals(commitMessage.compactIncrement().compactAfter().size(), 1);
        }
    }

    @Test
    public void compactinoWorkerExecuteMultiPartitionTest() throws Exception {
        // test muti-partition compaction task execute situation
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeInt(0, 0);
        binaryRowWriter.complete();
        BinaryRow partition0 = binaryRow.copy();

        binaryRowWriter.reset();
        binaryRowWriter.writeInt(0, 1);
        binaryRowWriter.complete();
        BinaryRow partition1 = binaryRow.copy();

        int fileNumer = 100;
        int taskSize = fileNumer / 6;
        List<DataFileMeta> writtenFiles0 = writeCommit(fileNumer);
        compactionCoordinator.notifyNewFiles(partition0, writtenFiles0);
        List<DataFileMeta> writtenFiles1 = writeCommit(fileNumer);
        compactionCoordinator.notifyNewFiles(partition1, writtenFiles1);
        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), taskSize * 2);

        compactionWorker.accept(tasks);
        List<CommitMessage> lists = compactionWorker.doCompact();

        Assertions.assertEquals(lists.size(), taskSize * 2);

        for (int i = 0; i < taskSize; i++) {
            AppendOnlyCompactionTask task = tasks.get(i);
            CommitMessageImpl commitMessage = (CommitMessageImpl) lists.get(i);
            Assertions.assertEquals(task.partition(), commitMessage.partition());
            Assertions.assertIterableEquals(
                    task.compactBefore(), commitMessage.compactIncrement().compactBefore());
        }
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3");
        schemaBuilder.option(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "6");
        return schemaBuilder.build();
    }

    private List<DataFileMeta> writeCommit(int number) throws Exception {
        List<DataFileMeta> fileMetas = new ArrayList<>();
        StreamTableWrite writer = appendOnlyFileStoreTable.newStreamWriteBuilder().newWrite();
        for (int i = 0; i < number; i++) {
            writer.write(randomRow());
            for (CommitMessage message : writer.prepareCommit(true, i)) {
                CommitMessageImpl commitMessage = (CommitMessageImpl) message;
                fileMetas.addAll(commitMessage.newFilesIncrement().newFiles());
            }
        }
        return fileMetas;
    }

    private InternalRow randomRow() {
        return GenericRow.of(
                RANDOM.nextInt(100),
                BinaryString.fromString("A" + RANDOM.nextInt(100)),
                BinaryString.fromString("B" + RANDOM.nextInt(100)),
                BinaryString.fromString("C" + RANDOM.nextInt(100)));
    }

    @BeforeEach
    public void createCoordinator() throws Exception {
        FileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        TableSchema tableSchema = schemaManager.createTable(schema());

        appendOnlyFileStoreTable =
                (AppendOnlyFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO,
                                new org.apache.paimon.fs.Path(tempDir.toString()),
                                tableSchema);
        compactionCoordinator = new AppendOnlyTableCompactionCoordinator(appendOnlyFileStoreTable);
        compactionWorker =
                new AppendOnlyTableCompactionWorker(appendOnlyFileStoreTable, commitUser);
        partition = BinaryRow.EMPTY_ROW;
    }
}
