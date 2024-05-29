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

package org.apache.paimon.operation;

import java.util.Collections;
import java.util.HashMap;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.Map;
import java.util.Random;

/** Tests for {@link AppendOnlyFileStoreWrite}. */
public class AppendOnlyFileStoreWriteTest {

    private static final Random RANDOM = new Random();

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWritesInBatch() throws Exception {
        FileStoreTable table = createFileStoreTable();

        AppendOnlyFileStoreWrite write = (AppendOnlyFileStoreWrite) table.store().newWrite("ss");
        write.withExecutionMode(false);

        write.write(partition(0), 0, GenericRow.of(0, 0, 0));
        write.write(partition(1), 1, GenericRow.of(1, 1, 0));
        write.write(partition(2), 2, GenericRow.of(2, 2, 0));
        write.write(partition(3), 3, GenericRow.of(3, 3, 0));
        write.write(partition(4), 4, GenericRow.of(4, 4, 0));

        for (Map<Integer, AbstractFileStoreWrite.WriterContainer<InternalRow>> bucketWriters :
                write.writers().values()) {
            for (AbstractFileStoreWrite.WriterContainer<InternalRow> writerContainer :
                    bucketWriters.values()) {
                Assertions.assertThat(((AppendOnlyWriter) writerContainer.writer).getWriteBuffer())
                        .isEqualTo(null);
            }
        }

        write.write(partition(5), 5, GenericRow.of(5, 5, 0));
        for (Map<Integer, AbstractFileStoreWrite.WriterContainer<InternalRow>> bucketWriters :
                write.writers().values()) {
            for (AbstractFileStoreWrite.WriterContainer<InternalRow> writerContainer :
                    bucketWriters.values()) {
                Assertions.assertThat(((AppendOnlyWriter) writerContainer.writer).getWriteBuffer())
                        .isInstanceOf(ExternalBuffer.class);
            }
        }

        write.write(partition(6), 6, GenericRow.of(6, 6, 0));
        write.write(partition(0), 0, GenericRow.of(0, 0, 0));
        write.write(partition(1), 1, GenericRow.of(1, 1, 0));
        write.write(partition(2), 2, GenericRow.of(2, 2, 0));
        write.write(partition(3), 3, GenericRow.of(3, 3, 0));
        List<CommitMessage> commit = write.prepareCommit(true, Long.MAX_VALUE);

        Assertions.assertThat(commit.size()).isEqualTo(7);

        long records =
                commit.stream()
                        .map(s -> (CommitMessageImpl) s)
                        .mapToLong(
                                s ->
                                        s.newFilesIncrement().newFiles().stream()
                                                .mapToLong(DataFileMeta::rowCount)
                                                .sum())
                        .sum();
        Assertions.assertThat(records).isEqualTo(11);
    }

    @Test
    public void testWritesInBatchWithNoExtraFiles() throws Exception {
        FileStoreTable table = createFileStoreTable();

        AppendOnlyFileStoreWrite write = (AppendOnlyFileStoreWrite) table.store().newWrite("ss");
        write.withExecutionMode(false);

        write.write(partition(0), 0, GenericRow.of(0, 0, 0));
        write.write(partition(1), 1, GenericRow.of(1, 1, 0));
        write.write(partition(2), 2, GenericRow.of(2, 2, 0));
        write.write(partition(3), 3, GenericRow.of(3, 3, 0));
        write.write(partition(4), 4, GenericRow.of(4, 4, 0));
        write.write(partition(5), 5, GenericRow.of(5, 5, 0));
        write.write(partition(6), 6, GenericRow.of(6, 6, 0));

        for (int i = 0; i < 1000; i++) {
            int number = RANDOM.nextInt(7);
            write.write(partition(number), number, GenericRow.of(number, number, 0));
        }

        List<CommitMessage> commit = write.prepareCommit(true, Long.MAX_VALUE);

        Assertions.assertThat(commit.size()).isEqualTo(7);

        long files =
                commit.stream()
                        .map(s -> (CommitMessageImpl) s)
                        .mapToLong(s -> s.newFilesIncrement().newFiles().size())
                        .sum();
        Assertions.assertThat(files).isEqualTo(7);

        long records =
                commit.stream()
                        .map(s -> (CommitMessageImpl) s)
                        .mapToLong(
                                s ->
                                        s.newFilesIncrement().newFiles().stream()
                                                .mapToLong(DataFileMeta::rowCount)
                                                .sum())
                        .sum();
        Assertions.assertThat(records).isEqualTo(1007);
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .partitionKeys("f0")
                        .option("bucket", "100")
                        .option("bucket-key", "f1")
                        .build();
        Identifier identifier = Identifier.create("default", "test");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private BinaryRow partition(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, i);
        writer.complete();
        return binaryRow;
    }

    @Test
    public void testScanFilterWithNullPartition() throws Exception {
        FileStoreTable table = createFileStoreTable();

        AppendOnlyFileStoreWrite write = (AppendOnlyFileStoreWrite) table.store().newWrite("ss");
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();
        write.withExecutionMode(false);

        for (int i = 0; i < 100; i++) {
            write.write(nullPartition(), i, GenericRow.of(null, i, i));
            commit.commit(i, write.prepareCommit(false, i));
        }

        BinaryRow binaryRow = nullPartition();
        FileStoreScan scan = table.store().newScan();
        List<SimpleFileEntry> l0 = scan.withPartitionFilter(Collections.singletonList(binaryRow)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(100);
    }


    private BinaryRow nullPartition() {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.setNullAt(0);
        writer.complete();
        return binaryRow;
    }
}
