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

import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_APPEND_ORDERED;
import static org.apache.paimon.CoreOptions.WRITE_MAX_WRITERS_TO_SPILL;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;

/** Tests for {@link BucketedAppendFileStoreWrite}. */
public class BucketedAppendFileStoreWriteTest {

    private static final Random RANDOM = new Random();

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWritesInBatch() throws Exception {
        FileStoreTable table = createFileStoreTable();
        table = table.copy(Collections.singletonMap(WRITE_MAX_WRITERS_TO_SPILL.key(), "5"));

        BucketedAppendFileStoreWrite write =
                (BucketedAppendFileStoreWrite) table.store().newWrite("ss");
        write.withIOManager(IOManager.create(tempDir.toString()));

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

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"parquet", "orc"})
    public void testSharedShreddingForceBufferSpill(String fileFormat) throws Exception {
        FileStoreTable table = createSharedShreddingForceBufferSpillTable(fileFormat);
        BucketedAppendFileStoreWrite write =
                (BucketedAppendFileStoreWrite) table.store().newWrite("ss");
        write.withIOManager(IOManager.create(tempDir.toString()));

        write.write(partition(0), 0, GenericRow.of(0, 0, map("a", 10L)));
        Assertions.assertThat(writeBuffers(write)).containsExactly((RowBuffer) null);

        // Creating the second writer reaches the configured limit and triggers force spill.
        write.write(partition(1), 1, GenericRow.of(1, 1, map("b", 20L)));
        Assertions.assertThat(writeBuffers(write))
                .hasSize(2)
                .allSatisfy(
                        buffer -> Assertions.assertThat(buffer).isInstanceOf(ExternalBuffer.class));

        write.write(partition(0), 0, GenericRow.of(0, 2, map("c", 30L)));
        write.write(partition(1), 1, GenericRow.of(1, 3, map("d", 40L)));

        List<CommitMessage> messages = write.prepareCommit(true, Long.MAX_VALUE);
        try (StreamTableCommit commit = table.newStreamWriteBuilder().newCommit()) {
            commit.commit(0, messages);
        }

        List<List<Object>> actual = new ArrayList<>();
        ReadBuilder readBuilder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            actual.add(
                                    Arrays.asList(
                                            row.getInt(0),
                                            row.getInt(1),
                                            toJavaMap(row.getMap(2)))));
        }
        Assertions.assertThat(actual)
                .containsExactlyInAnyOrder(
                        Arrays.asList(0, 0, Collections.singletonMap("a", 10L)),
                        Arrays.asList(1, 1, Collections.singletonMap("b", 20L)),
                        Arrays.asList(0, 2, Collections.singletonMap("c", 30L)),
                        Arrays.asList(1, 3, Collections.singletonMap("d", 40L)));
    }

    @Test
    public void testWritesInBatchWithNoExtraFiles() throws Exception {
        FileStoreTable table = createFileStoreTable();

        BaseAppendFileStoreWrite write = (BaseAppendFileStoreWrite) table.store().newWrite("ss");

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

    private FileStoreTable createSharedShreddingForceBufferSpillTable(String fileFormat)
            throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                        .partitionKeys("f0")
                        .option(BUCKET.key(), "100")
                        .option("bucket-key", "f1")
                        .option(WRITE_ONLY.key(), "true")
                        .option("file.format", fileFormat)
                        .option(WRITE_MAX_WRITERS_TO_SPILL.key(), "1")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build();
        Identifier identifier = Identifier.create("default", "spill_" + fileFormat);
        catalog.createDatabase("default", true);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private GenericMap map(String key, long value) {
        return new GenericMap(Collections.singletonMap(BinaryString.fromString(key), value));
    }

    private List<RowBuffer> writeBuffers(BucketedAppendFileStoreWrite write) {
        List<RowBuffer> buffers = new ArrayList<>();
        for (Map<Integer, AbstractFileStoreWrite.WriterContainer<InternalRow>> bucketWriters :
                write.writers().values()) {
            for (AbstractFileStoreWrite.WriterContainer<InternalRow> writerContainer :
                    bucketWriters.values()) {
                buffers.add(((AppendOnlyWriter) writerContainer.writer).getWriteBuffer());
            }
        }
        return buffers;
    }

    private Map<String, Long> toJavaMap(InternalMap map) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(map.keyArray().getString(i).toString(), map.valueArray().getLong(i));
        }
        return result;
    }

    @Test
    public void testIgnorePreviousFilesChecksPartitionBucketNumber() throws Exception {
        FileStoreTable table = createFileStoreTable().copy(bucketOptions(2, false, false));
        BaseAppendFileStoreWrite write = (BaseAppendFileStoreWrite) table.store().newWrite("ss");
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();

        write.write(partition(1), 1, GenericRow.of(1, 1, 0));
        commit.commit(0, write.prepareCommit(false, 0));

        FileStoreTable rescaledTable = table.copy(bucketOptions(4, false, true));
        write = (BaseAppendFileStoreWrite) rescaledTable.store().newWrite("ss");
        write.write(partition(1), 1, GenericRow.of(1, 1, 0));
        List<CommitMessage> commitMessages = write.prepareCommit(false, 1);
        Assertions.assertThat(commitMessages).isNotEmpty();
        Assertions.assertThatThrownBy(
                        () ->
                                rescaledTable
                                        .newStreamWriteBuilder()
                                        .newCommit()
                                        .commit(1, commitMessages))
                .hasMessageContaining("new bucket num 4")
                .hasMessageContaining("previous bucket num is 2");

        write = (BaseAppendFileStoreWrite) rescaledTable.store().newWrite("ss");
        write.write(partition(2), 2, GenericRow.of(2, 2, 0));
        rescaledTable.newStreamWriteBuilder().newCommit().commit(2, write.prepareCommit(false, 2));
    }

    private Map<String, String> bucketOptions(
            int bucket, boolean bucketAppendOrdered, boolean writeOnly) {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(bucket));
        options.put(BUCKET_APPEND_ORDERED.key(), String.valueOf(bucketAppendOrdered));
        options.put(WRITE_ONLY.key(), String.valueOf(writeOnly));
        return options;
    }

    private BinaryRow partition(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, i);
        writer.complete();
        return binaryRow;
    }

    @Test
    public void testScanFilterWithMixedPartitionWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        BaseAppendFileStoreWrite write = (BaseAppendFileStoreWrite) table.store().newWrite("ss");
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();

        for (int i = 0; i < 100; i++) {
            if (i == 0) {
                write.write(nullPartition(), i, GenericRow.of(null, i, i));
                commit.commit(i, write.prepareCommit(false, i));
            } else {
                write.write(partition(1), i, GenericRow.of(null, i, i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }

        BinaryRow binaryRow = nullPartition();
        FileStoreScan scan = table.store().newScan();
        List<SimpleFileEntry> l0 =
                scan.withPartitionFilter(Arrays.asList(binaryRow)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(1);

        BinaryRow binaryRow1 = partition(1);
        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow, binaryRow1)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(100);

        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow1)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(99);
    }

    @Test
    public void testScanFilterWithAllNullPartitionWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        BaseAppendFileStoreWrite write = (BaseAppendFileStoreWrite) table.store().newWrite("ss");
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();

        for (int i = 0; i < 100; i++) {
            write.write(nullPartition(), i, GenericRow.of(null, i, i));
            commit.commit(i, write.prepareCommit(false, i));
        }

        BinaryRow binaryRow = nullPartition();
        FileStoreScan scan = table.store().newScan();
        List<SimpleFileEntry> l0 =
                scan.withPartitionFilter(Arrays.asList(binaryRow)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(100);

        BinaryRow binaryRow1 = partition(1);
        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow, binaryRow1)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(100);

        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow1)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(0);
    }

    @Test
    public void testScanFilterWithNoneNullPartitionWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        BaseAppendFileStoreWrite write = (BaseAppendFileStoreWrite) table.store().newWrite("ss");
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();

        for (int i = 0; i < 100; i++) {
            write.write(partition(1), i, GenericRow.of(null, i, i));
            commit.commit(i, write.prepareCommit(false, i));
        }

        BinaryRow binaryRow = nullPartition();
        FileStoreScan scan = table.store().newScan();
        List<SimpleFileEntry> l0 =
                scan.withPartitionFilter(Arrays.asList(binaryRow)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(0);

        BinaryRow binaryRow1 = partition(1);
        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow, binaryRow1)).readSimpleEntries();
        Assertions.assertThat(l0.size()).isEqualTo(100);

        l0 = scan.withPartitionFilter(Arrays.asList(binaryRow1)).readSimpleEntries();
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
