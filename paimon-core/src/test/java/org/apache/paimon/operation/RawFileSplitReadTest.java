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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RawFileSplitRead}. */
class RawFileSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testReaderMappingIsNotSharedBetweenReadTypes() throws Exception {
        FileStoreTable table = createTable("mapping-cache");
        DataSplit split = singleSplit(table);
        InnerTableRead read = table.newRead();

        RowType firstProjection = table.rowType().project("first");
        read.withReadType(firstProjection);
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.next().getString(0).toString()).isEqualTo("value");
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }

        RowType secondProjection = table.rowType().project("second");
        read.withReadType(secondProjection);
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            assertThat(row.getFieldCount()).isEqualTo(1);
            assertThat(row.getInt(0)).isEqualTo(42);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
    }

    @Test
    void testVectorMappingUsesReadTypeCapturedByReader() throws Exception {
        FileStoreTable table = createTable("vector-output-type");
        appendRow(table, "value-2", 43);
        DataSplit split = singleSplit(table);
        assertThat(split.dataFiles()).hasSize(2);

        List<RowType> mappedOutputTypes = new ArrayList<>();
        FileFormat recordingFormat =
                new FlushingFileFormat(table.coreOptions().fileFormatString()) {
                    @Override
                    public FormatReaderFactory createReaderFactory(
                            RowType dataSchemaRowType,
                            RowType projectedRowType,
                            List<Predicate> filters) {
                        return context ->
                                new RecordingVectorReader(mappedOutputTypes, context.filePath());
                    }
                };
        RawFileSplitRead read =
                new RawFileSplitRead(
                        table.fileIO(),
                        table.schemaManager(),
                        table.schema(),
                        table.rowType(),
                        ignored -> recordingFormat,
                        table.store().pathFactory(),
                        table.coreOptions());

        RowType firstProjection = table.rowType().project("first");
        read.withReadType(firstProjection);
        try (RecordReader<InternalRow> existingReader = read.createReader(split)) {
            RowType secondProjection = table.rowType().project("second");
            read.withReadType(secondProjection);

            assertThat(consumeAllBatches(existingReader)).isEqualTo(2);
            assertThat(mappedOutputTypes).containsExactly(firstProjection, firstProjection);
        }

        RowType secondProjection = table.rowType().project("second");
        try (RecordReader<InternalRow> updatedReader = read.createReader(split)) {
            assertThat(consumeAllBatches(updatedReader)).isEqualTo(2);
            assertThat(mappedOutputTypes)
                    .containsExactly(
                            firstProjection, firstProjection, secondProjection, secondProjection);
        }
    }

    @Test
    void testEqualReadTypeReusesFormatReaderMapping() throws Exception {
        FileStoreTable table = createTable("equal-mapping-cache");
        AtomicInteger readerFactoryCreations = new AtomicInteger();
        FileFormat countingFormat =
                new FlushingFileFormat(table.coreOptions().fileFormatString()) {
                    @Override
                    public FormatReaderFactory createReaderFactory(
                            RowType dataSchemaRowType,
                            RowType projectedRowType,
                            List<Predicate> filters) {
                        readerFactoryCreations.incrementAndGet();
                        return super.createReaderFactory(
                                dataSchemaRowType, projectedRowType, filters);
                    }
                };
        RawFileSplitRead read =
                new RawFileSplitRead(
                        table.fileIO(),
                        table.schemaManager(),
                        table.schema(),
                        table.rowType(),
                        ignored -> countingFormat,
                        table.store().pathFactory(),
                        table.coreOptions());
        DataSplit split = singleSplit(table);

        RowType firstProjection = table.rowType().project("first");
        read.withReadType(firstProjection);
        try (RecordReader<InternalRow> ignored = read.createReader(split)) {
            assertThat(readerFactoryCreations).hasValue(1);
        }

        RowType equalFirstProjection = table.rowType().project("first");
        assertThat(equalFirstProjection).isEqualTo(firstProjection).isNotSameAs(firstProjection);
        read.withReadType(equalFirstProjection);
        try (RecordReader<InternalRow> ignored = read.createReader(split)) {
            assertThat(readerFactoryCreations).hasValue(1);
        }

        read.withReadType(table.rowType().project("second"));
        try (RecordReader<InternalRow> ignored = read.createReader(split)) {
            assertThat(readerFactoryCreations).hasValue(2);
        }
    }

    private FileStoreTable createTable(String directory) throws Exception {
        Path tablePath = new Path(tempDir.resolve(directory).toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "first");
        Schema schema =
                Schema.newBuilder()
                        .column("first", DataTypes.STRING())
                        .column("second", DataTypes.INT())
                        .options(options.toMap())
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(LocalFileIO.create(), tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        appendRow(table, "value", 42);
        return table;
    }

    private static void appendRow(FileStoreTable table, String first, int second) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString(first), second));
            commit.commit(write.prepareCommit());
        }
    }

    private static DataSplit singleSplit(FileStoreTable table) {
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).hasSize(1);
        return splits.get(0);
    }

    private static int consumeAllBatches(RecordReader<InternalRow> reader) throws Exception {
        int batches = 0;
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            batches++;
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
        return batches;
    }

    private static class RecordingVectorReader implements FileRecordReader<InternalRow> {

        private final List<RowType> mappedOutputTypes;
        private final Path filePath;
        private boolean emitted;

        private RecordingVectorReader(List<RowType> mappedOutputTypes, Path filePath) {
            this.mappedOutputTypes = mappedOutputTypes;
            this.filePath = filePath;
        }

        @Nullable
        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            if (emitted) {
                return null;
            }
            emitted = true;

            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[0]);
            batch.setNumRows(0);
            RecordingColumnarRowIterator iterator =
                    new RecordingColumnarRowIterator(filePath, batch, mappedOutputTypes);
            iterator.reset(0);
            return iterator;
        }

        @Override
        public void close() {}
    }

    private static class RecordingColumnarRowIterator extends ColumnarRowIterator {

        private final List<RowType> mappedOutputTypes;

        private RecordingColumnarRowIterator(
                Path filePath, VectorizedColumnBatch batch, List<RowType> mappedOutputTypes) {
            super(filePath, new ColumnarRow(batch), null);
            this.mappedOutputTypes = mappedOutputTypes;
        }

        @Override
        public ColumnarRowIterator mapping(
                RowType outputRowType,
                @Nullable PartitionInfo partitionInfo,
                @Nullable int[] indexMapping) {
            mappedOutputTypes.add(outputRowType);
            return this;
        }
    }
}
