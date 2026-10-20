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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOExceptionSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RawFileSplitRead}. */
class RawFileSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "row, false",
        "avro, false",
        "avro, true",
        "parquet, false",
        "parquet, true",
        "orc, false",
        "orc, true"
    })
    void testRawConvertibleReadUsesKeyValueSchema(String format, boolean thinMode)
            throws Exception {
        Path tablePath = new Path(tempDir.resolve("primary-key-raw").toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.FILE_FORMAT, format);
        options.set(CoreOptions.DATA_FILE_THIN_MODE, thinMode);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "a")
                        .options(options.toMap())
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 10, 100L));
            write.write(GenericRow.of(1, 20, 200L));
            write.write(GenericRow.of(1, 30, 300L));
            write.write(GenericRow.of(1, 40, 400L));
            write.write(GenericRow.of(2, 20, 220L));
            commit.commit(write.prepareCommit());
        }

        List<DataSplit> dataSplits = table.newSnapshotReader().read().dataSplits();
        assertThat(dataSplits).hasSize(2);
        for (DataSplit split : dataSplits) {
            assertThat(split.rawConvertible()).isTrue();
            assertThat(split.dataFiles()).hasSize(1);
        }
        List<Split> splits = new ArrayList<>(dataSplits);
        for (int schemaVersion = 0; schemaVersion < 2; schemaVersion++) {
            RowType readType =
                    new RowType(
                            Arrays.asList(
                                    table.rowType().getField(2),
                                    table.rowType().getField(1),
                                    table.rowType().getField(0)));
            PredicateBuilder builder = new PredicateBuilder(table.rowType());
            TableRead read =
                    table.newRead()
                            .withReadType(readType)
                            .withFilter(
                                    and(
                                            builder.equal(0, 1),
                                            builder.greaterOrEqual(1, 20),
                                            builder.lessOrEqual(2, 300L)))
                            .executeFilter();
            List<String> actual = new ArrayList<>();
            try (RecordReader<InternalRow> reader = read.createReader(splits)) {
                reader.forEachRemaining(
                        row -> {
                            assertThat(row.getFieldCount()).isEqualTo(3);
                            assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
                            actual.add(row.getLong(0) + "|" + row.getInt(1) + "|" + row.getInt(2));
                        });
            }
            assertThat(actual).containsExactlyInAnyOrder("200|20|1", "300|30|1");

            if (schemaVersion == 0) {
                table.schemaManager()
                        .commitChanges(
                                Arrays.asList(
                                        SchemaChange.renameColumn("a", "renamed_a"),
                                        SchemaChange.renameColumn("b", "renamed_b")));
                table = table.copyWithLatestSchema();
                assertThat(dataSplits.get(0).dataFiles().get(0).schemaId())
                        .isLessThan(table.schema().id());
            }
        }
    }

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
                        TableSchema::fields,
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

    @Test
    void testLimitAfterBitmap64DeletionVector() throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rows.add(GenericRow.of(BinaryString.fromString("value-" + i), i));
        }
        FileStoreTable table = createTable("bitmap64-limit", rows);
        DataSplit split = singleSplit(table);
        assertThat(split.dataFiles()).hasSize(1);

        DeletionVector deletionVector = new Bitmap64DeletionVector();
        for (int position = 0; position < 5; position++) {
            deletionVector.delete(position);
        }
        String fileName = split.dataFiles().get(0).fileName();
        Map<String, IOExceptionSupplier<DeletionVector>> deletionVectorFactories =
                Collections.singletonMap(fileName, () -> deletionVector);

        RawFileSplitRead read = ((AppendOnlyFileStore) table.store()).newRead();
        read.withLimit(10);
        AtomicInteger count = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                read.createReader(
                        split.partition(),
                        split.bucket(),
                        split.dataFiles(),
                        deletionVectorFactories)) {
            reader.forEachRemaining(ignored -> count.incrementAndGet());
        }

        assertThat(count).hasValue(10);
    }

    @Test
    void testTableReadSharesBatchSizer() throws Exception {
        FileStoreTable table = createTable("dynamic-batch-size", 20);
        ReadBatchSizer sizer = new ReadBatchSizer();
        sizer.setBatchSize(5);
        InnerTableRead read = table.newRead().withReadBatchSizer(sizer);

        try (RecordReader<InternalRow> reader = read.createReader(singleSplit(table))) {
            assertThat(readBatchSize(reader)).isEqualTo(5);

            sizer.setBatchSize(2);
            assertThat(readBatchSize(reader)).isEqualTo(2);

            sizer.setBatchSize(8);
            assertThat(readBatchSize(reader)).isEqualTo(8);
        }
    }

    private static int readBatchSize(RecordReader<InternalRow> reader) throws Exception {
        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        assertThat(batch).isNotNull();
        int count = 0;
        while (batch.next() != null) {
            count++;
        }
        batch.releaseBatch();
        return count;
    }

    private FileStoreTable createTable(String directory) throws Exception {
        return createTable(
                directory,
                Collections.singletonList(GenericRow.of(BinaryString.fromString("value"), 42)));
    }

    private FileStoreTable createTable(String directory, int rowCount) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(GenericRow.of(BinaryString.fromString("value"), i + 42));
        }
        return createTable(directory, rows);
    }

    private FileStoreTable createTable(String directory, List<? extends InternalRow> rows)
            throws Exception {
        Path tablePath = new Path(tempDir.resolve(directory).toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "first");
        options.set(CoreOptions.READ_BATCH_SIZE, 8);
        Schema schema =
                Schema.newBuilder()
                        .column("first", DataTypes.STRING())
                        .column("second", DataTypes.INT())
                        .options(options.toMap())
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
        return table;
    }

    private static DataSplit singleSplit(FileStoreTable table) {
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).hasSize(1);
        return splits.get(0);
    }
}
