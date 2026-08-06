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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for reading manifest partition columns when the physical Mosaic file contains
 * only business columns.
 */
class PartitionColumnsOutsidePhysicalMosaicTest {

    private static final List<String> BUSINESS_COLUMNS = Arrays.asList("payload", "vin");
    private static final String PAYLOAD = "payload-1";
    private static final String VIN = "VIN-0001";
    private static final String DT = "2026-08-04";
    private static final String HH = "13";
    private static final String RPT_DT = "2026-08-03";

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private FileStoreTable table;

    @BeforeEach
    void setUp() throws Exception {
        catalog =
                new FileSystemCatalog(
                        LocalFileIO.create(), new Path(tempDir.resolve("warehouse").toString()));
        catalog.createDatabase("db", true);

        table = createTable("target", false);
        writePhysicalBusinessColumns(table, 1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void testDefaultReadRestoresAllManifestPartitionColumns() throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        Split split = planSingleSplit(readBuilder);

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            assertThat(row.getFieldCount()).isEqualTo(5);
            assertThat(row.getString(0).toString()).isEqualTo(PAYLOAD);
            assertThat(row.getString(1).toString()).isEqualTo(VIN);
            assertThat(row.getString(2).toString()).isEqualTo(DT);
            assertThat(row.getString(3).toString()).isEqualTo(HH);
            assertThat(row.getString(4).toString()).isEqualTo(RPT_DT);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
    }

    @Test
    void testBusinessProjectionReadsPhysicalColumns() throws Exception {
        assertBusinessOnlyRead(table.newReadBuilder().withProjection(new int[] {0, 1}));
    }

    @Test
    void testBusinessReadTypeReadsPhysicalColumns() throws Exception {
        RowType businessType = table.rowType().project(BUSINESS_COLUMNS);
        assertBusinessOnlyRead(table.newReadBuilder().withReadType(businessType));
    }

    @Test
    void testPartitionOnlyProjectionKeepsRequestedOrder() throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {4, 2});
        assertThat(readBuilder.readType().getFieldNames()).containsExactly("rpt_dt", "dt");
        Split split = planSingleSplit(readBuilder);

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getString(0).toString()).isEqualTo(RPT_DT);
            assertThat(row.getString(1).toString()).isEqualTo(DT);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
    }

    @Test
    void testExistingLazyReaderKeepsOriginalReadType() throws Exception {
        FileStoreTable trackingTable = createTable("row_tracking", true);
        writePhysicalBusinessColumns(trackingTable, 2);

        List<DataSplit> splits = trackingTable.newSnapshotReader().read().dataSplits();
        assertThat(splits).hasSize(1);
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles()).hasSize(2);

        RawFileSplitRead read = (RawFileSplitRead) trackingTable.store().newRead();
        RowType trackingType = SpecialFields.rowTypeWithRowTracking(trackingTable.rowType());
        read.withReadType(trackingType.project("payload", SpecialFields.ROW_ID.name()));

        try (RecordReader<InternalRow> existingReader = read.createReader(split)) {
            read.withReadType(trackingTable.rowType().project("vin"));

            assertThat(readFirstStrings(existingReader, 2))
                    .containsExactlyInAnyOrder("payload-1", "payload-2");
        }

        try (RecordReader<InternalRow> updatedReader = read.createReader(split)) {
            assertThat(readFirstStrings(updatedReader, 1))
                    .containsExactlyInAnyOrder("VIN-0001", "VIN-0002");
        }
    }

    private void assertBusinessOnlyRead(ReadBuilder readBuilder) throws Exception {
        assertThat(readBuilder.readType().getFieldNames())
                .containsExactlyElementsOf(BUSINESS_COLUMNS);
        Split split = planSingleSplit(readBuilder);

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getString(0).toString()).isEqualTo(PAYLOAD);
            assertThat(row.getString(1).toString()).isEqualTo(VIN);
            assertThat(batch.next()).isNull();
            batch.releaseBatch();
        }
    }

    private Split planSingleSplit(ReadBuilder readBuilder) {
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0)).isInstanceOf(DataSplit.class);

        DataSplit dataSplit = (DataSplit) splits.get(0);
        assertThat(dataSplit.dataFiles()).hasSize(1);
        assertThat(dataSplit.dataFiles().get(0).writeCols())
                .containsExactlyElementsOf(BUSINESS_COLUMNS);
        return dataSplit;
    }

    private FileStoreTable createTable(String tableName, boolean rowTrackingEnabled)
            throws Exception {
        Identifier identifier = Identifier.create("db", tableName);
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("vin", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("hh", DataTypes.STRING())
                        .column("rpt_dt", DataTypes.STRING())
                        .partitionKeys("dt", "hh", "rpt_dt")
                        .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_MOSAIC);
        if (rowTrackingEnabled) {
            schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        } else {
            schemaBuilder
                    .option(CoreOptions.BUCKET.key(), "1")
                    .option(CoreOptions.BUCKET_KEY.key(), "vin");
        }
        catalog.createTable(identifier, schemaBuilder.build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private List<String> readFirstStrings(RecordReader<InternalRow> reader, int expectedFieldCount)
            throws Exception {
        List<String> values = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            try {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    assertThat(row.getFieldCount()).isEqualTo(expectedFieldCount);
                    values.add(row.getString(0).toString());
                }
            } finally {
                batch.releaseBatch();
            }
        }
        return values;
    }

    private void writePhysicalBusinessColumns(FileStoreTable targetTable, int fileCount)
            throws Exception {
        FileIO fileIO = targetTable.fileIO();
        RowType partitionType = targetTable.rowType().project(targetTable.partitionKeys());
        BinaryRow partition =
                new InternalRowSerializer(partitionType)
                        .toBinaryRow(
                                GenericRow.of(
                                        BinaryString.fromString(DT),
                                        BinaryString.fromString(HH),
                                        BinaryString.fromString(RPT_DT)))
                        .copy();

        DataFilePathFactory pathFactory =
                targetTable.store().pathFactory().createDataFilePathFactory(partition, 0);

        RowType physicalType =
                RowType.builder()
                        .field("payload", DataTypes.STRING())
                        .field("vin", DataTypes.STRING())
                        .build();
        MosaicFileFormat format =
                new MosaicFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            Path dataFile = pathFactory.newPath("data-copy-");
            fileIO.mkdirs(dataFile.getParent());
            try (FormatWriter writer =
                    format.createWriterFactory(physicalType)
                            .create(fileIO.newOutputStream(dataFile, false), "zstd")) {
                writer.addElement(
                        GenericRow.of(
                                BinaryString.fromString("payload-" + (i + 1)),
                                BinaryString.fromString(
                                        i == 0 ? VIN : String.format("VIN-%04d", i + 1))));
            }

            DataFileMeta meta =
                    DataFileMeta.forAppend(
                            dataFile.getName(),
                            fileIO.getFileSize(dataFile),
                            1,
                            SimpleStats.EMPTY_STATS,
                            0,
                            0,
                            targetTable.schema().id(),
                            Collections.emptyList(),
                            null,
                            FileSource.APPEND,
                            null,
                            null,
                            null,
                            BUSINESS_COLUMNS);
            assertThat(meta.writeCols()).containsExactlyElementsOf(BUSINESS_COLUMNS);
            files.add(meta);
        }

        CommitMessageImpl message =
                new CommitMessageImpl(
                        partition,
                        0,
                        null,
                        new DataIncrement(files, Collections.emptyList(), Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = targetTable.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }
}
