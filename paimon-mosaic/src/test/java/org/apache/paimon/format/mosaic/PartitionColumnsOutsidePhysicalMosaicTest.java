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
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

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
    void testReadPhysicalAndManifestColumns() throws Exception {
        assertRows(
                table.newReadBuilder(),
                Arrays.asList("payload", "vin", "dt", "hh", "rpt_dt"),
                Collections.singletonList(row(PAYLOAD, VIN, DT, HH, RPT_DT)));

        assertRows(
                table.newReadBuilder().withReadType(table.rowType().project(BUSINESS_COLUMNS)),
                BUSINESS_COLUMNS,
                Collections.singletonList(row(PAYLOAD, VIN)));

        assertRows(
                table.newReadBuilder().withProjection(new int[] {4, 2}),
                Arrays.asList("rpt_dt", "dt"),
                Collections.singletonList(row(RPT_DT, DT)));
    }

    @Test
    void testReadTypeChangeAffectsOnlyFutureReaders() throws Exception {
        FileStoreTable trackingTable = createTable("row_tracking", true);
        writePhysicalBusinessColumns(trackingTable, 2);

        DataSplit split = planSingleSplit(trackingTable.newReadBuilder(), 2);
        InnerTableRead read = trackingTable.newRead();
        RowType trackingType = SpecialFields.rowTypeWithRowTracking(trackingTable.rowType());
        read.withReadType(trackingType.project("payload", "dt", SpecialFields.ROW_ID.name()));

        try (RecordReader<InternalRow> existingReader = read.createReader(split)) {
            read.withReadType(trackingTable.rowType().project("vin"));

            assertThat(readRows(existingReader, 3, 2))
                    .containsExactlyInAnyOrder(row(PAYLOAD, DT), row("payload-2", DT));
        }

        try (RecordReader<InternalRow> updatedReader = read.createReader(split)) {
            assertThat(readRows(updatedReader, 1, 1))
                    .containsExactlyInAnyOrder(row(VIN), row("VIN-0002"));
        }
    }

    private void assertRows(
            ReadBuilder readBuilder, List<String> expectedFields, List<List<String>> expectedRows)
            throws Exception {
        assertThat(readBuilder.readType().getFieldNames())
                .containsExactlyElementsOf(expectedFields);
        DataSplit split = planSingleSplit(readBuilder, 1);

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
            assertThat(readRows(reader, expectedFields.size(), expectedFields.size()))
                    .containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    private DataSplit planSingleSplit(ReadBuilder readBuilder, int expectedFiles) {
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0)).isInstanceOf(DataSplit.class);

        DataSplit dataSplit = (DataSplit) splits.get(0);
        assertThat(dataSplit.dataFiles())
                .hasSize(expectedFiles)
                .allSatisfy(
                        file ->
                                assertThat(file.writeCols())
                                        .containsExactlyElementsOf(BUSINESS_COLUMNS));
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

    private List<List<String>> readRows(
            RecordReader<InternalRow> reader, int expectedFieldCount, int stringFieldCount)
            throws Exception {
        List<List<String>> rows = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            try {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    assertThat(row.getFieldCount()).isEqualTo(expectedFieldCount);
                    List<String> values = new ArrayList<>();
                    for (int i = 0; i < stringFieldCount; i++) {
                        values.add(row.getString(i).toString());
                    }
                    rows.add(values);
                }
            } finally {
                batch.releaseBatch();
            }
        }
        return rows;
    }

    private static List<String> row(String... values) {
        return Arrays.asList(values);
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
        for (int i = 1; i <= fileCount; i++) {
            Path dataFile = pathFactory.newPath("data-copy-");
            fileIO.mkdirs(dataFile.getParent());
            try (FormatWriter writer =
                    format.createWriterFactory(physicalType)
                            .create(fileIO.newOutputStream(dataFile, false), "zstd")) {
                writer.addElement(
                        GenericRow.of(
                                BinaryString.fromString("payload-" + i),
                                BinaryString.fromString(String.format("VIN-%04d", i))));
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

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
