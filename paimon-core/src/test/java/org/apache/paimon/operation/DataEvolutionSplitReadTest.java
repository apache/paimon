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
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataEvolutionSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDifferentRowIdRange() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 100, 10);
        DataFileMeta file2 = createFile("file2.parquet", 1L, 50, 20);

        List<DataFileMeta> files = Arrays.asList(file1, file2);
        assertThatThrownBy(() -> DataEvolutionSplitRead.mergeRangesAndSort(files))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSplitWithSameFirstRowId() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 10);
        DataFileMeta file2 = createFile("file2.parquet", 1L, 1, 20);
        DataFileMeta file3 = createFile("file3.parquet", 1L, 1, 30);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3);
        List<List<DataFileMeta>> result = DataEvolutionSplitRead.mergeRangesAndSort(files);

        assertEquals(1, result.size());
        assertEquals(Arrays.asList(file3, file2, file1), result.get(0));
    }

    @Test
    public void testSplitWithMixedFirstRowId() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 1);
        DataFileMeta file2 = createFile("file2.parquet", 2L, 1, 2);
        DataFileMeta file3 = createFile("file3.parquet", 1L, 1, 3);
        DataFileMeta file4 = createFile("file4.parquet", 2L, 1, 4);
        DataFileMeta file5 = createFile("file5.parquet", 3L, 1, 5);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5);
        List<List<DataFileMeta>> result = DataEvolutionSplitRead.mergeRangesAndSort(files);

        assertEquals(3, result.size());
        assertEquals(Arrays.asList(file3, file1), result.get(0));
        assertEquals(Arrays.asList(file4, file2), result.get(1));
        assertEquals(Collections.singletonList(file5), result.get(2));
    }

    @Test
    public void testSplitWithComplexScenario() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 1, 1);
        DataFileMeta file2 = createFile("file2.parquet", 2L, 1, 3);
        DataFileMeta file3 = createFile("file3.parquet", 3L, 1, 5);
        DataFileMeta file4 = createFile("file4.parquet", 1L, 1, 2);
        DataFileMeta file5 = createFile("file5.parquet", 4L, 1, 8);
        DataFileMeta file6 = createFile("file6.parquet", 2L, 1, 4);
        DataFileMeta file7 = createFile("file7.parquet", 3L, 1, 6);
        DataFileMeta file8 = createFile("file8.parquet", 3L, 1, 7);
        DataFileMeta file9 = createFile("file9.parquet", 5L, 1, 9);

        List<DataFileMeta> files =
                Arrays.asList(file1, file2, file3, file4, file5, file6, file7, file8, file9);
        List<List<DataFileMeta>> result = DataEvolutionSplitRead.mergeRangesAndSort(files);

        assertEquals(5, result.size());
        assertEquals(Arrays.asList(file4, file1), result.get(0));
        assertEquals(Arrays.asList(file6, file2), result.get(1));
        assertEquals(Arrays.asList(file8, file7, file3), result.get(2));
        assertEquals(Collections.singletonList(file5), result.get(3));
        assertEquals(Collections.singletonList(file9), result.get(4));
    }

    @Test
    public void testSplitWithMultipleBlobFilesPerGroup() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 10, 1);
        DataFileMeta file2 = createFile("file2.blob", 1L, 1, 1);
        DataFileMeta file3 = createFile("file3.blob", 2L, 9, 1);
        DataFileMeta file4 = createFile("file4.parquet", 20L, 10, 2);
        DataFileMeta file5 = createFile("file5.blob", 20L, 5, 2);
        DataFileMeta file6 = createFile("file6.blob", 25L, 5, 2);
        DataFileMeta file7 = createFile("file7.parquet", 1L, 10, 3);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5, file6, file7);
        List<List<DataFileMeta>> result = DataEvolutionSplitRead.mergeRangesAndSort(files);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList(file7, file1, file2, file3), result.get(0));
        assertEquals(Arrays.asList(file4, file5, file6), result.get(1));
    }

    @Test
    public void testSplitWithMultipleVectorStoreFilesPerGroup() {
        DataFileMeta file1 = createFile("file1.parquet", 1L, 10, 1);
        DataFileMeta file2 = createFile("file2.vector.json", 1L, 1, 1);
        DataFileMeta file3 = createFile("file3.vector.json", 2L, 9, 1);
        DataFileMeta file4 = createFile("file4.parquet", 20L, 10, 2);
        DataFileMeta file5 = createFile("file5.vector.json", 20L, 5, 2);
        DataFileMeta file6 = createFile("file6.vector.json", 25L, 5, 2);
        DataFileMeta file7 = createFile("file7.parquet", 1L, 10, 3);

        List<DataFileMeta> files = Arrays.asList(file1, file2, file3, file4, file5, file6, file7);
        List<List<DataFileMeta>> result = DataEvolutionSplitRead.mergeRangesAndSort(files);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList(file7, file1, file2, file3), result.get(0));
        assertEquals(Arrays.asList(file4, file5, file6), result.get(1));
    }

    @Test
    public void testSplitWithDedicatedFilesSpanningNormalGroups() {
        DataFileMeta first = createFile("first.parquet", 0, 4, 3);
        DataFileMeta middle = createFile("middle.parquet", 4, 4, 3);
        DataFileMeta last = createFile("last.parquet", 8, 4, 3);
        DataFileMeta blob = createFile("blob.blob", 0, 12, 1);
        DataFileMeta vector = createFile("vector.vector.json", 0, 12, 1);
        DataFileMeta vectorUpdate = createFile("update.vector.json", 4, 4, 2);

        List<List<DataFileMeta>> groups =
                DataEvolutionSplitRead.mergeRangesAndSort(
                        Arrays.asList(blob, vector, middle, last, first, vectorUpdate));

        assertEquals(
                Arrays.asList(
                        Arrays.asList(first, blob, vector),
                        Arrays.asList(middle, blob, vectorUpdate, vector),
                        Arrays.asList(last, blob, vector)),
                groups);
        // Associations must retain physical file offsets for readers of the second and third group.
        assertEquals(new Range(0, 11), groups.get(2).get(1).nonNullRowIdRange());
    }

    @ParameterizedTest
    @CsvSource({
        "false,4,false",
        "true,4,false",
        "false,12,false",
        "true,12,false",
        "false,4,true",
        "true,4,true",
        "false,12,true",
        "true,12,true"
    })
    public void testReadSpanningDedicatedFiles(
            boolean indexed, int normalRowCount, boolean renameBeforeUpdate) throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path tablePath = new Path(tempDir.resolve("spanning").toUri());
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tablePath);
        TableSchema schema =
                schemaManager.createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("blob", DataTypes.BLOB())
                                .column("vector", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                                .column("vector2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .build());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);
        FileStorePathFactory pathFactory = table.store().pathFactory();
        Path bucketPath = pathFactory.bucketPath(EMPTY_ROW, 0);
        fileIO.mkdirs(bucketPath);
        RowType rowType = schema.logicalRowType();
        List<DataFileMeta> files = new ArrayList<>();
        for (int from = 0; from < 12; from += normalRowCount) {
            files.add(
                    writeProjectedFile(
                            fileIO,
                            bucketPath,
                            "normal-" + from + ".parquet",
                            "parquet",
                            rowType.project("id"),
                            from,
                            normalRowCount,
                            3,
                            GenericRow::of));
        }
        files.add(
                writeProjectedFile(
                        fileIO,
                        bucketPath,
                        "base.blob",
                        "blob",
                        rowType.project("blob"),
                        0,
                        12,
                        1,
                        i -> GenericRow.of(new BlobData(new byte[] {(byte) i}))));
        files.add(
                writeProjectedFile(
                        fileIO,
                        bucketPath,
                        "update.blob",
                        "blob",
                        rowType.project("blob"),
                        2,
                        8,
                        2,
                        i ->
                                GenericRow.of(
                                        i % 2 == 0
                                                ? BlobPlaceholder.INSTANCE
                                                : new BlobData(new byte[] {(byte) (i + 20)}))));
        files.add(
                writeProjectedFile(
                        fileIO,
                        bucketPath,
                        "base.vector.json",
                        "json",
                        rowType.project("vector", "vector2"),
                        0,
                        12,
                        1,
                        i ->
                                GenericRow.of(
                                        BinaryVector.fromPrimitiveArray(new float[] {i, i + 1}),
                                        BinaryVector.fromPrimitiveArray(
                                                new float[] {i + 100, i + 101}))));
        if (renameBeforeUpdate) {
            schema =
                    schemaManager.commitChanges(
                            SchemaChange.renameColumn("vector", "renamed_vector"));
        }
        files.add(
                writeProjectedFile(
                        fileIO,
                        bucketPath,
                        "update.vector.json",
                        "json",
                        schema.logicalRowType()
                                .project(renameBeforeUpdate ? "renamed_vector" : "vector"),
                        4,
                        4,
                        2,
                        i ->
                                GenericRow.of(
                                        BinaryVector.fromPrimitiveArray(
                                                new float[] {i + 20, i + 21})),
                        schema.id()));
        if (!renameBeforeUpdate) {
            schema =
                    schemaManager.commitChanges(
                            SchemaChange.renameColumn("vector", "renamed_vector"));
        }
        RowType allReadType = SpecialFields.rowTypeWithRowId(schema.logicalRowType());
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(files)
                        .rawConvertible(false)
                        .build();
        for (boolean vectorOnly : new boolean[] {false, true}) {
            RowType readType =
                    vectorOnly
                            ? allReadType.project(
                                    "renamed_vector", "vector2", SpecialFields.ROW_ID.name())
                            : allReadType;
            DataSplit projectedSplit =
                    vectorOnly
                            ? dataSplit
                                    .filterDataFile(
                                            file ->
                                                    org.apache.paimon.types.VectorType
                                                            .isVectorStoreFile(file.fileName()))
                                    .get()
                            : dataSplit;
            Split split =
                    indexed
                            ? new IndexedSplit(
                                    projectedSplit,
                                    Arrays.asList(
                                            new Range(1, 2), new Range(5, 5), new Range(8, 9)),
                                    null)
                            : projectedSplit;
            DataEvolutionSplitRead splitRead =
                    new DataEvolutionSplitRead(
                            fileIO,
                            schemaManager,
                            schema,
                            readType,
                            table.coreOptions(),
                            pathFactory);
            List<Integer> ids = new ArrayList<>();
            try (RecordReader<InternalRow> reader = splitRead.createReader(split)) {
                reader.forEachRemaining(
                        row -> {
                            int id = (int) row.getLong(vectorOnly ? 2 : 4);
                            ids.add(id);
                            if (!vectorOnly) {
                                assertEquals(id, row.getInt(0));
                                int expectedBlob = id >= 2 && id < 10 && id % 2 != 0 ? id + 20 : id;
                                assertEquals((byte) expectedBlob, row.getBlob(1).toData()[0]);
                            }
                            int expectedVector = id >= 4 && id < 8 ? id + 20 : id;
                            org.assertj.core.api.Assertions.assertThat(
                                            row.getVector(vectorOnly ? 0 : 2).toFloatArray())
                                    .containsExactly(expectedVector, expectedVector + 1);
                            org.assertj.core.api.Assertions.assertThat(
                                            row.getVector(vectorOnly ? 1 : 3).toFloatArray())
                                    .containsExactly(id + 100, id + 101);
                        });
            }
            assertEquals(
                    indexed
                            ? Arrays.asList(1, 2, 5, 8, 9)
                            : IntStream.range(0, 12).boxed().collect(Collectors.toList()),
                    ids);
        }
    }

    private static DataFileMeta writeProjectedFile(
            LocalFileIO fileIO,
            Path bucketPath,
            String name,
            String formatIdentifier,
            RowType writeType,
            int firstRowId,
            int rowCount,
            int sequence,
            IntFunction<GenericRow> rowFactory)
            throws IOException {
        return writeProjectedFile(
                fileIO,
                bucketPath,
                name,
                formatIdentifier,
                writeType,
                firstRowId,
                rowCount,
                sequence,
                rowFactory,
                0);
    }

    private static DataFileMeta writeProjectedFile(
            LocalFileIO fileIO,
            Path bucketPath,
            String name,
            String formatIdentifier,
            RowType writeType,
            int firstRowId,
            int rowCount,
            int sequence,
            IntFunction<GenericRow> rowFactory,
            long schemaId)
            throws IOException {
        Path filePath = new Path(bucketPath, name);
        FileFormat format = FileFormat.fromIdentifier(formatIdentifier, new Options());
        try (PositionOutputStream output = fileIO.newOutputStream(filePath, false)) {
            FormatWriter writer = format.createWriterFactory(writeType).create(output, "none");
            for (int i = firstRowId; i < firstRowId + rowCount; i++) {
                writer.addElement(rowFactory.apply(i));
            }
            writer.close();
        }
        return DataFileMeta.forAppend(
                name,
                fileIO.getFileStatus(filePath).getLen(),
                rowCount,
                SimpleStats.EMPTY_STATS,
                sequence,
                sequence,
                schemaId,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                (long) firstRowId,
                writeType.getFieldNames());
    }

    @Test
    public void testRowSidecarFileName() {
        DataFileMeta file =
                createFile(
                        "file1.parquet",
                        1L,
                        100,
                        1,
                        Arrays.asList("file1.parquet.index", "file1.row"));

        assertEquals("file1.row", DataEvolutionSplitRead.rowSidecarFileName(file));
    }

    @Test
    public void testRowSidecarFileNameWithNoOrAmbiguousSidecar() {
        DataFileMeta noSidecar =
                createFile(
                        "file1.parquet",
                        1L,
                        100,
                        1,
                        Arrays.asList("file1.parquet.index", "lookup.sst"));
        DataFileMeta ambiguousSidecar =
                createFile(
                        "file1.parquet", 1L, 100, 1, Arrays.asList("file1.row", "file1-copy.row"));

        assertNull(DataEvolutionSplitRead.rowSidecarFileName(noSidecar));
        assertNull(DataEvolutionSplitRead.rowSidecarFileName(ambiguousSidecar));
    }

    @Test
    public void testShouldReadRowSidecarForSparseRowSelection() {
        DataFileMeta file =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));

        assertTrue(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        file, Arrays.asList(new Range(10L, 10L), new Range(42L, 42L))));
    }

    @Test
    public void testShouldReadRowSidecarRequiresSmallCountAndLowRatio() {
        DataFileMeta smallFile =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));
        DataFileMeta largeFile =
                createFile(
                        "file2.parquet", 10L, 1_000_000, 1, Collections.singletonList("file2.row"));

        assertTrue(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        largeFile, Collections.singletonList(new Range(10L, 4105L))));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        smallFile, Collections.singletonList(new Range(10L, 15L))));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        largeFile, Collections.singletonList(new Range(10L, 4106L))));
        assertTrue(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        smallFile, Collections.singletonList(new Range(10L, 15L)), 64L, 0.25d));
    }

    @Test
    public void testShouldNotReadRowSidecarWithoutSparseSelection() {
        DataFileMeta file =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));

        assertFalse(DataEvolutionSplitRead.shouldReadRowSidecar(file, null));
        assertFalse(DataEvolutionSplitRead.shouldReadRowSidecar(file, Collections.emptyList()));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        file, Collections.singletonList(new Range(10L, 109L))));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        createFile("file2.parquet", 10L, 100, 1),
                        Collections.singletonList(new Range(10L, 10L))));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        createFile(
                                "file3.blob", 10L, 100, 1, Collections.singletonList("file3.row")),
                        Collections.singletonList(new Range(10L, 10L))));
    }

    @Test
    public void testSelectedRowCountMergesOverlappingRanges() {
        DataFileMeta file = createFile("file1.parquet", 10L, 100, 1);

        assertEquals(
                12,
                DataEvolutionSplitRead.selectedRowCount(
                        file,
                        Arrays.asList(
                                new Range(5L, 12L), new Range(12L, 15L), new Range(20L, 25L))));
    }

    @Test
    public void testSparseRowIdReadUsesRowSidecar() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path tableRoot = new Path(tempDir.toUri().toString());
        CoreOptions coreOptions = new CoreOptions(new Options());
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        tableRoot,
                        RowType.of(),
                        coreOptions.partitionDefaultName(),
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);

        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .build();
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tableRoot);
        TableSchema tableSchema = schemaManager.createTable(schema);
        RowType rowType = tableSchema.logicalRowType();

        Path bucketPath = pathFactory.bucketPath(EMPTY_ROW, 0);
        fileIO.mkdirs(bucketPath);
        String rowSidecarName = "data-0.row";
        writeRowFile(fileIO, new Path(bucketPath, rowSidecarName), rowType, 100);

        DataFileMeta dataFile =
                createFile(
                        "data-0.parquet", 10L, 100, 1, Collections.singletonList(rowSidecarName));
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Collections.singletonList(dataFile))
                        .rawConvertible(false)
                        .build();

        DataEvolutionSplitRead splitRead =
                new DataEvolutionSplitRead(
                        fileIO, schemaManager, tableSchema, rowType, coreOptions, pathFactory);
        IndexedSplit indexedSplit =
                new IndexedSplit(
                        dataSplit, Arrays.asList(new Range(10L, 10L), new Range(42L, 42L)), null);

        List<Integer> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader = splitRead.createReader(indexedSplit)) {
            reader.forEachRemaining(row -> actual.add(row.getInt(0)));
        }

        assertEquals(Arrays.asList(1000, 1032), actual);
    }

    @Test
    public void testSparseRowIdReadUsesParquetRowsAtSelectedPositions() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path tableRoot = new Path(tempDir.toUri().toString(), "sparse-parquet");
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, "parquet");
        CoreOptions coreOptions = new CoreOptions(options);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        tableRoot,
                        RowType.of(),
                        coreOptions.partitionDefaultName(),
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);

        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.ARRAY(DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tableRoot);
        TableSchema tableSchema = schemaManager.createTable(schema);
        RowType rowType = tableSchema.logicalRowType();

        Path bucketPath = pathFactory.bucketPath(EMPTY_ROW, 0);
        fileIO.mkdirs(bucketPath);
        String fileName = "data-0.parquet";
        Path filePath = new Path(bucketPath, fileName);
        Options parquetOptions = new Options();
        parquetOptions.set("parquet.block.size", "65536");
        parquetOptions.set("parquet.page.size", "4096");
        parquetOptions.set("parquet.writer.version", "v2");
        parquetOptions.set("parquet.page.size.row.check.min", "100");
        writeNestedParquetFile(fileIO, filePath, rowType, 10_000, parquetOptions);

        DataFileMeta dataFile =
                createFile(fileName, fileIO.getFileStatus(filePath).getLen(), 10L, 10_000, 1);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Collections.singletonList(dataFile))
                        .rawConvertible(false)
                        .build();

        DataEvolutionSplitRead splitRead =
                new DataEvolutionSplitRead(
                        fileIO, schemaManager, tableSchema, rowType, coreOptions, pathFactory);
        IndexedSplit indexedSplit =
                new IndexedSplit(
                        dataSplit,
                        Arrays.asList(new Range(10L, 10L), new Range(4210L, 4210L)),
                        new float[] {1.0F, 0.5F});

        List<String> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader = splitRead.createReader(indexedSplit)) {
            reader.forEachRemaining(
                    row ->
                            actual.add(
                                    String.format(
                                            "%d:[%d,%d]",
                                            row.getInt(0),
                                            row.getArray(1).getInt(0),
                                            row.getArray(1).getInt(1))));
        }

        assertEquals(Arrays.asList("1000:[0,1]", "5200:[4200,4201]"), actual);
    }

    private static void writeNestedParquetFile(
            LocalFileIO fileIO, Path path, RowType rowType, int rowCount, Options options)
            throws IOException {
        FileFormat format = FileFormat.fromIdentifier("parquet", options);
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < rowCount; i++) {
                writer.addElement(GenericRow.of(1000 + i, new GenericArray(new int[] {i, i + 1})));
            }
            writer.close();
        }
    }

    private static void writeRowFile(LocalFileIO fileIO, Path path, RowType rowType, int rowCount)
            throws IOException {
        writeFormatFile(fileIO, path, rowType, rowCount, "row");
    }

    private static void writeFormatFile(
            LocalFileIO fileIO, Path path, RowType rowType, int rowCount, String formatIdentifier)
            throws IOException {
        writeFormatFile(fileIO, path, rowType, rowCount, formatIdentifier, new Options());
    }

    private static void writeFormatFile(
            LocalFileIO fileIO,
            Path path,
            RowType rowType,
            int rowCount,
            String formatIdentifier,
            Options options)
            throws IOException {
        FileFormat format = FileFormat.fromIdentifier(formatIdentifier, options);
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < rowCount; i++) {
                writer.addElement(GenericRow.of(1000 + i, BinaryString.fromString("row-" + i)));
            }
            writer.close();
        }
    }

    private static DataFileMeta createFile(
            String name, long firstRowId, long rowCount, long maxSequence) {
        return createFile(name, 10000L, firstRowId, rowCount, maxSequence);
    }

    private static DataFileMeta createFile(
            String name, long fileSize, long firstRowId, long rowCount, long maxSequence) {
        return createFile(
                name, fileSize, firstRowId, rowCount, maxSequence, Collections.emptyList());
    }

    private static DataFileMeta createFile(
            String name,
            long firstRowId,
            long rowCount,
            long maxSequence,
            List<String> extraFiles) {
        return createFile(name, 10000L, firstRowId, rowCount, maxSequence, extraFiles);
    }

    private static DataFileMeta createFile(
            String name,
            long fileSize,
            long firstRowId,
            long rowCount,
            long maxSequence,
            List<String> extraFiles) {
        return DataFileMeta.create(
                        name,
                        fileSize,
                        (int) rowCount,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        null,
                        0L,
                        maxSequence,
                        0,
                        0,
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        firstRowId,
                        null)
                .copy(extraFiles);
    }
}
