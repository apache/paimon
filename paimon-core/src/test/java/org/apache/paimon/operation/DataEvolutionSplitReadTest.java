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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        SchemaManager schemaManager = new SchemaManager(fileIO, tableRoot);
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

    private static void writeRowFile(LocalFileIO fileIO, Path path, RowType rowType, int rowCount)
            throws IOException {
        FileFormat format = FileFormat.fromIdentifier("row", new Options());
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
        return createFile(name, firstRowId, rowCount, maxSequence, Collections.emptyList());
    }

    private static DataFileMeta createFile(
            String name,
            long firstRowId,
            long rowCount,
            long maxSequence,
            List<String> extraFiles) {
        return DataFileMeta.create(
                        name,
                        10000L,
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
