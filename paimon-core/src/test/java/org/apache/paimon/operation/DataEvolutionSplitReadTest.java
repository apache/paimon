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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
    public void testShouldReadRowSidecarRequiresSmallCountAndLowRatio() {
        DataFileMeta smallFile =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));
        DataFileMeta largeFile =
                createFile(
                        "file2.parquet", 10L, 1_000_000, 1, Collections.singletonList("file2.row"));

        assertTrue(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        largeFile, RoaringBitmap32.bitmapOfRange(0L, 4096L), 4096L, 0.05d));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        smallFile, RoaringBitmap32.bitmapOfRange(0L, 6L), 4096L, 0.05d));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        largeFile, RoaringBitmap32.bitmapOfRange(0L, 4097L), 4096L, 0.05d));
        assertTrue(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        smallFile, RoaringBitmap32.bitmapOfRange(0L, 6L), 64L, 0.25d));
    }

    @Test
    public void testDeletionVectorOnlyAffectsSidecarSelection() {
        DataFileMeta file =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));
        BitmapIndexResult bitmap =
                new BitmapIndexResult(() -> RoaringBitmap32.bitmapOf(1, 2, 4, 6, 8, 9));
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(14L); // file-local position 4 for anchor range starting at 0
        deletionVector.delete(18L); // file-local position 8
        deletionVector.delete(150L); // outside this file

        RoaringBitmap32 fileSelection =
                DataEvolutionSplitRead.fileSelection(
                        file, Collections.singletonList(new Range(12L, 18L)), bitmap);
        assertEquals(RoaringBitmap32.bitmapOf(2, 4, 6, 8), fileSelection);
        assertEquals(
                RoaringBitmap32.bitmapOf(2, 6),
                DataEvolutionSplitRead.sidecarSelection(
                        file, fileSelection, deletionVector, new Range(0L, 199L)));
    }

    @Test
    public void testShouldNotReadRowSidecarWithoutSparseSelection() {
        DataFileMeta file =
                createFile("file1.parquet", 10L, 100, 1, Collections.singletonList("file1.row"));

        assertFalse(DataEvolutionSplitRead.shouldReadRowSidecar(file, null, 4096L, 0.05d));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        file, new RoaringBitmap32(), 4096L, 0.05d));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        createFile("file2.parquet", 10L, 100, 1),
                        RoaringBitmap32.bitmapOf(0),
                        4096L,
                        0.05d));
        assertFalse(
                DataEvolutionSplitRead.shouldReadRowSidecar(
                        createFile(
                                "file3.blob", 10L, 100, 1, Collections.singletonList("file3.row")),
                        RoaringBitmap32.bitmapOf(0),
                        4096L,
                        0.05d));
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDvSelectionNarrowsRowSidecarRead(boolean bitmap64) throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path tableRoot = new Path(tempDir.toUri().toString(), "dv-sidecar-" + bitmap64);
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
        int rowCount = 60_000;
        int liveRowCount = 500;
        String rowSidecarName = "data-0.row";
        Path rowSidecarPath = new Path(bucketPath, rowSidecarName);
        Options rowFormatOptions = new Options();
        rowFormatOptions.set(CoreOptions.FILE_BLOCK_SIZE, MemorySize.ofKibiBytes(4));
        writeFormatFile(fileIO, rowSidecarPath, rowType, rowCount, "row", rowFormatOptions);

        DeletionVector deletionVector =
                bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
        for (int i = 0; i < rowCount - liveRowCount; i++) {
            deletionVector.delete(i);
        }
        Path dvPath = new Path(bucketPath, "data-0.parquet.dv");
        long dvSize;
        try (PositionOutputStream out = fileIO.newOutputStream(dvPath, false);
                DataOutputStream dataOutput = new DataOutputStream(out)) {
            dvSize = deletionVector.serializeTo(dataOutput);
        }

        DataFileMeta dataFile =
                createFile(
                        "data-0.parquet",
                        0L,
                        rowCount,
                        1,
                        Collections.singletonList(rowSidecarName));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Collections.singletonList(dataFile))
                        .withDataDeletionFiles(
                                Collections.singletonList(
                                        new DeletionFile(
                                                dvPath.toString(),
                                                0,
                                                dvSize,
                                                (long) (rowCount - liveRowCount))))
                        .rawConvertible(false)
                        .build();

        CountingFileIO countingFileIO = new CountingFileIO(fileIO, rowSidecarPath);
        DataEvolutionSplitRead splitRead =
                new DataEvolutionSplitRead(
                        countingFileIO,
                        schemaManager,
                        tableSchema,
                        rowType,
                        coreOptions,
                        pathFactory);

        List<Integer> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader = splitRead.createReader(split)) {
            reader.forEachRemaining(row -> actual.add(row.getInt(0)));
        }

        assertEquals(liveRowCount, actual.size());
        assertEquals(1000 + rowCount - liveRowCount, (int) actual.get(0));
        assertEquals(1000 + rowCount - 1, (int) actual.get(actual.size() - 1));
        long sidecarSize = fileIO.getFileStatus(rowSidecarPath).getLen();

        CountingFileIO fullScanFileIO = new CountingFileIO(fileIO, rowSidecarPath);
        RecordReader<InternalRow> fullScanReader =
                FileFormat.fromIdentifier("row", rowFormatOptions)
                        .createReaderFactory(rowType, rowType, null)
                        .createReader(
                                new FormatReaderContext(
                                        fullScanFileIO, rowSidecarPath, sidecarSize, null, null));
        fullScanReader.forEachRemaining(ignored -> {});

        assertTrue(
                countingFileIO.bytesRead() < fullScanFileIO.bytesRead(),
                "Selection read "
                        + countingFileIO.bytesRead()
                        + " sidecar bytes; full scan read "
                        + fullScanFileIO.bytesRead()
                        + ".");
    }

    private static final class CountingFileIO extends LocalFileIO {

        private final LocalFileIO fileIO;
        private final Path countedPath;
        private final AtomicLong bytesRead = new AtomicLong();

        private CountingFileIO(LocalFileIO fileIO, Path countedPath) {
            this.fileIO = fileIO;
            this.countedPath = countedPath;
        }

        private long bytesRead() {
            return bytesRead.get();
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream input = fileIO.newInputStream(path);
            if (!countedPath.equals(path)) {
                return input;
            }
            return new SeekableInputStream() {
                @Override
                public void seek(long desired) throws IOException {
                    input.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return input.getPos();
                }

                @Override
                public int read(byte[] buffer, int offset, int length) throws IOException {
                    int read = input.read(buffer, offset, length);
                    if (read > 0) {
                        bytesRead.addAndGet(read);
                    }
                    return read;
                }

                @Override
                public int read() throws IOException {
                    int read = input.read();
                    if (read >= 0) {
                        bytesRead.incrementAndGet();
                    }
                    return read;
                }

                @Override
                public void close() throws IOException {
                    input.close();
                }
            };
        }
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
