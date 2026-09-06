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

package org.apache.paimon.benchmark.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.ParquetFastPathCompactRewriter;
import org.apache.paimon.append.ParquetFooterReadExecutor;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/**
 * Benchmark for append-only table compaction: the traditional REWRITE path (decode all rows and
 * re-encode them) versus the Parquet RowGroup copy fast path ({@link
 * ParquetFastPathCompactRewriter}), including its preserve-page-index variant and concurrent footer
 * reads.
 *
 * <p>The parameter sweep is driven by system properties (comma separated lists), all prefixed with
 * {@code rowGroupCopyBenchmark.}:
 *
 * <ul>
 *   <li>{@code shapes}: table shapes, {@code narrow-numeric} (5 numeric columns) and/or {@code
 *       wide-string} (1 int key + 20 string columns). Default {@code narrow-numeric,wide-string}.
 *   <li>{@code codecs}: values of {@code file.compression}. Default {@code none,snappy,zstd}.
 *   <li>{@code rowGroupSizeKb}: {@code parquet.block.size} per combo, in KB. Default {@code 1024}.
 *   <li>{@code rowsPerFile}: rows written per input file (controls file size together with the
 *       shape). Default {@code 50000}.
 *   <li>{@code fileCount}: input files per combo. Default {@code 6}.
 *   <li>{@code iterations}: measured iterations per case. Default {@code 3}.
 *   <li>{@code footerParallelisms}: footer read parallelism values for the copy path. Every value
 *       greater than 1 adds one extra case. Default {@code 1,4}.
 *   <li>{@code verifyContent}: when {@code true}, reads back the output of the rewrite path, the
 *       copy path and its preserve-page-index variant once per combo and asserts row content
 *       equality. The copy path is invoked through {@link ParquetFastPathCompactRewriter} directly
 *       and fails loudly on a fast-path miss, so a silent fallback to the rewrite path cannot fake
 *       a passing verification. Default {@code false}.
 * </ul>
 *
 * <p>Notes on measurement fidelity: footer read executors are created once per combo and reused
 * across iterations, matching the production behavior where {@link BaseAppendFileStoreWrite} caches
 * the executor on the write instance; deleting output files happens after the measured iterations,
 * outside the timed region.
 *
 * <p>Example (full sweep):
 *
 * <pre>{@code
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -Dtest=RowGroupCopyCompactionBenchmark \
 *   -DrowGroupCopyBenchmark.shapes=narrow-numeric,wide-string \
 *   -DrowGroupCopyBenchmark.codecs=none,snappy,zstd \
 *   -DrowGroupCopyBenchmark.rowGroupSizeKb=256,8192,65536 \
 *   -DrowGroupCopyBenchmark.rowsPerFile=50000,200000 \
 *   -DrowGroupCopyBenchmark.iterations=5 test
 * }</pre>
 */
public class RowGroupCopyCompactionBenchmark {

    private static final String PROP_PREFIX = "rowGroupCopyBenchmark.";

    private static final String SHAPE_NARROW_NUMERIC = "narrow-numeric";
    private static final String SHAPE_WIDE_STRING = "wide-string";
    private static final int WIDE_STRING_COLUMNS = 20;

    @TempDir java.nio.file.Path tempDir;

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Test
    public void testRowGroupCopyVsRewrite() throws Exception {
        List<String> shapes = getList("shapes", SHAPE_NARROW_NUMERIC + "," + SHAPE_WIDE_STRING);
        List<String> codecs = getList("codecs", "none,snappy,zstd");
        List<Long> rowGroupSizeKb = getLongList("rowGroupSizeKb", "1024");
        List<Integer> rowsPerFileList = getIntList("rowsPerFile", "50000");
        int fileCount = Integer.parseInt(System.getProperty(PROP_PREFIX + "fileCount", "6"));
        int iterations = Integer.parseInt(System.getProperty(PROP_PREFIX + "iterations", "3"));
        List<Integer> footerParallelisms = getIntList("footerParallelisms", "1,4");
        boolean verifyContent =
                Boolean.parseBoolean(System.getProperty(PROP_PREFIX + "verifyContent", "false"));

        for (String shape : shapes) {
            for (String codec : codecs) {
                for (long rgKb : rowGroupSizeKb) {
                    for (int rowsPerFile : rowsPerFileList) {
                        runCombo(
                                shape,
                                codec,
                                rgKb,
                                rowsPerFile,
                                fileCount,
                                iterations,
                                footerParallelisms,
                                verifyContent);
                    }
                }
            }
        }
    }

    private void runCombo(
            String shape,
            String codec,
            long rowGroupSizeKb,
            int rowsPerFile,
            int fileCount,
            int iterations,
            List<Integer> footerParallelisms,
            boolean verifyContent)
            throws Exception {
        PreparedData prepared = prepareData(shape, codec, rowGroupSizeKb, rowsPerFile, fileCount);
        long totalRows = prepared.files.stream().mapToLong(DataFileMeta::rowCount).sum();
        long totalBytes = prepared.files.stream().mapToLong(DataFileMeta::fileSize).sum();
        String name =
                String.format(
                        "%s_%s_rg%dKB_%dx%d",
                        shape, codec, rowGroupSizeKb, prepared.files.size(), rowsPerFile);
        System.out.printf(
                "Combo %s: inputFiles=%d, inputRows=%d, inputBytes=%d (%.1f MB)%n",
                name, prepared.files.size(), totalRows, totalBytes, totalBytes / 1024.0 / 1024.0);

        Benchmark benchmark =
                new Benchmark(name, totalRows).setNumWarmupIters(1).setOutputPerIteration(false);

        // Footer read executors are created once per combo and reused across iterations, matching
        // the production behavior where BaseAppendFileStoreWrite caches the executor on the write
        // instance. This keeps thread pool creation/teardown out of the measured time.
        Map<Integer, ParquetFooterReadExecutor> footerExecutors = new HashMap<>();
        // Output files are collected during the timed iterations and deleted afterwards, so the
        // deletion cost is not part of the measured compaction time.
        List<DataFileMeta> pendingCleanup = new ArrayList<>();
        try {
            ParquetFooterReadExecutor serialExecutor =
                    footerExecutors.computeIfAbsent(1, ParquetFooterReadExecutor::new);
            benchmark.addCase(
                    "rewrite",
                    iterations,
                    () ->
                            pendingCleanup.addAll(
                                    compactOnce(
                                            prepared, false, false, serialExecutor, totalRows)));
            benchmark.addCase(
                    "row-group-copy",
                    iterations,
                    () ->
                            pendingCleanup.addAll(
                                    compactOnce(prepared, true, false, serialExecutor, totalRows)));
            benchmark.addCase(
                    "row-group-copy-preserve-page-index",
                    iterations,
                    () ->
                            pendingCleanup.addAll(
                                    compactOnce(prepared, true, true, serialExecutor, totalRows)));
            for (int parallelism : footerParallelisms) {
                if (parallelism <= 1) {
                    continue;
                }
                ParquetFooterReadExecutor footerReadExecutor =
                        footerExecutors.computeIfAbsent(
                                parallelism, ParquetFooterReadExecutor::new);
                benchmark.addCase(
                        "row-group-copy-footer-parallel-" + parallelism,
                        iterations,
                        () ->
                                pendingCleanup.addAll(
                                        compactOnce(
                                                prepared,
                                                true,
                                                false,
                                                footerReadExecutor,
                                                totalRows)));
            }
            benchmark.run();
        } finally {
            footerExecutors.values().forEach(ParquetFooterReadExecutor::close);
            FileIO fileIO = prepared.table.fileIO();
            DataFilePathFactory pathFactory =
                    prepared.table
                            .store()
                            .pathFactory()
                            .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
            for (DataFileMeta file : pendingCleanup) {
                fileIO.deleteQuietly(pathFactory.toPath(file));
            }
        }

        if (verifyContent) {
            verifyContent(prepared);
        }
    }

    /**
     * Runs one compaction and returns the produced files without deleting them; the caller deletes
     * the outputs outside the timed region. The row-group-copy cases call {@link
     * ParquetFastPathCompactRewriter#tryRewrite} directly and fail loudly when the fast path is
     * missed (a {@code null} result), so a silently degraded run cannot produce misleading numbers.
     */
    private List<DataFileMeta> compactOnce(
            PreparedData prepared,
            boolean fastPath,
            boolean preservePageIndex,
            ParquetFooterReadExecutor footerReadExecutor,
            long expectedRows) {
        List<DataFileMeta> result;
        try {
            if (!fastPath) {
                BaseAppendFileStoreWrite write =
                        (BaseAppendFileStoreWrite)
                                prepared.table.store().newWrite(UUID.randomUUID().toString());
                try {
                    result =
                            write.compactRewrite(
                                    prepared.partition, UNAWARE_BUCKET, null, prepared.files);
                } finally {
                    write.close();
                }
            } else {
                result = tryFastPathRewrite(prepared, preservePageIndex, footerReadExecutor);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long outputRows = result.stream().mapToLong(DataFileMeta::rowCount).sum();
        if (outputRows != expectedRows) {
            throw new IllegalStateException(
                    String.format(
                            "Row count mismatch after compaction: input %d but output %d",
                            expectedRows, outputRows));
        }
        return result;
    }

    /**
     * Invokes the row-group copy fast path directly and fails loudly on a miss, instead of letting
     * {@link BaseAppendFileStoreWrite#compactRewrite} silently fall back to the rewrite path.
     */
    private static List<DataFileMeta> tryFastPathRewrite(
            PreparedData prepared,
            boolean preservePageIndex,
            ParquetFooterReadExecutor footerReadExecutor) {
        FileStoreTable table = prepared.table;
        FileStoreTable configured =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_PRESERVE_PAGE_INDEX
                                        .key(),
                                String.valueOf(preservePageIndex)));
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        List<DataFileMeta> result =
                ParquetFastPathCompactRewriter.tryRewrite(
                        table.fileIO(),
                        FileFormat.fromIdentifier(
                                FILE_FORMAT_PARQUET, configured.coreOptions().toConfiguration()),
                        configured.rowType(),
                        configured.coreOptions(),
                        prepared.partition,
                        UNAWARE_BUCKET,
                        null,
                        prepared.files,
                        pathFactory,
                        prepared.files.get(0).schemaId(),
                        null,
                        footerReadExecutor);
        if (result == null) {
            throw new IllegalStateException(
                    "RowGroup copy fast path missed, see log for the miss reason.");
        }
        return result;
    }

    /**
     * Reads back the output of the rewrite path, the copy path and its preserve-page-index variant,
     * and asserts that the row contents are identical.
     */
    private void verifyContent(PreparedData prepared) throws Exception {
        List<String> rewriteRows =
                readRows(prepared, compactForVerification(prepared, false, false));
        List<String> copyRows = readRows(prepared, compactForVerification(prepared, true, false));
        List<String> copyPreserveIndexRows =
                readRows(prepared, compactForVerification(prepared, true, true));
        if (!rewriteRows.equals(copyRows)) {
            throw new IllegalStateException(
                    String.format(
                            "Content mismatch between rewrite and row-group copy: %d vs %d rows",
                            rewriteRows.size(), copyRows.size()));
        }
        if (!rewriteRows.equals(copyPreserveIndexRows)) {
            throw new IllegalStateException(
                    String.format(
                            "Content mismatch between rewrite and row-group copy with preserved page"
                                    + " index: %d vs %d rows",
                            rewriteRows.size(), copyPreserveIndexRows.size()));
        }
        System.out.printf("Verified content equality of %d rows.%n", rewriteRows.size());
    }

    private List<DataFileMeta> compactForVerification(
            PreparedData prepared, boolean fastPath, boolean preservePageIndex) throws Exception {
        if (fastPath) {
            // Call the fast path directly so that a miss fails loudly instead of silently falling
            // back to the rewrite path and faking a passing verification.
            try (ParquetFooterReadExecutor footerReadExecutor = new ParquetFooterReadExecutor(1)) {
                return tryFastPathRewrite(prepared, preservePageIndex, footerReadExecutor);
            }
        }
        FileStoreTable table =
                prepared.table.copy(
                        Collections.singletonMap(
                                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(),
                                "false"));
        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
        try {
            return write.compactRewrite(prepared.partition, UNAWARE_BUCKET, null, prepared.files);
        } finally {
            write.close();
        }
    }

    private List<String> readRows(PreparedData prepared, List<DataFileMeta> files)
            throws Exception {
        FileStoreTable table = prepared.table;
        FileIO fileIO = table.fileIO();
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        FormatReaderFactory readerFactory =
                FileFormat.fromIdentifier(
                                FILE_FORMAT_PARQUET, table.coreOptions().toConfiguration())
                        .createReaderFactory(
                                table.rowType(), table.rowType(), Collections.emptyList());
        List<String> rows = new ArrayList<>();
        try {
            for (DataFileMeta file : files) {
                Path path = pathFactory.toPath(file);
                try (FileRecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, path, file.fileSize(), null, null))) {
                    RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
                    while (iterator != null) {
                        InternalRow row;
                        while ((row = iterator.next()) != null) {
                            rows.add(rowToString(row, table.rowType()));
                        }
                        iterator.releaseBatch();
                        iterator = reader.readBatch();
                    }
                }
            }
        } finally {
            for (DataFileMeta file : files) {
                fileIO.deleteQuietly(pathFactory.toPath(file));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private String rowToString(InternalRow row, RowType rowType) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            if (row.isNullAt(i)) {
                sb.append("null");
                continue;
            }
            DataType type = rowType.getTypeAt(i);
            if (type instanceof IntType) {
                sb.append(row.getInt(i));
            } else if (type instanceof BigIntType) {
                sb.append(row.getLong(i));
            } else if (type instanceof DoubleType) {
                sb.append(row.getDouble(i));
            } else if (type instanceof VarCharType) {
                sb.append(row.getString(i));
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        return sb.toString();
    }

    private PreparedData prepareData(
            String shape, String codec, long rowGroupSizeKb, int rowsPerFile, int fileCount)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.resolve(UUID.randomUUID().toString()).toString());
        Schema.Builder schemaBuilder = Schema.newBuilder();
        switch (shape) {
            case SHAPE_NARROW_NUMERIC:
                schemaBuilder.column("id", DataTypes.INT());
                schemaBuilder.column("v1", DataTypes.BIGINT());
                schemaBuilder.column("v2", DataTypes.DOUBLE());
                schemaBuilder.column("v3", DataTypes.INT());
                schemaBuilder.column("v4", DataTypes.BIGINT());
                break;
            case SHAPE_WIDE_STRING:
                schemaBuilder.column("id", DataTypes.INT());
                for (int i = 1; i <= WIDE_STRING_COLUMNS; i++) {
                    schemaBuilder.column("f" + i, DataTypes.STRING());
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown shape: " + shape);
        }
        schemaBuilder.option(CoreOptions.BUCKET.key(), "-1");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), FILE_FORMAT_PARQUET);
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), codec);
        schemaBuilder.option("parquet.block.size", String.valueOf(rowGroupSizeKb * 1024));
        TableSchema tableSchema =
                new FileSystemSchemaManager(fileIO, tablePath).createTable(schemaBuilder.build());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);

        String commitUser = UUID.randomUUID().toString();
        try (StreamTableWrite writer = table.newStreamWriteBuilder().newWrite()) {
            for (int file = 0; file < fileCount; file++) {
                for (int row = 0; row < rowsPerFile; row++) {
                    int id = file * rowsPerFile + row;
                    writer.write(newRow(shape, id));
                }
                try (TableCommitImpl commit = table.newCommit(commitUser)) {
                    commit.commit(writer.prepareCommit(true, file));
                }
            }
        }

        List<DataFileMeta> files =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        return new PreparedData(table, files, BinaryRow.EMPTY_ROW);
    }

    private GenericRow newRow(String shape, int id) {
        switch (shape) {
            case SHAPE_NARROW_NUMERIC:
                return GenericRow.of(
                        id,
                        random.nextLong(0, Long.MAX_VALUE),
                        random.nextGaussian(0, 1),
                        random.nextInt(0, Integer.MAX_VALUE),
                        random.nextLong(0, Long.MAX_VALUE));
            case SHAPE_WIDE_STRING:
                GenericRow row = new GenericRow(1 + WIDE_STRING_COLUMNS);
                row.setField(0, id);
                for (int i = 1; i <= WIDE_STRING_COLUMNS; i++) {
                    row.setField(i, BinaryString.fromString(random.nextHexString(20)));
                }
                return row;
            default:
                throw new IllegalArgumentException("Unknown shape: " + shape);
        }
    }

    private static List<String> getList(String key, String defaultValue) {
        String value = System.getProperty(PROP_PREFIX + key, defaultValue);
        List<String> result = new ArrayList<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    private static List<Long> getLongList(String key, String defaultValue) {
        return getList(key, defaultValue).stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());
    }

    private static List<Integer> getIntList(String key, String defaultValue) {
        return getList(key, defaultValue).stream()
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    private static final class PreparedData {
        private final FileStoreTable table;
        private final List<DataFileMeta> files;
        private final BinaryRow partition;

        private PreparedData(FileStoreTable table, List<DataFileMeta> files, BinaryRow partition) {
            this.table = table;
            this.files = files;
            this.partition = partition;
        }
    }
}
