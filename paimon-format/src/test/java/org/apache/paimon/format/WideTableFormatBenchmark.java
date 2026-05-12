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

package org.apache.paimon.format;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.mosaic.MosaicFileFormat;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark to compare file sizes and projection read performance between Parquet, ORC and Mosaic
 * for wide tables (10,000+ columns).
 *
 * <p>Run manually: {@code mvn exec:java -pl paimon-format
 * -Dexec.mainClass="org.apache.paimon.format.WideTableFileFormatSizeTest"
 * -Dexec.classpathScope="test"}
 */
public class WideTableFormatBenchmark {

    private static final int COLUMN_COUNT = 10000;
    private static final int ROW_COUNT = 10;
    private static final String COMPRESSION = "zstd";

    public static void main(String[] args) throws Exception {
        run(WideTableFormatBenchmark::fileSizeComparison);
        run(tempDir -> projectionReadPerformance(tempDir, 500));
        run(tempDir -> projectionReadPerformance(tempDir, 4500));
    }

    private static void run(Runner runner) throws IOException {
        java.nio.file.Path tempDir = Files.createTempDirectory("mosaic-benchmark");
        try {
            runner.run(tempDir);
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static void fileSizeComparison(java.nio.file.Path tempDir) throws IOException {
        RowType rowType = buildWideRowType();
        int fieldCount = rowType.getFieldCount();
        LocalFileIO fileIO = new LocalFileIO();

        long parquetSize =
                writeParquet(
                        rowType,
                        ROW_COUNT,
                        new Path(tempDir.toString(), "wide_table.parquet"),
                        fileIO);

        long orcSize =
                writeOrc(
                        rowType, ROW_COUNT, new Path(tempDir.toString(), "wide_table.orc"), fileIO);

        Path mosaicPath = new Path(tempDir.toString(), "wide_table.mosaic");
        long mosaicSize = writeMosaic(rowType, ROW_COUNT, mosaicPath, fileIO);

        System.out.println("=== Wide Table File Size Comparison ===");
        System.out.println("Columns: " + COLUMN_COUNT + ", Rows: " + ROW_COUNT);
        System.out.println("Column name avg length: ~80 bytes");
        System.out.println("Compression: " + COMPRESSION + " (level 9)");
        System.out.println("---------------------------------------");
        System.out.printf("Parquet:    %,d bytes (%.1f KB)%n", parquetSize, parquetSize / 1024.0);
        System.out.printf("ORC:        %,d bytes (%.1f KB)%n", orcSize, orcSize / 1024.0);
        System.out.printf("Mosaic:     %,d bytes (%.1f KB)%n", mosaicSize, mosaicSize / 1024.0);
        System.out.println("---------------------------------------");

        // verify Mosaic correctness
        List<InternalRow> mosaicResult = readMosaic(rowType, rowType, mosaicPath, fileIO);
        check(mosaicResult.size() == ROW_COUNT, "Row count mismatch");
        for (int r = 0; r < ROW_COUNT; r++) {
            GenericRow expected = generateRow(r, fieldCount);
            for (int c = 0; c < COLUMN_COUNT; c++) {
                assertCellEqual(mosaicResult.get(r), expected, c);
            }
        }
        System.out.println("Correctness check: PASSED");
    }

    private static void projectionReadPerformance(java.nio.file.Path tempDir, int rows)
            throws IOException {
        RowType rowType = buildWideRowType();
        LocalFileIO fileIO = new LocalFileIO();

        Path parquetPath = new Path(tempDir.toString(), "proj_test.parquet");
        long parquetFileSize = writeParquet(rowType, rows, parquetPath, fileIO);

        Path orcPath = new Path(tempDir.toString(), "proj_test.orc");
        long orcFileSize = writeOrc(rowType, rows, orcPath, fileIO);

        Path mosaicPath = new Path(tempDir.toString(), "proj_test.mosaic");
        long mosaicFileSize = writeMosaic(rowType, rows, mosaicPath, fileIO);

        int[] projected10Cols = {0, 100, 500, 1000, 2000, 5000, 7000, 8000, 9000, 9999};
        int[] projected1Col = {1000};

        System.out.printf("\n=== Projection Read Performance (%d rows) ===%n", rows);
        System.out.printf(
                "File size - Parquet: %.1f MB, ORC: %.1f MB, Mosaic: %.1f MB%n",
                parquetFileSize / 1024.0 / 1024.0,
                orcFileSize / 1024.0 / 1024.0,
                mosaicFileSize / 1024.0 / 1024.0);
        System.out.println("---------------------------------------");

        benchmarkProjection(
                rowType, projected10Cols, rows, parquetPath, orcPath, mosaicPath, fileIO);
        benchmarkProjection(rowType, projected1Col, rows, parquetPath, orcPath, mosaicPath, fileIO);
    }

    private static void benchmarkProjection(
            RowType rowType,
            int[] projectedColumns,
            int rows,
            Path parquetPath,
            Path orcPath,
            Path mosaicPath,
            LocalFileIO fileIO)
            throws IOException {
        RowType projectedType = rowType.project(projectedColumns);

        int warmup = 3;
        int iterations = 10;

        for (int i = 0; i < warmup; i++) {
            readParquetProjected(rowType, projectedType, parquetPath, fileIO);
        }
        long parquetStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            readParquetProjected(rowType, projectedType, parquetPath, fileIO);
        }
        long parquetTimeNs = (System.nanoTime() - parquetStart) / iterations;

        for (int i = 0; i < warmup; i++) {
            readOrcProjected(rowType, projectedType, orcPath, fileIO);
        }
        long orcStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            readOrcProjected(rowType, projectedType, orcPath, fileIO);
        }
        long orcTimeNs = (System.nanoTime() - orcStart) / iterations;

        for (int i = 0; i < warmup; i++) {
            readMosaic(rowType, projectedType, mosaicPath, fileIO);
        }
        long mosaicStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            readMosaic(rowType, projectedType, mosaicPath, fileIO);
        }
        long mosaicTimeNs = (System.nanoTime() - mosaicStart) / iterations;

        System.out.printf(
                "Project %2d / %d cols: Parquet %,d us, ORC %,d us, Mosaic %,d us%n",
                projectedColumns.length,
                COLUMN_COUNT,
                parquetTimeNs / 1000,
                orcTimeNs / 1000,
                mosaicTimeNs / 1000);

        // verify projection results
        List<InternalRow> parquetResult =
                readParquetProjected(rowType, projectedType, parquetPath, fileIO);
        List<InternalRow> mosaicResult = readMosaic(rowType, projectedType, mosaicPath, fileIO);
        check(
                mosaicResult.size() == parquetResult.size(),
                "Projection row count mismatch: parquet="
                        + parquetResult.size()
                        + " mosaic="
                        + mosaicResult.size());
        for (int r = 0; r < parquetResult.size(); r++) {
            for (int c = 0; c < projectedColumns.length; c++) {
                int origCol = projectedColumns[c];
                if (isIntColumn(origCol)) {
                    check(
                            mosaicResult.get(r).getInt(c) == parquetResult.get(r).getInt(c),
                            "INT mismatch at row=" + r + " col=" + c);
                } else {
                    check(
                            mosaicResult
                                    .get(r)
                                    .getString(c)
                                    .toString()
                                    .equals(parquetResult.get(r).getString(c).toString()),
                            "STRING mismatch at row=" + r + " col=" + c);
                }
            }
        }
    }

    // ==================== Parquet helpers ====================

    private static long writeParquet(RowType rowType, int rowCount, Path path, LocalFileIO fileIO)
            throws IOException {
        ParquetFileFormat parquet = new ParquetFileFormat(createFormatContext());
        FormatWriterFactory writerFactory = parquet.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, COMPRESSION);
        int fieldCount = rowType.getFieldCount();
        for (int r = 0; r < rowCount; r++) {
            writer.addElement(generateRow(r, fieldCount));
        }
        writer.close();
        out.close();
        return fileIO.getFileSize(path);
    }

    private static List<InternalRow> readParquetProjected(
            RowType fullType, RowType projectedType, Path path, LocalFileIO fileIO)
            throws IOException {
        ParquetFileFormat parquet = new ParquetFileFormat(createFormatContext());
        RecordReader<InternalRow> reader =
                parquet.createReaderFactory(fullType, projectedType, null)
                        .createReader(
                                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    Object[] fields = new Object[projectedType.getFieldCount()];
                    for (int i = 0; i < fields.length; i++) {
                        if (row.isNullAt(i)) {
                            fields[i] = null;
                        } else if (projectedType.getTypeAt(i).getTypeRoot()
                                == DataTypeRoot.INTEGER) {
                            fields[i] = row.getInt(i);
                        } else {
                            fields[i] = BinaryString.fromString(row.getString(i).toString());
                        }
                    }
                    result.add(GenericRow.of(fields));
                });
        reader.close();
        return result;
    }

    // ==================== ORC helpers ====================

    private static long writeOrc(RowType rowType, int rowCount, Path path, LocalFileIO fileIO)
            throws IOException {
        OrcFileFormat orc = new OrcFileFormat(createFormatContext());
        FormatWriterFactory writerFactory = orc.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, COMPRESSION);
        int fieldCount = rowType.getFieldCount();
        for (int r = 0; r < rowCount; r++) {
            writer.addElement(generateRow(r, fieldCount));
        }
        writer.close();
        out.close();
        return fileIO.getFileSize(path);
    }

    private static List<InternalRow> readOrcProjected(
            RowType fullType, RowType projectedType, Path path, LocalFileIO fileIO)
            throws IOException {
        OrcFileFormat orc = new OrcFileFormat(createFormatContext());
        RecordReader<InternalRow> reader =
                orc.createReaderFactory(fullType, projectedType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    Object[] fields = new Object[projectedType.getFieldCount()];
                    for (int i = 0; i < fields.length; i++) {
                        if (row.isNullAt(i)) {
                            fields[i] = null;
                        } else if (projectedType.getTypeAt(i).getTypeRoot()
                                == DataTypeRoot.INTEGER) {
                            fields[i] = row.getInt(i);
                        } else {
                            fields[i] = row.getString(i);
                        }
                    }
                    result.add(GenericRow.of(fields));
                });
        reader.close();
        return result;
    }

    // ==================== Mosaic helpers ====================

    private static long writeMosaic(RowType rowType, int rowCount, Path path, LocalFileIO fileIO)
            throws IOException {
        MosaicFileFormat mosaic = new MosaicFileFormat(createFormatContext());
        FormatWriterFactory writerFactory = mosaic.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, COMPRESSION);
        int fieldCount = rowType.getFieldCount();
        for (int r = 0; r < rowCount; r++) {
            writer.addElement(generateRow(r, fieldCount));
        }
        writer.close();
        out.close();
        return fileIO.getFileSize(path);
    }

    private static List<InternalRow> readMosaic(
            RowType fullType, RowType projectedType, Path path, LocalFileIO fileIO)
            throws IOException {
        MosaicFileFormat mosaic = new MosaicFileFormat(createFormatContext());
        RecordReader<InternalRow> reader =
                mosaic.createReaderFactory(fullType, projectedType, null)
                        .createReader(
                                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    Object[] fields = new Object[projectedType.getFieldCount()];
                    for (int i = 0; i < fields.length; i++) {
                        if (row.isNullAt(i)) {
                            fields[i] = null;
                        } else if (projectedType.getTypeAt(i).getTypeRoot()
                                == DataTypeRoot.INTEGER) {
                            fields[i] = row.getInt(i);
                        } else {
                            fields[i] = row.getString(i);
                        }
                    }
                    result.add(GenericRow.of(fields));
                });
        reader.close();
        return result;
    }

    // ==================== Helpers ====================

    private static final int INT_COLUMN_INTERVAL = 10;
    private static final String[] STRING_SAMPLES = {
        "uuid: 550e8400-e29b-41d4-a716-446655440000",
        "{\"user_id\": 12345, \"action\": \"click\", \"page\": \"home\"}",
        "https://example.com/api/v1/resource/abc123?query=active&sort=desc",
        "customer_service@company-name.example.com",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "2024-01-15T09:23:47.123Z",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "error: connection timeout after 30000ms, retrying...",
        "session_token_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0",
        "active,verified,premium,notifications_enabled,marketing_opt_in",
        "New York, NY 10001, United States",
        "REF-ORD-2024-8847293-XJ",
        "0x7f8a9b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9",
        "Approved by manager at 2024-01-15T10:00:00Z",
        "file:///data/storage/partition_2024_01/batch_17.parquet",
        "[ERROR] NullPointerException at com.example.Service.processLine(42)",
        "User preferences: theme=dark, lang=zh-CN, timezone=Asia/Shanghai",
        "192.168.1.105",
        "Batch job completed successfully. Processed 1,234,567 records in 45.3s.",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0",
        "Shipping via FedEx Ground, tracking #: 784930123456, est. 3 business days",
        "comment: This product exceeded my expectations! Would recommend to everyone.",
        "department=engineering|team=platform|role=senior|level=L6",
        "version=3.2.1-SNAPSHOT, build=20240115.1423, commit=abc123def",
        "payment_method=visa_ending_4242|billing_cycle=monthly|amount=99.99USD"
    };

    private static RowType buildWideRowType() {
        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < COLUMN_COUNT; i++) {
            String name =
                    String.format(
                            "this_is_a_very_long_column_name_for_testing_compression_ratio_column_index_%05d",
                            i);
            if (i % INT_COLUMN_INTERVAL == 0) {
                builder.field(name, DataTypes.INT());
            } else {
                builder.field(name, DataTypes.STRING());
            }
        }
        return builder.build();
    }

    private static GenericRow generateRow(int rowIndex, int fieldCount) {
        Object[] fields = new Object[fieldCount];
        for (int c = 0; c < fieldCount; c++) {
            if (c % INT_COLUMN_INTERVAL == 0) {
                fields[c] = rowIndex * fieldCount + c;
            } else {
                int sampleIdx = (rowIndex + c) % STRING_SAMPLES.length;
                fields[c] =
                        BinaryString.fromString(
                                STRING_SAMPLES[sampleIdx]
                                        + " [row="
                                        + rowIndex
                                        + ",col="
                                        + c
                                        + "]");
            }
        }
        return GenericRow.of(fields);
    }

    private static boolean isIntColumn(int index) {
        return index % INT_COLUMN_INTERVAL == 0;
    }

    private static void assertCellEqual(InternalRow actual, InternalRow expected, int col) {
        if (isIntColumn(col)) {
            check(actual.getInt(col) == expected.getInt(col), "INT mismatch at col=" + col);
        } else {
            check(
                    actual.getString(col).toString().equals(expected.getString(col).toString()),
                    "STRING mismatch at col=" + col);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static FormatContext createFormatContext() {
        return new FormatContext(new Options(), 1024, 1024, MemorySize.ofMebiBytes(128), 9, null);
    }

    private static void deleteRecursively(java.nio.file.Path dir) {
        try {
            Files.walk(dir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException e) {
                                    // ignore
                                }
                            });
        } catch (IOException e) {
            // ignore
        }
    }

    private interface Runner {
        void run(java.nio.file.Path tempDir) throws IOException;
    }
}
