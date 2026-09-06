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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Quantifies the read-side benefit of preserving the Parquet page index (column index / offset
 * index) when {@link ParquetRowGroupCopier} concatenates RowGroups during append-table compaction.
 *
 * <p>The experiment compares four file/reader variants under high-selectivity queries (point lookup
 * and narrow range filter on a sorted column) plus a full-scan control:
 *
 * <ol>
 *   <li>original file with page index, read with column-index filtering enabled;
 *   <li>RowGroup-copied file with page index dropped ({@code preservePageIndex=false});
 *   <li>RowGroup-copied file with page index preserved ({@code preservePageIndex=true});
 *   <li>the preserved file read with {@code parquet.filter.columnindex.enabled=false} (metadata
 *       present but unused, as a control).
 * </ol>
 *
 * <p>Primary metric is bytes actually read from the underlying stream (measured by a counting
 * {@link FileIO}); read latency is secondary. The source file is written with a small {@code
 * parquet.page.size} so every RowGroup of every column contains multiple data pages, which is the
 * precondition for page-level pruning to pay off. A markdown report is written to {@code
 * target/page-index-benchmark-report.md}.
 */
class ParquetPageIndexBenchmark {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataField(0, "id", new IntType()),
                    new DataField(1, "val", new BigIntType()),
                    new DataField(2, "payload", new VarCharType()));

    private static final int TOTAL_ROWS = 200_000;
    private static final int PAGE_SIZE = 16 * 1024;
    private static final long BLOCK_SIZE = 4L * 1024 * 1024;
    private static final int PAYLOAD_LENGTH = 100;
    private static final String COMPRESSION = "zstd";
    private static final int BATCH_SIZE = 1024;

    /** Point query hits exactly one row; range query selects 0.1% of rows. */
    private static final int POINT_ID = TOTAL_ROWS / 2 + 123;

    private static final int RANGE_LO = TOTAL_ROWS / 4;
    private static final int RANGE_HI = RANGE_LO + TOTAL_ROWS / 1000;

    private static final int WARMUP_ITERATIONS = 2;
    private static final int MEASURED_ITERATIONS = 7;

    private static final String ALNUM =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void benchmark() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();

        // ---------------- 1. prepare files ----------------
        Path source = new Path(tempDir.toString(), "source.parquet");
        writeSourceFile(fileIO, source);

        Path dropped = new Path(tempDir.toString(), "copied-drop-index.parquet");
        Path preserved = new Path(tempDir.toString(), "copied-preserve-index.parquet");
        copyRowGroups(fileIO, source, dropped, false);
        copyRowGroups(fileIO, source, preserved, true);

        // ---------------- 2. verify page structure ----------------
        IndexInfo sourceInfo = inspectPages(fileIO, source);
        IndexInfo droppedInfo = inspectPages(fileIO, dropped);
        IndexInfo preservedInfo = inspectPages(fileIO, preserved);

        assertThat(sourceInfo.columnIndexPresent)
                .as("source file must carry a page index for the experiment to be meaningful")
                .isTrue();
        assertThat(sourceInfo.minPagesPerBlockColumn)
                .as("every (RowGroup, column) of the source file must have multiple data pages")
                .isGreaterThanOrEqualTo(2);
        assertThat(preservedInfo.columnIndexPresent).isTrue();
        assertThat(preservedInfo.totalDataPages)
                .as("preserved copy must keep the same page layout as the source")
                .isEqualTo(sourceInfo.totalDataPages);
        assertThat(droppedInfo.columnIndexPresent)
                .as("copied file with preservePageIndex=false must not carry a page index")
                .isFalse();

        long sourceSize = fileIO.getFileSize(source);
        long droppedSize = fileIO.getFileSize(dropped);
        long preservedSize = fileIO.getFileSize(preserved);

        // ---------------- 3. run read groups ----------------
        PredicateBuilder predicates = new PredicateBuilder(ROW_TYPE);
        Predicate point = predicates.equal(0, POINT_ID);
        Predicate range = predicates.between(0, RANGE_LO, RANGE_HI);
        List<QuerySpec> queries = new ArrayList<>();
        queries.add(
                new QuerySpec(
                        "point (id = " + POINT_ID + ")",
                        Collections.singletonList(point),
                        POINT_ID,
                        POINT_ID));
        queries.add(
                new QuerySpec(
                        "range (id between " + RANGE_LO + " and " + RANGE_HI + ")",
                        Collections.singletonList(range),
                        RANGE_LO,
                        RANGE_HI));
        queries.add(new QuerySpec("full scan (no filter)", null, null, null));

        Map<String, Path> variants = new LinkedHashMap<>();
        variants.put("1. original + page index", source);
        variants.put("2. copy, index dropped", dropped);
        variants.put("3. copy, index preserved", preserved);
        variants.put("4. preserved, reader disabled", preserved);

        // variant 4 reads the preserved file but with column-index filtering turned off
        Options disabledOptions = new Options();
        disabledOptions.set("parquet.filter.columnindex.enabled", "false");

        List<String> reportLines = new ArrayList<>();
        reportLines.add(
                "| query | variant | rows returned | bytes read | bytes vs dropped | avg ms |");
        reportLines.add("| --- | --- | --- | --- | --- | --- |");

        for (QuerySpec query : queries) {
            String queryName = query.name;

            long droppedBytes = -1;
            Map<String, Measurement> results = new LinkedHashMap<>();
            for (Map.Entry<String, Path> variant : variants.entrySet()) {
                Options readOptions =
                        variant.getKey().startsWith("4.") ? disabledOptions : new Options();
                Measurement m =
                        measure(
                                variant.getValue(),
                                fileIO.getFileSize(variant.getValue()),
                                readOptions,
                                query);
                results.put(variant.getKey(), m);
                if (variant.getKey().startsWith("2.")) {
                    droppedBytes = m.bytesRead;
                }
            }

            Measurement droppedM = results.get("2. copy, index dropped");
            Measurement originalM = results.get("1. original + page index");
            Measurement preservedM = results.get("3. copy, index preserved");
            Measurement disabledM = results.get("4. preserved, reader disabled");
            if (query.hitLo != null) {
                long expectedHits = (long) query.hitHi - query.hitLo + 1;
                // The pushed-down filter is used for I/O pruning only; the reader returns a
                // superset of matching rows and the engine re-applies the predicate. So the
                // correctness invariant is: every true match must be returned by every variant.
                for (Map.Entry<String, Measurement> e : results.entrySet()) {
                    assertThat(e.getValue().hits)
                            .as("%s under %s must return all matching rows", e.getKey(), queryName)
                            .isEqualTo(expectedHits);
                }
                // page pruning must return a subset of the unpruned variant's rows
                assertThat(preservedM.rows)
                        .as("preserved index must prune rows for %s", queryName)
                        .isLessThanOrEqualTo(droppedM.rows);
                // the preserved copy must behave essentially like the original file (tiny footer
                // key-value metadata differences are expected)
                assertThat(preservedM.rows).isEqualTo(originalM.rows);
                assertThat((double) preservedM.bytesRead / originalM.bytesRead)
                        .as("preserved copy must track the original file's read bytes")
                        .isBetween(0.98, 1.02);
                // sanity: page pruning must read strictly fewer bytes when the index exists
                assertThat(preservedM.bytesRead)
                        .as("preserved index must reduce bytes for %s", queryName)
                        .isLessThan(droppedM.bytesRead);
                // sanity: disabling the reader-side knob must behave like a dropped index
                assertThat((double) disabledM.bytesRead / droppedM.bytesRead)
                        .as("reader-disabled control must track the dropped-index variant")
                        .isBetween(0.85, 1.15);
            } else {
                // full scan: all variants return all rows and read essentially the whole file
                for (Map.Entry<String, Measurement> e : results.entrySet()) {
                    assertThat(e.getValue().rows).isEqualTo(TOTAL_ROWS);
                    assertThat((double) e.getValue().bytesRead / e.getValue().fileSize)
                            .as("full scan of %s reads most of the file", e.getKey())
                            .isBetween(0.80, 1.20);
                }
            }

            for (Map.Entry<String, Measurement> e : results.entrySet()) {
                reportLines.add(
                        String.format(
                                "| %s | %s | %d | %d | %s | %.2f |",
                                queryName,
                                e.getKey(),
                                e.getValue().rows,
                                e.getValue().bytesRead,
                                droppedBytes > 0
                                        ? String.format(
                                                "%.1f%%",
                                                100.0 * e.getValue().bytesRead / droppedBytes)
                                        : "-",
                                e.getValue().avgNanos / 1_000_000.0));
            }
        }

        // ---------------- 4. report ----------------
        StringBuilder report = new StringBuilder();
        report.append("# Parquet page index benchmark (RowGroup copy)\n\n");
        report.append(
                String.format(
                        "- rows=%d, page.size=%d, block.size=%d, compression=%s, dictionary"
                                + " disabled, payload=%d bytes/row%n",
                        TOTAL_ROWS, PAGE_SIZE, BLOCK_SIZE, COMPRESSION, PAYLOAD_LENGTH));
        report.append(
                String.format(
                        "- source: size=%d, blocks=%d, dataPages=%d, minPagesPerBlockColumn=%d%n",
                        sourceSize,
                        sourceInfo.blocks,
                        sourceInfo.totalDataPages,
                        sourceInfo.minPagesPerBlockColumn));
        report.append(
                String.format(
                        "- copy(dropped): size=%d; copy(preserved): size=%d (index overhead %.2f%%"
                                + " of file size)%n%n",
                        droppedSize,
                        preservedSize,
                        100.0 * (preservedSize - droppedSize) / droppedSize));
        for (String line : reportLines) {
            report.append(line).append('\n');
        }
        String text = report.toString();
        System.out.println(text);
        java.nio.file.Path reportFile =
                java.nio.file.Paths.get("target", "page-index-benchmark-report.md");
        Files.createDirectories(reportFile.getParent());
        Files.write(reportFile, text.getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------------
    // setup helpers
    // ---------------------------------------------------------------------

    private void writeSourceFile(LocalFileIO fileIO, Path path) throws IOException {
        Options options = new Options();
        options.set("parquet.block.size", String.valueOf(BLOCK_SIZE));
        options.set("parquet.page.size", String.valueOf(PAGE_SIZE));
        // keep every column plain-encoded so page boundaries are driven by parquet.page.size
        // and the filter column itself spans many pages per RowGroup
        options.set("parquet.enable.dictionary", "false");

        ParquetWriterFactory writerFactory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, options));
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(path, false), COMPRESSION)) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                writer.addElement(
                        GenericRow.of(i, (long) i * 7, BinaryString.fromString(payload(i))));
            }
        }
    }

    private static String payload(int id) {
        Random random = new Random(id * 31L + 7);
        StringBuilder builder = new StringBuilder(PAYLOAD_LENGTH);
        for (int i = 0; i < PAYLOAD_LENGTH; i++) {
            builder.append(ALNUM.charAt(random.nextInt(ALNUM.length())));
        }
        return builder.toString();
    }

    private void copyRowGroups(LocalFileIO fileIO, Path source, Path target, boolean preserve)
            throws IOException {
        ParquetMetadataHolder holder = readFooter(fileIO, source);
        AtomicInteger counter = new AtomicInteger();
        ParquetRowGroupCopier copier =
                new ParquetRowGroupCopier(
                        fileIO,
                        ROW_TYPE,
                        Long.MAX_VALUE,
                        () -> counter.getAndIncrement() == 0 ? target : unusedPath(target),
                        new Options(),
                        preserve);
        List<ParquetRowGroupCopier.OutputFile> outputs = copier.copy(holder.inputs);
        assertThat(outputs).hasSize(1);
        assertThat(outputs.get(0).path()).isEqualTo(target);
        assertThat(outputs.get(0).rowCount()).isEqualTo(TOTAL_ROWS);
    }

    private static Path unusedPath(Path base) {
        return new Path(base.getParent(), base.getName() + ".unexpected");
    }

    private static class ParquetMetadataHolder {
        private List<ParquetRowGroupCopier.Input> inputs;
    }

    private static ParquetMetadataHolder readFooter(FileIO fileIO, Path path) throws IOException {
        long length = fileIO.getFileSize(path);
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(fileIO, path, length, new Options())) {
            ParquetMetadataHolder holder = new ParquetMetadataHolder();
            org.apache.parquet.hadoop.metadata.ParquetMetadata footer = reader.getFooter();
            holder.inputs =
                    Collections.singletonList(
                            new ParquetRowGroupCopier.Input(
                                    ParquetInputFile.fromPath(fileIO, path, length), footer));
            return holder;
        }
    }

    private static class IndexInfo {
        private int blocks;
        private int totalDataPages;
        private int minPagesPerBlockColumn = Integer.MAX_VALUE;
        private boolean columnIndexPresent = true;
    }

    private static IndexInfo inspectPages(FileIO fileIO, Path path) throws IOException {
        IndexInfo info = new IndexInfo();
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, path, fileIO.getFileSize(path), new Options())) {
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            info.blocks = blocks.size();
            for (BlockMetaData block : blocks) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    OffsetIndex offsetIndex = reader.readOffsetIndex(column);
                    if (reader.readColumnIndex(column) == null || offsetIndex == null) {
                        info.columnIndexPresent = false;
                        continue;
                    }
                    int pages = offsetIndex.getPageCount();
                    info.totalDataPages += pages;
                    info.minPagesPerBlockColumn = Math.min(info.minPagesPerBlockColumn, pages);
                }
            }
        }
        if (info.minPagesPerBlockColumn == Integer.MAX_VALUE) {
            info.minPagesPerBlockColumn = 0;
        }
        return info;
    }

    // ---------------------------------------------------------------------
    // measurement
    // ---------------------------------------------------------------------

    /** A query to measure: the pushed-down filter plus the true match range for validation. */
    private static class QuerySpec {
        private final String name;
        @Nullable private final List<Predicate> filter;
        private final Integer hitLo;
        private final Integer hitHi;

        private QuerySpec(
                String name, @Nullable List<Predicate> filter, Integer hitLo, Integer hitHi) {
            this.name = name;
            this.filter = filter;
            this.hitLo = hitLo;
            this.hitHi = hitHi;
        }
    }

    private static class Measurement {
        private long rows;
        private long hits;
        private long bytesRead;
        private long fileSize;
        private long avgNanos;
    }

    private Measurement measure(Path file, long fileSize, Options readOptions, QuerySpec query)
            throws IOException {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            readOnce(new CountingFileIO(), file, fileSize, readOptions, query);
        }

        Measurement m = new Measurement();
        long totalNanos = 0;
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            CountingFileIO io = new CountingFileIO();
            long start = System.nanoTime();
            long[] rowsAndHits = readOnce(io, file, fileSize, readOptions, query);
            long nanos = System.nanoTime() - start;
            totalNanos += nanos;
            if (i == 0) {
                m.rows = rowsAndHits[0];
                m.hits = rowsAndHits[1];
                m.bytesRead = io.bytesRead.get();
                m.fileSize = fileSize;
            } else {
                assertThat(rowsAndHits[0]).isEqualTo(m.rows);
                assertThat(rowsAndHits[1]).isEqualTo(m.hits);
                assertThat(io.bytesRead.get())
                        .as("bytes read must be deterministic across iterations")
                        .isEqualTo(m.bytesRead);
            }
        }
        m.avgNanos = totalNanos / MEASURED_ITERATIONS;
        return m;
    }

    /**
     * Reads the whole file through the paimon parquet read path. Returns {rows returned, rows
     * matching the query's true match range}. The pushed-down filter only prunes I/O, so the
     * returned row set is a superset of the true matches.
     */
    private long[] readOnce(
            CountingFileIO io, Path file, long fileSize, Options readOptions, QuerySpec query)
            throws IOException {
        ParquetReaderFactory factory =
                new ParquetReaderFactory(readOptions, ROW_TYPE, BATCH_SIZE, query.filter);
        long rows = 0;
        long hits = 0;
        try (FileRecordReader<InternalRow> reader =
                factory.createReader(new FormatReaderContext(io, file, fileSize, null, null))) {
            RecordReader.RecordIterator<InternalRow> iterator;
            while ((iterator = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = iterator.next()) != null) {
                    rows++;
                    int id = row.getInt(0);
                    if (query.hitLo != null && id >= query.hitLo && id <= query.hitHi) {
                        hits++;
                    }
                }
                iterator.releaseBatch();
            }
        }
        return new long[] {rows, hits};
    }

    /** A {@link LocalFileIO} that counts bytes actually read from the file. */
    private static class CountingFileIO extends LocalFileIO {

        private final AtomicLong bytesRead = new AtomicLong();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream in = super.newInputStream(path);
            return new SeekableInputStreamWrapper(in) {
                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) {
                        bytesRead.addAndGet(n);
                    }
                    return n;
                }

                @Override
                public int read() throws IOException {
                    int n = super.read();
                    if (n >= 0) {
                        bytesRead.incrementAndGet();
                    }
                    return n;
                }
            };
        }
    }
}
