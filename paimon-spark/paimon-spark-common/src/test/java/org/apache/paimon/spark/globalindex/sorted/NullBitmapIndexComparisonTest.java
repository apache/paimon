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

package org.apache.paimon.spark.globalindex.sorted;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexer;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.sorted.SortedSingleColumnIndexWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.spark.Partitioner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Compares real index files and queries after scalar and bitmap Spark exchanges. */
class NullBitmapIndexComparisonTest {

    private static final int ROW_COUNT = 200000;
    private static final int RECORDS_PER_FILE = 10000;
    private static final DataField FIELD = new DataField(0, "index_key", DataTypes.INT());
    private static final FieldRef REF = new FieldRef(0, "index_key", DataTypes.INT());
    private static final Comparator<Row> ORDER =
            Comparator.comparingLong((Row row) -> row.getLong(0))
                    .thenComparing(
                            row -> row.isNullAt(1) ? null : row.getInt(1),
                            Comparator.nullsFirst(Comparator.<Integer>naturalOrder()))
                    .thenComparingLong(row -> row.getLong(2));

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testRealFilesAndQueriesAcrossSortedPartitions() throws Exception {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("null-bitmap-index-comparison")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .getOrCreate();
        try {
            // count, null percentage, hot-key flag; probe both sides of rotation boundaries.
            int[][] scenarios = {
                {ROW_COUNT, 100, 0},
                {ROW_COUNT, 90, 0},
                {ROW_COUNT, 50, 0},
                {ROW_COUNT, 0, 0},
                {ROW_COUNT, 2, 0},
                {ROW_COUNT, 50, 1},
                {RECORDS_PER_FILE - 1, 100, 0},
                {RECORDS_PER_FILE, 100, 0},
                {RECORDS_PER_FILE + 1, 100, 0},
                {4 * RECORDS_PER_FILE - 1, 100, 0},
                {4 * RECORDS_PER_FILE, 100, 0},
                {4 * RECORDS_PER_FILE + 1, 100, 0}
            };
            for (int[] scenario : scenarios) {
                int rowCount = scenario[0];
                int percent = scenario[1];
                boolean hotKey = scenario[2] == 1;
                // The sparse case scatters nulls deterministically rather than periodically.
                String nullCondition =
                        percent == 2
                                ? "pmod(xxhash64(id, 42), 100) < 2"
                                : "pmod(id * 31, 100) < " + percent;
                String key =
                        hotKey
                                ? "CASE WHEN pmod(id, 100) < 99 THEN 990 ELSE CAST(id % 1000 AS INT) END"
                                : "CAST(id % 1000 AS INT)";
                Dataset<Row> input =
                        spark.range(0, rowCount, 1, 4)
                                .selectExpr(
                                        "CAST(0 AS BIGINT) AS task_id",
                                        "CASE WHEN "
                                                + nullCondition
                                                + " THEN CAST(NULL AS INT) ELSE "
                                                + key
                                                + " END AS index_key",
                                        "id AS `" + SpecialFields.ROW_ID.name() + "`");
                List<Row> source = input.collectAsList();
                Partitioner partitioner = NullBitmapShuffle.createPartitioner(input, 4);
                List<List<Row>> baseline = sortedPartitions(input, partitioner, false);
                List<List<Row>> optimized = sortedPartitions(input, partitioner, true);
                List<Row> expanded = new ArrayList<>();
                for (int partitionId = 0; partitionId < optimized.size(); partitionId++) {
                    List<Row> partition = optimized.get(partitionId);
                    assertThat(partition)
                            .as("partition %s", partitionId)
                            .isEqualTo(baseline.get(partitionId));
                    expanded.addAll(partition);
                    assertSorted(partition);
                }
                Comparator<Row> rowIdOrder = Comparator.comparingLong(row -> row.getLong(2));
                source.sort(rowIdOrder);
                expanded.sort(rowIdOrder);
                assertThat(expanded).hasSize(rowCount).isEqualTo(source);
                if (percent != 2) {
                    assertThat(source.stream().filter(row -> row.isNullAt(1)).count())
                            .isEqualTo((long) rowCount * percent / 100);
                }
                for (String type : new String[] {"btree", "bitmap"}) {
                    String label = type + "-null" + percent + "-rows" + rowCount + "-hot" + hotKey;
                    List<GlobalIndexIOMeta> before = write(type, label + "-before", baseline);
                    List<GlobalIndexIOMeta> after = write(type, label + "-after", optimized);
                    assertSameFiles(before, after);
                    assertThat(before.stream().mapToLong(GlobalIndexIOMeta::rowCount).sum())
                            .isEqualTo(rowCount);
                    assertThat(after.stream().mapToLong(GlobalIndexIOMeta::rowCount).sum())
                            .isEqualTo(rowCount);
                    System.out.printf(
                            "%s rows=%d nulls=%d files=%d/%d fileBytes=%d/%d%n",
                            label,
                            rowCount,
                            source.stream().filter(row -> row.isNullAt(1)).count(),
                            before.size(),
                            after.size(),
                            before.stream().mapToLong(GlobalIndexIOMeta::fileSize).sum(),
                            after.stream().mapToLong(GlobalIndexIOMeta::fileSize).sum());
                    compareQueries(type, label, source, before, after);

                    // Also exercise exact rotation thresholds in a single output partition.
                    List<Row> ordered = new ArrayList<>(source);
                    ordered.sort(ORDER);
                    expanded.sort(ORDER);
                    List<GlobalIndexIOMeta> orderedBefore =
                            write(
                                    type,
                                    label + "-ordered-before",
                                    Collections.singletonList(ordered));
                    List<GlobalIndexIOMeta> orderedAfter =
                            write(
                                    type,
                                    label + "-ordered-after",
                                    Collections.singletonList(expanded));
                    assertThat(orderedBefore)
                            .hasSize((rowCount + RECORDS_PER_FILE - 1) / RECORDS_PER_FILE);
                    assertSameFiles(orderedBefore, orderedAfter);
                }
            }
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    private static List<List<Row>> sortedPartitions(
            Dataset<Row> input, Partitioner partitioner, boolean compress) {
        Dataset<Row> sorted = NullBitmapShuffle.sort(input, partitioner, compress);
        List<List<Row>> transport = sorted.javaRDD().glom().collect();
        assertThat(transport).hasSize(4);
        List<List<Row>> result = new ArrayList<>();
        for (List<Row> partition : transport) {
            List<Row> rows = new ArrayList<>();
            NullBitmapShuffle.expandRows(partition.iterator()).forEachRemaining(rows::add);
            result.add(rows);
        }
        return result;
    }

    private static void assertSameFiles(
            List<GlobalIndexIOMeta> before, List<GlobalIndexIOMeta> after) throws IOException {
        assertThat(after).hasSize(before.size());
        for (int i = 0; i < before.size(); i++) {
            GlobalIndexIOMeta left = before.get(i);
            GlobalIndexIOMeta right = after.get(i);
            assertThat(right.rowCount()).as("file %s source count", i).isEqualTo(left.rowCount());
            assertThat(right.fileSize()).as("file %s size", i).isEqualTo(left.fileSize());
            assertThat(right.metadata()).as("file %s metadata", i).isEqualTo(left.metadata());
            assertThat(Files.readAllBytes(java.nio.file.Paths.get(right.filePath().toUri())))
                    .as("file %s bytes", i)
                    .isEqualTo(
                            Files.readAllBytes(java.nio.file.Paths.get(left.filePath().toUri())));
        }
    }

    private static void assertSorted(List<Row> rows) {
        Row previous = null;
        for (Row row : rows) {
            if (previous != null) {
                assertThat(ORDER.compare(previous, row)).isLessThanOrEqualTo(0);
            }
            previous = row;
        }
    }

    private static GlobalIndexer indexer(String type) {
        return "btree".equals(type)
                ? new BTreeGlobalIndexer(Collections.singletonList(FIELD), new Options())
                : new BitmapGlobalIndexer(FIELD, new Options());
    }

    private List<GlobalIndexIOMeta> write(String type, String directory, List<List<Row>> partitions)
            throws Exception {
        java.nio.file.Path local = Files.createDirectories(tempDir.resolve(directory));
        Path base = new Path(local.toUri());
        FileIO io = LocalFileIO.create();
        GlobalIndexer indexer = indexer(type);
        GlobalIndexFileWriter files =
                new GlobalIndexFileWriter() {
                    private int sequence;

                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + sequence++ + ".index";
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name) throws IOException {
                        return io.newOutputStream(new Path(base, name), false);
                    }
                };
        List<GlobalIndexIOMeta> result = new ArrayList<>();
        for (List<Row> partition : partitions) {
            try (SortedSingleColumnIndexWriter writer =
                    new SortedSingleColumnIndexWriter(
                            RECORDS_PER_FILE,
                            () -> (GlobalIndexSingleColumnWriter) indexer.createWriter(files))) {
                for (Row row : partition) {
                    writer.write(row.isNullAt(1) ? null : row.getInt(1), row.getLong(2));
                }
                for (List<ResultEntry> group : writer.finish()) {
                    assertThat(group).hasSize(1);
                    ResultEntry entry = group.get(0);
                    Path path = new Path(base, entry.fileName());
                    result.add(
                            new GlobalIndexIOMeta(
                                    path, io.getFileSize(path), entry.rowCount(), entry.meta()));
                }
            }
        }
        return result;
    }

    private static void compareQueries(
            String type,
            String label,
            List<Row> source,
            List<GlobalIndexIOMeta> before,
            List<GlobalIndexIOMeta> after)
            throws Exception {
        for (int query = 0; query < 4; query++) {
            List<Long> expected = new ArrayList<>();
            for (Row row : source) {
                boolean isNull = row.isNullAt(1);
                if ((query == 0 && isNull)
                        || (query == 1 && !isNull)
                        || (query == 2 && !isNull && row.getInt(1) == 990)
                        || (query == 3
                                && !isNull
                                && row.getInt(1) >= 250
                                && row.getInt(1) <= 750)) {
                    expected.add(row.getLong(2));
                }
            }
            expected.sort(Long::compare);
            long baselineColdBytes = -1;
            for (int variant = 0; variant < 2; variant++) {
                CountingReader files = new CountingReader();
                ExecutorService executor = newDirectExecutorService();
                try (GlobalIndexReader reader =
                        indexer(type)
                                .createReader(
                                        files,
                                        variant == 0 ? before : after,
                                        source.size(),
                                        null,
                                        executor)) {
                    Optional<GlobalIndexResult> cold = query(reader, query);
                    assertThat(cold).as(label + " query=" + query).isPresent();
                    assertThat(rows(cold.get())).isEqualTo(expected);
                    long coldBytes = files.bytes.get();
                    if (variant == 0) {
                        baselineColdBytes = coldBytes;
                    } else {
                        assertThat(coldBytes)
                                .as(label + " query=" + query + " cold read bytes")
                                .isEqualTo(baselineColdBytes);
                    }
                    for (int warmup = 0; warmup < 3; warmup++) {
                        rows(query(reader, query).get());
                    }
                    long startBytes = files.bytes.get();
                    long[] nanos = new long[7];
                    for (int iteration = 0; iteration < nanos.length; iteration++) {
                        long start = System.nanoTime();
                        List<Long> actual = rows(query(reader, query).get());
                        nanos[iteration] = System.nanoTime() - start;
                        assertThat(actual).isEqualTo(expected);
                    }
                    Arrays.sort(nanos);
                    System.out.printf(
                            "%s query=%d variant=%s expected=%d coldReadBytes=%d warmReadBytes=%d medianQueryNs=%d%n",
                            label,
                            query,
                            variant == 0 ? "scalar" : "bitmap",
                            expected.size(),
                            coldBytes,
                            files.bytes.get() - startBytes,
                            nanos[nanos.length / 2]);
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    private static Optional<GlobalIndexResult> query(GlobalIndexReader reader, int query) {
        switch (query) {
            case 0:
                return reader.visitIsNull(REF).join();
            case 1:
                return reader.visitIsNotNull(REF).join();
            case 2:
                return reader.visitEqual(REF, 990).join();
            case 3:
                return reader.visitBetween(REF, 250, 750).join();
            default:
                throw new IllegalArgumentException("Unknown query: " + query);
        }
    }

    private static List<Long> rows(GlobalIndexResult result) {
        List<Long> rows = new ArrayList<>();
        Iterator<Long> iterator = result.results().iterator();
        iterator.forEachRemaining(rows::add);
        return rows;
    }

    private static class CountingReader implements GlobalIndexFileReader {
        private final AtomicLong bytes = new AtomicLong();

        @Override
        public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
            return new SeekableInputStreamWrapper(
                    LocalFileIO.create().newInputStream(meta.filePath())) {
                @Override
                public int read() throws IOException {
                    int value = super.read();
                    if (value >= 0) {
                        bytes.incrementAndGet();
                    }
                    return value;
                }

                @Override
                public int read(byte[] buffer, int offset, int length) throws IOException {
                    int count = super.read(buffer, offset, length);
                    if (count > 0) {
                        bytes.addAndGet(count);
                    }
                    return count;
                }
            };
        }
    }
}
