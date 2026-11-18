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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.UnionGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.rtree.RTreeIndexReadContext.PageBlock;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.apache.paimon.shade.guava30.com.google.common.io.Resources;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ForwardingExecutorService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.geo.WkbTestUtils.line;
import static org.apache.paimon.geo.WkbTestUtils.point;
import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral and corruption tests for a reloadable, bounded-memory XY R-tree index. */
class RTreeGlobalIndexTest {

    private final DataField field = new DataField(7, "geom", DataTypes.GEOMETRY("EPSG:3857"));
    private final FieldRef ref = new FieldRef(0, "geom", field.type());
    @TempDir java.nio.file.Path directory;

    private FileIO io;
    private Path base;
    private GlobalIndexFileWriter output;
    private GlobalIndexFileReader input;

    @BeforeEach
    void setUp() {
        io = LocalFileIO.create();
        base = new Path(directory.toUri());
        output =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID() + ".index";
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return io.newOutputStream(new Path(base, fileName), false);
                    }
                };
        input = file -> io.newInputStream(file.filePath());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 16, 256})
    void testReloadedMultiFileIndexMatchesIndependentEnvelopeScan(int pageSize) throws Exception {
        RTreeGlobalIndexer indexer = indexer(pageSize, 37, 4);
        List<byte[]> values = new ArrayList<>();
        List<double[]> rectangles = new ArrayList<>();
        List<Long> nulls = new ArrayList<>();
        List<Long> nonNulls = new ArrayList<>();
        Random random = new Random(20261117);
        for (int i = 0; i < 513; i++) {
            double x = random.nextDouble() * 200 - 100;
            double y = random.nextDouble() * 200 - 100;
            double width = random.nextDouble() * 30;
            double height = random.nextDouble() * 30;
            rectangles.add(new double[] {x, y, x + width, y + height});
            if (i % 17 == 0) {
                values.add(null);
                nulls.add((long) i);
            } else {
                nonNulls.add((long) i);
                values.add(i % 19 == 0 ? line() : line(x, y, x + width, y + height));
            }
        }
        List<GlobalIndexIOMeta> files = write(indexer, values);
        assertThat(files).hasSize(14);
        assertThat(files.stream().mapToLong(GlobalIndexIOMeta::rowCount).sum()).isEqualTo(513);
        try (GlobalIndexReader reader = reader(indexer, files, values.size())) {
            assertRows(reader.visitIsNull(ref), nulls);
            assertRows(reader.visitIsNotNull(ref), nonNulls);
            for (int q = 0; q < 40; q++) {
                double x = random.nextDouble() * 200 - 100;
                double y = random.nextDouble() * 200 - 100;
                double right = x + random.nextDouble() * 100;
                double top = y + random.nextDouble() * 100;
                List<Long> expected = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    double[] r = rectangles.get(i);
                    if (i % 17 != 0
                            && i % 19 != 0
                            && r[0] <= right
                            && r[2] >= x
                            && r[1] <= top
                            && r[3] >= y) {
                        expected.add((long) i);
                    }
                }
                assertRows(reader.visitBBoxIntersects(ref, x, y, right, top), expected);
            }
            assertThat(reader.visitEqual(ref, point(0, 0)).join()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 8, 9, 17})
    void testPageBoundariesEmptyNullAndDegenerateTrees(int count) throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 100, 0);
        List<byte[]> values = new ArrayList<>();
        List<Long> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(point(3, i));
            expected.add((long) i);
        }
        try (GlobalIndexReader reader = reader(indexer, write(indexer, values), count)) {
            assertRows(reader.visitBBoxIntersects(ref, 3, 0, 3, count), expected);
            assertRows(
                    reader.visitBBoxIntersects(ref, Math.nextUp(3d), 0, 4, count),
                    Collections.emptyList());
            assertRows(reader.visitIsNull(ref), Collections.emptyList());
        }
        values = Arrays.asList(null, point(Double.NaN, Double.NaN), line());
        try (GlobalIndexReader reader = reader(indexer, write(indexer, values), 3)) {
            assertRows(reader.visitBBoxIntersects(ref, -1, -1, 1, 1), Collections.emptyList());
            assertRows(reader.visitIsNull(ref), Collections.singletonList(0L));
            assertRows(reader.visitIsNotNull(ref), Arrays.asList(1L, 2L));
        }
    }

    @Test
    void testSparseLongRowIdsOffsetsAndCompoundEvaluation() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 3, 0);
        long largeId = 1L << 33;
        List<GlobalIndexIOMeta> files;
        try (RTreeGlobalIndexWriter writer = indexer.createWriter(output)) {
            writer.write(point(0, 0), largeId);
            writer.write(null, 8);
            writer.write(point(1, 1), 3);
            files = metas(writer.finish());
        }
        try (GlobalIndexReader reader = reader(indexer, files, largeId + 1)) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 1, 1), Arrays.asList(3L, largeId));
        }
        List<GlobalIndexIOMeta> first = write(indexer, Arrays.asList(point(0, 0), point(10, 10)));
        List<GlobalIndexIOMeta> second = write(indexer, Arrays.asList(null, point(1, 1)));
        GlobalIndexReader union =
                new UnionGlobalIndexReader(
                        Arrays.asList(
                                new OffsetGlobalIndexReader(reader(indexer, first, 2), 100, 101),
                                new OffsetGlobalIndexReader(reader(indexer, second, 2), 200, 201)));
        RowType rowType = new RowType(Collections.singletonList(field));
        PredicateBuilder builder = new PredicateBuilder(rowType);
        try (GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, id -> Collections.singletonList(union))) {
            Predicate box = builder.bboxIntersects(0, 0, 0, 1, 1);
            assertThat(evaluator.evaluate(box).get().results().toRangeList())
                    .containsExactly(new Range(100, 100), new Range(201, 201));
            assertThat(
                            evaluator
                                    .evaluate(PredicateBuilder.or(box, builder.isNull(0)))
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(100, 100), new Range(200, 201));
            assertThat(
                            evaluator
                                    .evaluate(PredicateBuilder.and(box, builder.isNotNull(0)))
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(100, 100), new Range(201, 201));
        }
    }

    @Test
    void testLazyPruningCacheAndClosedReader() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 256, 32);
        List<byte[]> points = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            points.add(point(i, 0));
        }
        List<GlobalIndexIOMeta> files = write(indexer, points);
        AtomicInteger opens = new AtomicInteger();
        GlobalIndexFileReader counted =
                file -> {
                    opens.incrementAndGet();
                    return input.getInputStream(file);
                };
        GlobalIndexReader reader =
                indexer.createReader(counted, files, 128, null, newDirectExecutorService());
        assertThat(opens).hasValue(0);
        assertRows(
                reader.visitBBoxIntersects(ref, 1000, 1000, 2000, 2000), Collections.emptyList());
        assertThat(opens).hasValue(0);
        assertRows(reader.visitBBoxIntersects(ref, 0, 0, 0, 0), Collections.singletonList(0L));
        assertThat(opens).hasValue(1);
        assertRows(reader.visitBBoxIntersects(ref, 0, 0, 0, 0), Collections.singletonList(0L));
        assertThat(opens).hasValue(1);
        reader.close();
        assertThatThrownBy(() -> reader.visitBBoxIntersects(ref, 0, 0, 0, 0).join())
                .hasRootCauseMessage("R-tree reader is closed.");
        try (GlobalIndexReader pruned =
                indexer.createReader(
                        counted,
                        files,
                        128,
                        Collections.singletonList(new Range(200, 300)),
                        newDirectExecutorService())) {
            assertRows(pruned.visitBBoxIntersects(ref, 0, 0, 127, 0), Collections.emptyList());
            assertThat(opens).hasValue(1);
        }
    }

    @Test
    void testExtremeFiniteBounds() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        List<byte[]> values =
                Arrays.asList(
                        point(-Double.MAX_VALUE, 0),
                        point(Double.MAX_VALUE, 0),
                        line(-Double.MAX_VALUE, -1, Double.MAX_VALUE, 1));
        try (GlobalIndexReader reader = reader(indexer, write(indexer, values), 3)) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 0, 0), Collections.singletonList(2L));
            assertRows(
                    reader.visitBBoxIntersects(ref, -Double.MAX_VALUE, 0, Double.MAX_VALUE, 0),
                    Arrays.asList(0L, 1L, 2L));
        }
    }

    @Test
    void testCacheIsSharedAcrossReadersAndSurvivesReaderClose() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 256, 32);
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            values.add(point(i, 0));
        }
        List<GlobalIndexIOMeta> files = write(indexer, values);
        AtomicInteger opens = new AtomicInteger();
        GlobalIndexFileReader counted =
                file -> {
                    opens.incrementAndGet();
                    return input.getInputStream(file);
                };
        try (GlobalIndexReader reader =
                indexer.createReader(counted, files, 128, null, newDirectExecutorService())) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 0, 0), Collections.singletonList(0L));
        }
        assertThat(opens).hasValue(1);
        try (GlobalIndexReader reader =
                indexer.createReader(counted, files, 128, null, newDirectExecutorService())) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 0, 0), Collections.singletonList(0L));
        }
        assertThat(opens).hasValue(1);
    }

    @Test
    void testNullCountsCanAnswerEmptyResultsWithoutIo() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 100, 0);
        List<GlobalIndexIOMeta> nonNull = write(indexer, Arrays.asList(point(1, 1), line()));
        List<GlobalIndexIOMeta> allNull = write(indexer, Arrays.asList(null, null));
        GlobalIndexFileReader noIo =
                file -> {
                    throw new AssertionError("No I/O expected for an empty null lookup");
                };
        try (GlobalIndexReader reader =
                indexer.createReader(noIo, nonNull, 2, null, newDirectExecutorService())) {
            assertRows(reader.visitIsNull(ref), Collections.emptyList());
        }
        try (GlobalIndexReader reader =
                indexer.createReader(noIo, allNull, 2, null, newDirectExecutorService())) {
            assertRows(reader.visitIsNotNull(ref), Collections.emptyList());
        }
    }

    @Test
    void testAdjacentPagesAreCoalescedWithoutVectoredSupport() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 256, 0);
        List<byte[]> values = new ArrayList<>();
        List<Long> expected = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            values.add(point(i, 0));
            expected.add((long) i);
        }
        List<GlobalIndexIOMeta> files = write(indexer, values);
        MemoryInput stream =
                new MemoryInput(
                        Files.readAllBytes(
                                java.nio.file.Paths.get(files.get(0).filePath().toUri())));
        try (GlobalIndexReader reader =
                indexer.createReader(
                        file -> stream, files, 128, null, newDirectExecutorService())) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 127, 0), expected);
        }
        // Header plus one range per level rather than 127 individual page requests.
        assertThat(stream.reads.get()).isLessThanOrEqualTo(9);
        assertThat(stream.closed).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDisjointReadsAreParallelAndDrainedBeforeClose(boolean fail) throws Exception {
        RTreeGlobalIndexer indexer = indexer(4, 8192, 0);
        List<byte[]> values = new ArrayList<>();
        List<Long> expected = new ArrayList<>();
        for (int y = 0; y < 64; y++) {
            for (int x = 0; x < 64; x++) {
                values.add(point(x, y));
                if (y == 32) {
                    expected.add((long) y * 64 + x);
                }
            }
        }
        List<GlobalIndexIOMeta> files = write(indexer, values);
        RTreeIndexFile.Metadata meta = RTreeIndexFile.Metadata.read(files.get(0), values.size());
        long leavesEnd =
                RTreeIndexFile.HEADER_SIZE
                        + (long) ((meta.items + 3) / 4) * RTreeIndexFile.pageBytes(4);
        ParallelInput stream =
                new ParallelInput(
                        Files.readAllBytes(
                                java.nio.file.Paths.get(files.get(0).filePath().toUri())),
                        leavesEnd,
                        fail);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (GlobalIndexReader reader =
                indexer.createReader(file -> stream, files, values.size(), null, executor)) {
            CompletableFuture<Optional<GlobalIndexResult>> future =
                    reader.visitBBoxIntersects(ref, 0, 32, 63, 32);
            if (fail) {
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("injected parallel read failure");
            } else {
                future.get(10, TimeUnit.SECONDS);
                assertRows(future, expected);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(stream.maxActive.get()).isBetween(2, 4);
        assertThat(stream.active.get()).isZero();
        assertThat(stream.closedWhileActive).isFalse();
        assertThat(stream.closed).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"bbox", "null", "non-null", "corrupt", "shards"})
    void testBoundedFileQueries(String predicate) throws Exception {
        Options options = new Options();
        options.set(RTreeIndexOptions.READ_FILE_CONCURRENCY, 2);
        options.set(RTreeIndexOptions.PARTITION_ROW_COUNT, 2);
        RTreeGlobalIndexer indexer = new RTreeGlobalIndexer(field, options);
        List<byte[]> values =
                Arrays.asList(
                        null,
                        point(0, 0),
                        null,
                        point(1, 0),
                        null,
                        point(2, 0),
                        null,
                        point(100, 0));
        List<GlobalIndexIOMeta> files = write(indexer, values);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        GlobalIndexFileReader tracked =
                file -> {
                    byte[] bytes =
                            Files.readAllBytes(java.nio.file.Paths.get(file.filePath().toUri()));
                    if ("corrupt".equals(predicate) && file == files.get(0)) {
                        bytes[0] ^= 1;
                    }
                    opens.incrementAndGet();
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    started.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("Files did not overlap");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    return new MemoryInput(bytes) {
                        @Override
                        public void close() {
                            super.close();
                            active.decrementAndGet();
                        }
                    };
                };
        ExecutorService executor = Executors.newFixedThreadPool("shards".equals(predicate) ? 3 : 6);
        try (GlobalIndexReader reader =
                "shards".equals(predicate)
                        ? new UnionGlobalIndexReader(
                                files.stream()
                                        .map(
                                                file ->
                                                        indexer.createReader(
                                                                tracked,
                                                                Collections.singletonList(file),
                                                                8,
                                                                null,
                                                                executor))
                                        .collect(java.util.stream.Collectors.toList()))
                        : indexer.createReader(tracked, files, 8, null, executor)) {
            CompletableFuture<Optional<GlobalIndexResult>> result;
            List<Long> expected;
            if ("null".equals(predicate)) {
                result = reader.visitIsNull(ref);
                expected = Arrays.asList(0L, 2L, 4L, 6L);
            } else if ("non-null".equals(predicate) || "shards".equals(predicate)) {
                result = reader.visitIsNotNull(ref);
                expected = Arrays.asList(1L, 3L, 5L, 7L);
            } else {
                result = reader.visitBBoxIntersects(ref, 0, 0, 2, 0);
                expected = Arrays.asList(1L, 3L, 5L);
            }
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(result).isNotDone();
            if ("shards".equals(predicate)) {
                // The two active file reads leave the third executor thread available to other
                // queries.
                assertThat(executor.submit(() -> true).get(5, TimeUnit.SECONDS)).isTrue();
            }
            release.countDown();
            if ("corrupt".equals(predicate)) {
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage(
                                "R-tree file header disagrees with container metadata.");
            } else {
                result.get(5, TimeUnit.SECONDS);
                assertRows(result, expected);
                assertThat(opens.get()).isEqualTo("bbox".equals(predicate) ? 3 : 4);
            }
            assertThat(active).hasValue(0);
            assertThat(peak).hasValue(2);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testRejectedWorkerDrainsAdmittedFile() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 1, 0);
        List<GlobalIndexIOMeta> files = write(indexer, Arrays.asList(point(0, 0), point(1, 0)));
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        byte[] bytes = Files.readAllBytes(java.nio.file.Paths.get(files.get(0).filePath().toUri()));
        MemoryInput stream = new MemoryInput(bytes);
        GlobalIndexFileReader blocking =
                file -> {
                    assertThat(file).isSameAs(files.get(0));
                    opened.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting for file release");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    return stream;
                };
        ExecutorService pool = Executors.newSingleThreadExecutor();
        ExecutorService rejecting =
                new ForwardingExecutorService() {
                    private int submissions;

                    @Override
                    protected ExecutorService delegate() {
                        return pool;
                    }

                    @Override
                    public void execute(Runnable command) {
                        if (++submissions == 2) {
                            try {
                                assertThat(opened.await(5, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                            throw new RejectedExecutionException("injected rejection");
                        }
                        super.execute(command);
                    }
                };
        try (GlobalIndexReader reader = indexer.createReader(blocking, files, 2, null, rejecting)) {
            CompletableFuture<Optional<GlobalIndexResult>> query =
                    reader.visitBBoxIntersects(ref, 0, 0, 1, 0);
            assertThat(query).isNotDone();
            release.countDown();
            assertThatThrownBy(() -> query.get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("injected rejection");
            assertThat(stream.closed).isTrue();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void testRunningWorkerFinishesWhenExecutorDiscardsQueuedWorkers() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 1, 0);
        List<GlobalIndexIOMeta> files = write(indexer, Arrays.asList(point(0, 0), point(1, 0)));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<byte[]> bodies = new ArrayList<>();
        for (GlobalIndexIOMeta file : files) {
            bodies.add(Files.readAllBytes(java.nio.file.Paths.get(file.filePath().toUri())));
        }
        GlobalIndexFileReader input =
                file -> {
                    started.countDown();
                    if (!awaitUninterruptibly(release, 10, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting for executor shutdown");
                    }
                    return new MemoryInput(bodies.get(files.indexOf(file)));
                };
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (GlobalIndexReader reader = indexer.createReader(input, files, 2, null, pool)) {
            CompletableFuture<Optional<GlobalIndexResult>> query =
                    reader.visitBBoxIntersects(ref, 0, 0, 1, 0);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(pool.shutdownNow()).isNotEmpty();
            release.countDown();
            query.get(3, TimeUnit.SECONDS);
            assertRows(query, Arrays.asList(0L, 1L));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompletionCanWaitForAnotherQueryOnTheSameReader(boolean failFirst) throws Exception {
        Options options = new Options();
        options.set(RTreeIndexOptions.READ_FILE_CONCURRENCY, 1);
        RTreeGlobalIndexer indexer = new RTreeGlobalIndexer(field, options);
        List<GlobalIndexIOMeta> files = write(indexer, Arrays.asList(null, point(0, 0)));
        byte[] bytes = Files.readAllBytes(java.nio.file.Paths.get(files.get(0).filePath().toUri()));
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger opens = new AtomicInteger();
        GlobalIndexFileReader input =
                file -> {
                    if (!awaitUninterruptibly(release, 5, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting for callback registration");
                    }
                    if (failFirst && opens.getAndIncrement() == 0) {
                        throw new IOException("Injected read failure");
                    }
                    return new MemoryInput(bytes);
                };
        try (GlobalIndexReader reader = indexer.createReader(input, files, 2, null, pool)) {
            CompletableFuture<Optional<GlobalIndexResult>> next =
                    reader.visitIsNull(ref)
                            .handle(
                                    (ignored, error) -> {
                                        assertThat(error != null).isEqualTo(failFirst);
                                        try {
                                            return reader.visitIsNotNull(ref)
                                                    .get(5, TimeUnit.SECONDS);
                                        } catch (Exception e) {
                                            throw new AssertionError(e);
                                        }
                                    });
            release.countDown();
            next.get(10, TimeUnit.SECONDS);
            assertRows(next, Collections.singletonList(1L));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void testCloseFailsQueuedReadsAfterExecutorTerminates() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 1, 0);
        List<GlobalIndexIOMeta> files = write(indexer, Collections.singletonList(point(1, 1)));
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        pool.submit(
                () -> {
                    started.countDown();
                    awaitUninterruptibly(release);
                });
        GlobalIndexReader reader =
                indexer.createReader(
                        f -> {
                            throw new AssertionError();
                        },
                        files,
                        1,
                        null,
                        pool);
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Optional<GlobalIndexResult>> pending = reader.visitIsNotNull(ref);
            assertThat(pool.shutdownNow()).isNotEmpty();
            release.countDown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            reader.close();
            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
            pool.shutdownNow();
            reader.close();
        }
    }

    @Test
    void testFileConcurrencyMustBePositive() {
        Options options = new Options();
        options.set(RTreeIndexOptions.READ_FILE_CONCURRENCY, 0);
        assertThatThrownBy(() -> new RTreeGlobalIndexer(field, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file concurrency");
    }

    @Test
    void testCacheBudgetAndFileIdentity() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 2, 0);
        GlobalIndexIOMeta first = write(indexer, Collections.singletonList(point(1, 1))).get(0);
        GlobalIndexIOMeta second = write(indexer, Collections.singletonList(point(2, 2))).get(0);
        RTreeIndexReadContext context = new RTreeIndexReadContext(200, 1);
        RTreeIndexReadContext.FileKey a =
                new RTreeIndexReadContext.FileKey(first, first.metadata());
        RTreeIndexReadContext.FileKey b =
                new RTreeIndexReadContext.FileKey(second, second.metadata());
        PageBlock firstPage = new PageBlock(0, 92, new byte[92]);
        PageBlock nextPage = new PageBlock(1, 92, new byte[92]);
        context.put(a, firstPage);
        context.put(b, new PageBlock(0, 92, new byte[92]));
        assertThat(context.get(a, 0)).isSameAs(firstPage);
        context.put(a, nextPage);
        assertThat(context.cachedBytes()).isEqualTo(184);
        assertThat(context.get(b, 0)).isNull();
        // An oversized page must not displace useful cached pages.
        context.put(b, new PageBlock(1, 201, new byte[201]));
        assertThat(context.get(a, 0)).isSameAs(firstPage);
        assertThat(context.get(a, 1)).isSameAs(nextPage);
        // Reusing a path with different metadata must not reuse old pages.
        RTreeIndexReadContext.FileKey changed =
                new RTreeIndexReadContext.FileKey(first, second.metadata());
        assertThat(context.get(changed, 0)).isNull();
    }

    @Test
    void testOverlappingPageBlocksChargeLiveBackingArrays() throws Exception {
        GlobalIndexIOMeta file =
                write(indexer(2, 2, 0), Collections.singletonList(point(1, 1))).get(0);
        RTreeIndexReadContext.FileKey key =
                new RTreeIndexReadContext.FileKey(file, file.metadata());
        RTreeIndexReadContext context = new RTreeIndexReadContext(4, 1);
        PageBlock first = new PageBlock(0, 1, new byte[] {10, 11});
        PageBlock overlap = new PageBlock(1, 1, new byte[] {11, 12});
        context.put(key, first);
        context.put(key, overlap);
        assertThat(context.cachedBytes()).isEqualTo(4);
        assertThat(context.get(key, 0)).isSameAs(first);
        assertThat(context.get(key, 1)).isSameAs(overlap);
        assertThat(context.get(key, 2)).isSameAs(overlap);

        context.put(key, new PageBlock(3, 1, new byte[] {13}));
        assertThat(context.get(key, 0)).isNull();
        assertThat(context.cachedBytes()).isEqualTo(3);
        // A query holding the evicted block can still read it with its own cursor.
        assertThat(first.page(1).get()).isEqualTo((byte) 11);
        assertThat(overlap.page(1).get()).isEqualTo((byte) 11);
        assertThat(overlap.page(2).get()).isEqualTo((byte) 12);
    }

    @Test
    void testOversizedBlockCachesOnlyWholePagesThatFit() throws Exception {
        GlobalIndexIOMeta file =
                write(indexer(2, 2, 0), Collections.singletonList(point(1, 1))).get(0);
        RTreeIndexReadContext.FileKey key =
                new RTreeIndexReadContext.FileKey(file, file.metadata());
        RTreeIndexReadContext context = new RTreeIndexReadContext(5, 1);
        PageBlock source = new PageBlock(7, 2, new byte[] {10, 11, 12, 13, 14, 15});
        context.put(key, source);
        assertThat(context.cachedBytes()).isEqualTo(4);
        assertThat(context.get(key, 7)).isNull();
        PageBlock tail = context.get(key, 8);
        assertThat(tail).isNotSameAs(source);
        assertThat(context.get(key, 9)).isSameAs(tail);
        assertThat(tail.page(8).getShort()).isEqualTo((short) 0x0c0d);
        assertThat(tail.page(9).getShort()).isEqualTo((short) 0x0e0f);
    }

    @Test
    void testChecksumUsesIndividualPageBoundariesInMergedRead() throws IOException {
        byte[] a = {1, 2, 3, 4, 0, 0, 0, 0};
        byte[] b = {5, 6, 7, 8, 0, 0, 0, 0};
        RTreeIndexFile.checksum(a);
        RTreeIndexFile.checksum(b);
        byte[] merged = ByteBuffer.allocate(16).put(a).put(b).array();
        RTreeIndexFile.verify(merged, 8, 8);
        merged[8] ^= 1;
        RTreeIndexFile.verify(merged, 0, 8);
        assertThatThrownBy(() -> RTreeIndexFile.verify(merged, 8, 8))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void testQueryInterruptionDrainsPositionedReadsBeforeClose() throws Exception {
        RTreeGlobalIndexer indexer = indexer(4, 8192, 0);
        List<byte[]> values = new ArrayList<>();
        List<Long> expected = new ArrayList<>();
        for (int y = 0; y < 64; y++) {
            for (int x = 0; x < 64; x++) {
                values.add(point(x, y));
                if (y == 32) {
                    expected.add((long) y * 64 + x);
                }
            }
        }
        List<GlobalIndexIOMeta> files = write(indexer, values);
        long leavesEnd = RTreeIndexFile.HEADER_SIZE + 1024L * RTreeIndexFile.pageBytes(4);
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        ParallelInput stream =
                new ParallelInput(
                        Files.readAllBytes(
                                java.nio.file.Paths.get(files.get(0).filePath().toUri())),
                        leavesEnd,
                        false,
                        started,
                        release);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (GlobalIndexReader reader =
                indexer.createReader(file -> stream, files, values.size(), null, executor)) {
            CompletableFuture<Optional<GlobalIndexResult>> future =
                    reader.visitBBoxIntersects(ref, 0, 32, 63, 32);
            try {
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                executor.shutdownNow();
                // Held I/O must keep both the query and stream alive after interruption.
                assertThatThrownBy(() -> future.get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                assertThat(stream.closed).isFalse();
            } finally {
                release.countDown();
            }
            future.get(10, TimeUnit.SECONDS);
            assertRows(future, expected);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
        assertThat(stream.closedWhileActive).isFalse();
        assertThat(stream.closed).isTrue();
    }

    @Test
    void testCorruptMetadataIsRejectedBeforeRangePruning() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        GlobalIndexIOMeta file = write(indexer, Collections.singletonList(point(1, 2))).get(0);
        byte[] bad = file.metadata().clone();
        bad[12] ^= 1;
        GlobalIndexIOMeta corrupted = new GlobalIndexIOMeta(file.filePath(), file.fileSize(), bad);
        assertThatThrownBy(
                        () ->
                                indexer.createReader(
                                        input,
                                        Collections.singletonList(corrupted),
                                        1,
                                        Collections.emptyList(),
                                        newDirectExecutorService()))
                .hasRootCauseMessage("R-tree checksum mismatch.");
        byte[] version = file.metadata().clone();
        ByteBuffer.wrap(version).putInt(4, 2);
        RTreeIndexFile.checksum(version);
        assertThatThrownBy(
                        () ->
                                reader(
                                        indexer,
                                        Collections.singletonList(
                                                new GlobalIndexIOMeta(
                                                        file.filePath(), file.fileSize(), version)),
                                        1))
                .hasRootCauseMessage("Unsupported R-tree file format.");
        assertThatThrownBy(
                        () ->
                                reader(
                                        indexer,
                                        Collections.singletonList(
                                                new GlobalIndexIOMeta(
                                                        file.filePath(),
                                                        file.fileSize() - 1,
                                                        file.metadata())),
                                        1))
                .hasRootCauseMessage("R-tree file size or source row count mismatch.");
    }

    @Test
    void testCorruptHeaderPageNullBlockAndTruncationFailQueries() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        for (int kind = 0; kind < 4; kind++) {
            GlobalIndexIOMeta file =
                    write(indexer, Arrays.asList(point(0, 0), null, point(1, 1), point(2, 2)))
                            .get(0);
            RTreeIndexFile.Metadata meta = RTreeIndexFile.Metadata.read(file, 4);
            try (RandomAccessFile bytes =
                    new RandomAccessFile(file.filePath().toUri().getPath(), "rw")) {
                if (kind == 3) {
                    bytes.setLength(meta.rowsOffset() - 1);
                } else {
                    long offset =
                            kind == 0
                                    ? 0
                                    : kind == 1
                                            ? RTreeIndexFile.HEADER_SIZE
                                                    + (long) (meta.pages - 1)
                                                            * RTreeIndexFile.pageBytes(2)
                                                    + 8
                                            : meta.rowsOffset();
                    bytes.seek(offset);
                    int original = bytes.read();
                    bytes.seek(offset);
                    bytes.write(original ^ 1);
                }
            }
            try (GlobalIndexReader reader = reader(indexer, Collections.singletonList(file), 4)) {
                final boolean nullQuery = kind == 2;
                assertThatThrownBy(
                                () ->
                                        (nullQuery
                                                        ? reader.visitIsNull(ref)
                                                        : reader.visitBBoxIntersects(
                                                                ref, -1, -1, 3, 3))
                                                .join())
                        .hasCauseInstanceOf(java.io.UncheckedIOException.class);
            }
        }
    }

    @Test
    void testOptionsDiscoveryWriterLifecycleAndMalformedInput() throws Exception {
        Options options = new Options();
        options.set("global-index.rtree.index-columns", "geom");
        options.set("fields.geom.index-options", "{\"page-size\":2}");
        GlobalIndexer resolved =
                GlobalIndexer.create("rtree", Collections.singletonList(field), options);
        assertThat(resolved).isInstanceOf(RTreeGlobalIndexer.class);
        assertThat(CoreOptions.isVectorIndexType("rtree")).isFalse();
        try (RTreeGlobalIndexWriter writer = ((RTreeGlobalIndexer) resolved).createWriter(output)) {
            assertThatThrownBy(() -> writer.write(point(1, 2), -1))
                    .hasMessageContaining("non-negative");
            assertThatThrownBy(() -> writer.write(new byte[] {1}, 0))
                    .hasMessageContaining("Truncated");
            writer.write(point(1, 2), 0);
            List<GlobalIndexIOMeta> files = metas(writer.finish());
            assertThat(RTreeIndexFile.Metadata.read(files.get(0), 1).pageSize).isEqualTo(2);
            assertThatThrownBy(() -> writer.write(point(1, 2), 1)).hasMessageContaining("finished");
            assertThatThrownBy(writer::finish).hasMessageContaining("finished");
        }
        for (org.apache.paimon.types.DataType type :
                Arrays.asList(DataTypes.GEOGRAPHY(), DataTypes.BYTES())) {
            assertThatThrownBy(
                            () ->
                                    new RTreeGlobalIndexer(
                                            new DataField(1, "bad", type), new Options()))
                    .hasMessageContaining("GEOMETRY");
        }
        for (int pageSize : new int[] {0, 1, 4097}) {
            assertThatThrownBy(() -> indexer(pageSize, 10, 0)).hasMessageContaining("page size");
        }
        assertThatThrownBy(() -> indexer(2, 0, 0)).hasMessageContaining("partition row count");
        assertThatThrownBy(() -> indexer(2, 10, -1)).hasMessageContaining("cache page count");
    }

    @Test
    void testLargestInteroperableRowId() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        try (RTreeGlobalIndexWriter writer = indexer.createWriter(output)) {
            assertThatThrownBy(() -> writer.write(null, Long.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class);
            writer.write(null, Long.MAX_VALUE - 1);
            try (GlobalIndexReader reader =
                    reader(indexer, metas(writer.finish()), Long.MAX_VALUE)) {
                assertRows(reader.visitIsNull(ref), Collections.singletonList(Long.MAX_VALUE - 1));
            }
        }
    }

    @Test
    void testWriteFailurePreservesCause() {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        GlobalIndexFileWriter failing =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "failed.index";
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name) throws IOException {
                        throw new IOException("injected write failure");
                    }
                };
        try (RTreeGlobalIndexWriter writer = indexer.createWriter(failing)) {
            writer.write(point(1, 2), 0);
            assertThatThrownBy(writer::finish).hasRootCauseMessage("injected write failure");
        }
    }

    @Test
    void testUnorderedSourceIds() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        for (long last : new long[] {3, 5}) {
            try (RTreeGlobalIndexWriter writer = indexer.createWriter(output)) {
                writer.write(null, 5);
                writer.write(point(0, 0), last);
                if (last == 5) {
                    assertThatThrownBy(writer::finish).hasMessageContaining("Duplicate");
                } else {
                    try (GlobalIndexReader reader = reader(indexer, metas(writer.finish()), 6)) {
                        assertRows(reader.visitIsNull(ref), Collections.singletonList(5L));
                        assertRows(reader.visitIsNotNull(ref), Collections.singletonList(3L));
                    }
                }
            }
        }
    }

    @Test
    void testEmptyGeometryOnlyAndPhysicalRowLimit() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 10, 0);
        GlobalIndexIOMeta file = write(indexer, Collections.singletonList(line())).get(0);
        byte[] body = Files.readAllBytes(java.nio.file.Paths.get(file.filePath().toUri()));
        assertThat(body).hasSize(96);
        assertThat(ByteBuffer.wrap(body).getInt(80)).isZero();
        try (GlobalIndexReader reader = reader(indexer, Collections.singletonList(file), 1)) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 1, 1), Collections.emptyList());
            assertRows(reader.visitIsNotNull(ref), Collections.singletonList(0L));
        }
        for (int count : new int[] {1 << 20, (1 << 20) + 1}) {
            byte[] header = file.metadata().clone();
            ByteBuffer.wrap(header).putInt(24, count).putLong(36, count - 1);
            RTreeIndexFile.checksum(header);
            GlobalIndexIOMeta manifest =
                    new GlobalIndexIOMeta(file.filePath(), 88L + 8L * count, count, header);
            if (count == 1 << 20) {
                assertThat(RTreeIndexFile.Metadata.read(manifest, count).rowCount())
                        .isEqualTo(count);
            } else {
                assertThatThrownBy(() -> RTreeIndexFile.Metadata.read(manifest, count))
                        .hasRootCauseMessage("Invalid R-tree header counts or row range.");
            }
        }
    }

    private GlobalIndexIOMeta fixtureFile(byte[] bytes, int rows) throws IOException {
        java.nio.file.Path path = Files.createTempFile(directory, "fixture-", ".index");
        Files.write(path, bytes);
        return new GlobalIndexIOMeta(
                new Path(path.toUri()), bytes.length, rows, Arrays.copyOf(bytes, 80));
    }

    private static byte[] fixture(String name) throws IOException {
        return Base64.getMimeDecoder()
                .decode(Resources.toByteArray(Resources.getResource("rtree/" + name + ".b64")));
    }

    @Test
    void testMixedInteropFixtures() throws Exception {
        RTreeGlobalIndexer indexer = indexer(3, 64, 0);
        List<Long> spatial = new ArrayList<>();
        List<Long> nulls = new ArrayList<>();
        List<Long> nonNulls = new ArrayList<>();
        byte[] golden = fixture("mixed-v1");
        try (RTreeGlobalIndexWriter writer = indexer.createWriter(output)) {
            for (int i = 0; i < 64; i++) {
                long id = (1L << 33) + i * 13;
                if (i % 7 == 0) {
                    writer.write(null, id);
                    nulls.add(id);
                } else {
                    nonNulls.add(id);
                    if (i % 11 == 0) {
                        writer.write(point(Double.NaN, Double.NaN), id);
                    } else {
                        writer.write(point(i % 9 - 4, i * i % 13 - 6), id);
                        spatial.add(id);
                    }
                }
            }
            GlobalIndexIOMeta file = metas(writer.finish()).get(0);
            assertThat(Files.readAllBytes(java.nio.file.Paths.get(file.filePath().toUri())))
                    .isEqualTo(golden);
            assertThat(file.metadata()).isEqualTo(Arrays.copyOf(golden, 80));
        }
        try (GlobalIndexReader reader =
                reader(
                        indexer,
                        Collections.singletonList(fixtureFile(golden, 64)),
                        (1L << 33) + 64 * 13)) {
            assertRows(reader.visitBBoxIntersects(ref, -4, -6, 4, 6), spatial);
            assertRows(reader.visitIsNull(ref), nulls);
            assertRows(reader.visitIsNotNull(ref), nonNulls);
        }
    }

    @Test
    void testIndependentRowBlockReadsAndChecksums() throws Exception {
        byte[] bytes = fixture("mixed-v1");
        int rowsOffset = 80 + 26 * 132;
        List<Long> offsets = new ArrayList<>();
        List<Integer> lengths = new ArrayList<>();
        GlobalIndexFileReader traced =
                file ->
                        new MemoryInput(bytes) {
                            @Override
                            public int read(byte[] target, int offset, int length) {
                                offsets.add(getPos());
                                lengths.add(length);
                                return super.read(target, offset, length);
                            }
                        };
        GlobalIndexIOMeta file = fixtureFile(bytes, 64);
        try (GlobalIndexReader reader =
                indexer(3, 64, 0)
                        .createReader(
                                traced,
                                Collections.singletonList(file),
                                (1L << 33) + 64 * 13,
                                null,
                                newDirectExecutorService())) {
            reader.visitIsNull(ref).join();
            reader.visitIsNotNull(ref).join();
            assertThat(offsets).containsExactly(0L, (long) rowsOffset, 0L, rowsOffset + 84L);
            assertThat(lengths).containsExactly(80, 84, 80, 436);
            bytes[rowsOffset] ^= 1;
            assertThatThrownBy(() -> reader.visitIsNull(ref).join())
                    .hasRootCauseMessage("R-tree checksum mismatch.");
            assertThat(reader.visitIsNotNull(ref).join().get().results().getLongCardinality())
                    .isEqualTo(54);
            bytes[rowsOffset] ^= 1;
            // Duplicate the first non-null ID and repair the CRC. The null block stays valid.
            int start = rowsOffset + 84;
            byte[] block = Arrays.copyOfRange(bytes, start, bytes.length);
            System.arraycopy(block, 0, block, 8, 8);
            RTreeIndexFile.checksum(block);
            System.arraycopy(block, 0, bytes, start, block.length);
            assertThatThrownBy(() -> reader.visitIsNotNull(ref).join())
                    .hasRootCauseMessage("Duplicate R-tree source row ID.");
            assertThat(reader.visitIsNull(ref).join().get().results().getLongCardinality())
                    .isEqualTo(10);
        }
    }

    @Test
    void testInterleavedChildPages() throws Exception {
        RTreeGlobalIndexer indexer = indexer(2, 8, 0);
        byte[] bytes = fixture("interleaved-v1");
        try (GlobalIndexReader reader =
                reader(indexer, Collections.singletonList(fixtureFile(bytes, 8)), 8)) {
            assertRows(reader.visitBBoxIntersects(ref, 0, 0, 3, 0), Arrays.asList(0L, 1L, 2L, 3L));
        }
        for (long child : new long[] {0, 4}) {
            byte[] root = Arrays.copyOfRange(bytes, 80 + 6 * 92, 80 + 7 * 92);
            ByteBuffer.wrap(root).putLong(80, child);
            RTreeIndexFile.checksum(root);
            System.arraycopy(root, 0, bytes, 80 + 6 * 92, root.length);
            try (GlobalIndexReader reader =
                    reader(indexer, Collections.singletonList(fixtureFile(bytes, 8)), 8)) {
                assertThatThrownBy(() -> reader.visitBBoxIntersects(ref, 0, 0, 3, 0).join())
                        .hasRootCauseMessage("Invalid R-tree child page.");
            }
        }
    }

    private RTreeGlobalIndexer indexer(int pageSize, int partitionRows, int cachePages) {
        Options options = new Options();
        options.set(RTreeIndexOptions.PAGE_SIZE, pageSize);
        options.set(RTreeIndexOptions.PARTITION_ROW_COUNT, partitionRows);
        options.set(RTreeIndexOptions.READ_CACHE_PAGES, cachePages);
        return new RTreeGlobalIndexer(field, options);
    }

    private List<GlobalIndexIOMeta> write(RTreeGlobalIndexer indexer, List<byte[]> values)
            throws IOException {
        try (RTreeGlobalIndexWriter writer = indexer.createWriter(output)) {
            for (int i = 0; i < values.size(); i++) {
                writer.write(values.get(i), i);
            }
            return metas(writer.finish());
        }
    }

    private List<GlobalIndexIOMeta> metas(List<ResultEntry> entries) throws IOException {
        List<GlobalIndexIOMeta> files = new ArrayList<>();
        for (ResultEntry entry : entries) {
            Path path = new Path(base, entry.fileName());
            files.add(
                    new GlobalIndexIOMeta(
                            path, io.getFileSize(path), entry.rowCount(), entry.meta()));
        }
        return files;
    }

    private GlobalIndexReader reader(
            RTreeGlobalIndexer indexer, List<GlobalIndexIOMeta> files, long rows) {
        return indexer.createReader(input, files, rows, null, newDirectExecutorService());
    }

    private static void assertRows(
            CompletableFuture<Optional<GlobalIndexResult>> result, List<Long> expected) {
        List<Long> actual = new ArrayList<>();
        for (long row : result.join().get().results()) {
            actual.add(row);
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static class MemoryInput extends SeekableInputStream {
        final byte[] bytes;
        final AtomicInteger reads = new AtomicInteger();
        int position;
        volatile boolean closed;

        MemoryInput(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public void seek(long desired) {
            position = (int) desired;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            reads.incrementAndGet();
            int count = Math.min(length, bytes.length - position);
            if (count == 0) {
                return -1;
            }
            System.arraycopy(bytes, position, target, offset, count);
            position += count;
            return count;
        }

        @Override
        public int read() {
            return position < bytes.length ? bytes[position++] & 255 : -1;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class ParallelInput extends MemoryInput implements VectoredReadable {
        private final long leavesEnd;
        private final boolean fail;
        private final CountDownLatch started;
        private final CountDownLatch release;
        private final CountDownLatch leafReads = new CountDownLatch(2);
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private volatile boolean closedWhileActive;

        ParallelInput(byte[] bytes, long leavesEnd, boolean fail) {
            this(bytes, leavesEnd, fail, null, null);
        }

        ParallelInput(
                byte[] bytes,
                long leavesEnd,
                boolean fail,
                CountDownLatch started,
                CountDownLatch release) {
            super(bytes);
            this.leavesEnd = leavesEnd;
            this.fail = fail;
            this.started = started;
            this.release = release;
        }

        @Override
        public int pread(long position, byte[] target, int offset, int length) throws IOException {
            int running = active.incrementAndGet();
            maxActive.accumulateAndGet(running, Math::max);
            try {
                if (position < leavesEnd) {
                    leafReads.countDown();
                    try {
                        if (!leafReads.await(2, TimeUnit.SECONDS)) {
                            throw new IOException("Expected overlapping leaf reads");
                        }
                        if (started != null) {
                            started.countDown();
                            if (!release.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Timed out waiting for test read release");
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    if (fail) {
                        throw new IOException("injected parallel read failure");
                    }
                }
                System.arraycopy(bytes, (int) position, target, offset, length);
                return length;
            } finally {
                active.decrementAndGet();
            }
        }

        @Override
        public void close() {
            closedWhileActive = active.get() != 0;
            super.close();
        }
    }
}
