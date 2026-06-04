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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Thread-safety tests for BTree global index readers. */
public class BTreeThreadSafetyTest {

    private static final CacheManager CACHE_MANAGER = new CacheManager(MemorySize.VALUE_8_MB);

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private GlobalIndexFileReader fileReader;
    private GlobalIndexFileWriter fileWriter;
    private BTreeGlobalIndexer globalIndexer;
    private KeySerializer keySerializer;
    private Comparator<Object> comparator;
    private ExecutorService executor;

    private List<Pair<Object, Long>> data;
    private static final int DATA_NUM = 10000;
    private static final int FILE_NUM = 10;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + UUID.randomUUID() + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        fileReader =
                meta ->
                        fileIO.newInputStream(
                                new Path(new Path(tempPath.toUri()), meta.filePath()));

        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(8));
        DataField dataField = new DataField(1, "id", new IntType());
        globalIndexer = new BTreeGlobalIndexer(dataField, options);
        keySerializer = KeySerializer.create(new IntType());
        comparator = keySerializer.createComparator();

        data = new ArrayList<>(DATA_NUM);
        for (int i = 0; i < DATA_NUM; i++) {
            data.add(Pair.of(i, (long) i));
        }
        data.sort((p1, p2) -> comparator.compare(p1.getKey(), p2.getKey()));
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void testNoDeadlockMoreFilesThanThreads() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        List<GlobalIndexIOMeta> metas = writeMultipleFiles();

        try (GlobalIndexReader reader = globalIndexer.createReader(fileReader, metas, executor)) {
            FieldRef ref = new FieldRef(1, "id", new IntType());
            Optional<GlobalIndexResult> result =
                    reader.visitIsNotNull(ref).get(10, TimeUnit.SECONDS);
            assertThat(result).isPresent();
            assertThat(result.get().results().iterator().hasNext()).isTrue();
        }
    }

    @Test
    void testConcurrentVisitEqualOnSameReader() throws Exception {
        executor = Executors.newFixedThreadPool(8);
        List<GlobalIndexIOMeta> metas = writeMultipleFiles();

        try (GlobalIndexReader reader = globalIndexer.createReader(fileReader, metas, executor)) {
            FieldRef ref = new FieldRef(1, "id", new IntType());
            int numThreads = 32;
            CountDownLatch latch = new CountDownLatch(numThreads);
            ExecutorService queryPool = Executors.newFixedThreadPool(numThreads);

            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                int literal = (i * 313) % DATA_NUM;
                futures.add(
                        queryPool.submit(
                                () -> {
                                    latch.countDown();
                                    latch.await();
                                    Optional<GlobalIndexResult> result =
                                            reader.visitEqual(ref, literal)
                                                    .get(10, TimeUnit.SECONDS);
                                    assertThat(result).isPresent();
                                    return toList(result.get());
                                }));
            }

            for (int i = 0; i < numThreads; i++) {
                int literal = (i * 313) % DATA_NUM;
                List<Long> expected =
                        data.stream()
                                .filter(p -> comparator.compare(p.getKey(), literal) == 0)
                                .map(Pair::getValue)
                                .collect(Collectors.toList());
                List<Long> actual = futures.get(i).get(15, TimeUnit.SECONDS);
                assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
            }

            queryPool.shutdown();
            queryPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConcurrentMixedPredicates() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        List<GlobalIndexIOMeta> metas = writeMultipleFiles();

        try (GlobalIndexReader reader = globalIndexer.createReader(fileReader, metas, executor)) {
            FieldRef ref = new FieldRef(1, "id", new IntType());
            int numThreads = 24;
            CountDownLatch latch = new CountDownLatch(numThreads);
            ExecutorService queryPool = Executors.newFixedThreadPool(numThreads);

            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                int idx = i;
                futures.add(
                        queryPool.submit(
                                () -> {
                                    latch.countDown();
                                    latch.await();
                                    Optional<GlobalIndexResult> result;
                                    switch (idx % 4) {
                                        case 0:
                                            result =
                                                    reader.visitEqual(ref, idx * 100)
                                                            .get(10, TimeUnit.SECONDS);
                                            break;
                                        case 1:
                                            result =
                                                    reader.visitLessThan(ref, idx * 100)
                                                            .get(10, TimeUnit.SECONDS);
                                            break;
                                        case 2:
                                            result =
                                                    reader.visitGreaterThan(ref, idx * 100)
                                                            .get(10, TimeUnit.SECONDS);
                                            break;
                                        default:
                                            result =
                                                    reader.visitBetween(ref, idx * 50, idx * 100)
                                                            .get(10, TimeUnit.SECONDS);
                                            break;
                                    }
                                    assertThat(result).isPresent();
                                    return toList(result.get());
                                }));
            }

            for (int i = 0; i < numThreads; i++) {
                int idx = i;
                List<Long> expected;
                switch (idx % 4) {
                    case 0:
                        expected = filter(obj -> comparator.compare(obj, idx * 100) == 0);
                        break;
                    case 1:
                        expected = filter(obj -> comparator.compare(obj, idx * 100) < 0);
                        break;
                    case 2:
                        expected = filter(obj -> comparator.compare(obj, idx * 100) > 0);
                        break;
                    default:
                        expected =
                                filter(
                                        obj ->
                                                comparator.compare(obj, idx * 50) >= 0
                                                        && comparator.compare(obj, idx * 100) <= 0);
                        break;
                }
                List<Long> actual = futures.get(i).get(15, TimeUnit.SECONDS);
                assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
            }

            queryPool.shutdown();
            queryPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testLazyCreationOnlyOncePerFile() throws Exception {
        AtomicInteger creationCount = new AtomicInteger(0);

        executor = Executors.newFixedThreadPool(8);
        List<GlobalIndexIOMeta> metas = writeMultipleFiles();

        GlobalIndexFileReader countingFileReader =
                meta -> {
                    creationCount.incrementAndGet();
                    return fileIO.newInputStream(
                            new Path(new Path(tempPath.toUri()), meta.filePath()));
                };

        try (GlobalIndexReader reader =
                globalIndexer.createReader(countingFileReader, metas, executor)) {
            FieldRef ref = new FieldRef(1, "id", new IntType());
            int numThreads = 32;
            CountDownLatch latch = new CountDownLatch(numThreads);
            ExecutorService queryPool = Executors.newFixedThreadPool(numThreads);

            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                futures.add(
                        queryPool.submit(
                                () -> {
                                    latch.countDown();
                                    try {
                                        latch.await();
                                        reader.visitIsNotNull(ref).get(10, TimeUnit.SECONDS);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }));
            }

            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }

            queryPool.shutdown();
            queryPool.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Each file should be opened at most once per reader creation
        // (ConcurrentHashMap.computeIfAbsent).
        // BTreeIndexReader opens the file once during construction.
        assertThat(creationCount.get()).isLessThanOrEqualTo(FILE_NUM);
    }

    @Test
    void testSelectorPrunesCorrectly() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        List<GlobalIndexIOMeta> metas = writeMultipleFiles();

        Set<String> openedFiles = new HashSet<>();
        GlobalIndexFileReader trackingFileReader =
                meta -> {
                    synchronized (openedFiles) {
                        openedFiles.add(meta.filePath().getName());
                    }
                    return fileIO.newInputStream(
                            new Path(new Path(tempPath.toUri()), meta.filePath()));
                };

        try (GlobalIndexReader reader =
                globalIndexer.createReader(trackingFileReader, metas, executor)) {
            FieldRef ref = new FieldRef(1, "id", new IntType());

            // Query for value 5 — should only need to open the first file (keys 0-999)
            Optional<GlobalIndexResult> result =
                    reader.visitEqual(ref, 5).get(10, TimeUnit.SECONDS);
            assertThat(result).isPresent();
            List<Long> hits = toList(result.get());
            assertThat(hits).contains(5L);

            // Should NOT have opened all files
            assertThat(openedFiles.size()).isLessThan(FILE_NUM);
        }
    }

    private List<GlobalIndexIOMeta> writeMultipleFiles() throws IOException {
        int recordPerFile = DATA_NUM / FILE_NUM;
        List<GlobalIndexIOMeta> metas = new ArrayList<>(FILE_NUM);
        int currentStart = 0;
        while (currentStart < DATA_NUM) {
            int nextStart = Math.min(currentStart + recordPerFile, DATA_NUM);
            metas.add(writeData(data.subList(currentStart, nextStart)));
            currentStart = nextStart;
        }
        return metas;
    }

    private GlobalIndexIOMeta writeData(List<Pair<Object, Long>> subData) throws IOException {
        GlobalIndexParallelWriter indexWriter = globalIndexer.createWriter(fileWriter);
        for (Pair<Object, Long> pair : subData) {
            indexWriter.write(pair.getKey(), pair.getValue());
        }
        List<ResultEntry> results = indexWriter.finish();
        Assertions.assertEquals(1, results.size());
        ResultEntry resultEntry = results.get(0);
        String fileName = resultEntry.fileName();
        return new GlobalIndexIOMeta(
                new Path(new Path(tempPath.toUri()), fileName),
                fileIO.getFileSize(new Path(new Path(tempPath.toUri()), fileName)),
                resultEntry.meta());
    }

    private List<Long> filter(java.util.function.Predicate<Object> predicate) {
        return data.stream()
                .filter(pair -> predicate.test(pair.getKey()))
                .map(Pair::getValue)
                .collect(Collectors.toList());
    }

    private static List<Long> toList(GlobalIndexResult result) {
        Iterator<Long> iter = result.results().iterator();
        List<Long> list = new ArrayList<>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        return list;
    }
}
