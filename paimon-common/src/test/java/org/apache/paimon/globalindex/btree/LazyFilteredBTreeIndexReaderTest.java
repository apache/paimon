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

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SemaphoredDelegatingExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LazyFilteredBTreeReader} to read multiple files. */
@ExtendWith(ParameterizedTestExtension.class)
public class LazyFilteredBTreeIndexReaderTest extends AbstractIndexReaderTest {

    LazyFilteredBTreeIndexReaderTest(List<Object> args) {
        super(args);
    }

    @Override
    protected GlobalIndexReader prepareDataAndCreateReader() throws Exception {
        List<GlobalIndexIOMeta> written = writeData();
        return globalIndexer.createReader(fileReader, written, newDirectExecutorService());
    }

    private List<GlobalIndexIOMeta> writeData() throws Exception {
        int fileNum = 10;
        List<GlobalIndexIOMeta> written = new ArrayList<>(fileNum);

        int currentStart = 0;
        int recordPerFile = dataNum / fileNum;
        while (currentStart < dataNum) {
            int nextStart = Math.min(currentStart + recordPerFile, dataNum);
            written.add(writeData(data.subList(currentStart, nextStart)));
            currentStart = nextStart;
        }

        return written;
    }

    @TestTemplate
    public void testUnorderedIterator() throws Exception {
        // Set some null values
        for (int i = dataNum - 1; i >= dataNum * 0.85; i--) {
            data.get(i).setLeft(null);
        }

        List<GlobalIndexIOMeta> written = writeData();

        Comparator<Object> nullComparator = Comparator.nullsFirst(comparator);

        // Build expected map from original data
        Map<Object, Set<Long>> expectedMap = new TreeMap<>(nullComparator);
        for (Pair<Object, Long> pair : data) {
            Object key = pair.getKey();
            Long rowId = pair.getValue();

            if (!expectedMap.containsKey(key)) {
                expectedMap.put(key, new TreeSet<>());
            }
            expectedMap.get(key).add(rowId);
        }

        Map<Object, Set<Long>> actualMap = new TreeMap<>(nullComparator);

        for (GlobalIndexIOMeta index : written) {
            try (BTreeIndexReader reader =
                    new BTreeIndexReader(keySerializer, fileReader, index, CACHE_MANAGER)) {

                TreeSet<Long> nullRowIds = new TreeSet<>();
                reader.scanNullRowIds(nullRowIds::add);
                if (!nullRowIds.isEmpty()) {
                    if (!actualMap.containsKey(null)) {
                        actualMap.put(null, new TreeSet<>());
                    }
                    actualMap.get(null).addAll(nullRowIds);
                }

                BTreeIndexReader.EntryIterator iter = reader.entryIterator();

                // Collect all entries from iterator
                while (iter.hasNext()) {
                    BTreeIndexReader.KeyRowIds entry = iter.next();
                    Object key = entry.key();
                    long[] rowIds = entry.rowIds();

                    if (!actualMap.containsKey(key)) {
                        actualMap.put(key, new TreeSet<>());
                    }
                    for (long rowId : rowIds) {
                        actualMap.get(key).add(rowId);
                    }
                }
            }
        }

        // Verify all keys are present
        assertThat(actualMap.keySet()).containsExactlyElementsOf(expectedMap.keySet());

        // Verify all rowIds for each key
        for (Object key : expectedMap.keySet()) {
            assertThat(actualMap.get(key))
                    .as("RowIds for key: " + key)
                    .containsExactlyElementsOf(expectedMap.get(key));
        }
    }

    /**
     * Stress-test thread safety of {@link LazyFilteredBTreeReader} under concurrent access with
     * aggressive cache eviction. Uses small block size (4KB) to create many cache blocks per file,
     * tiny cache (64KB) to force frequent evictions, 20 index files sharing one CacheManager, and
     * 16 concurrent threads each running 15 random queries of all types. This exercises:
     *
     * <ul>
     *   <li>ConcurrentHashMap reader cache in LazyFilteredBTreeReader
     *   <li>BlockCache.getBlock() check-then-act race under eviction pressure
     *   <li>CacheManager eviction callbacks racing with concurrent reads
     *   <li>BTreeIndexReader readLock contention across query types
     *   <li>LazyField initialization race for null bitmaps
     *   <li>SegmentContainer.accessCount non-atomic increment under contention
     * </ul>
     */
    @TestTemplate
    public void testConcurrentAccess() throws Exception {
        // Small block size → many blocks per file → more cache entries
        // Tiny cache → aggressive eviction under concurrent load
        Options stressOptions = new Options();
        stressOptions.set(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE, MemorySize.ofKibiBytes(4));
        stressOptions.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofKibiBytes(64));
        stressOptions.set(BTreeIndexOptions.BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO, 0.1);
        BTreeGlobalIndexer stressIndexer =
                new BTreeGlobalIndexer(new DataField(1, "testField", dataType), stressOptions);

        // Inject null values at the tail to test isNull/isNotNull under concurrency
        for (int i = dataNum - 1; i >= dataNum * 0.9; i--) {
            data.get(i).setLeft(null);
        }

        // Split into 20 files → 20 BTreeIndexReader instances sharing one CacheManager
        int fileNum = 20;
        List<GlobalIndexIOMeta> written = new ArrayList<>(fileNum);
        int currentStart = 0;
        int recordPerFile = dataNum / fileNum;
        while (currentStart < dataNum) {
            int nextStart = Math.min(currentStart + recordPerFile, dataNum);
            written.add(writeDataWithIndexer(stressIndexer, data.subList(currentStart, nextStart)));
            currentStart = nextStart;
        }

        // Real multi-threaded executor for the reader's internal file-level parallelism
        ExecutorService readerExecutor = Executors.newFixedThreadPool(8);
        try (GlobalIndexReader reader =
                stressIndexer.createReader(fileReader, written, readerExecutor)) {
            FieldRef ref = new FieldRef(1, "testField", dataType);

            int concurrency = 16;
            int queriesPerThread = 15;
            CyclicBarrier barrier = new CyclicBarrier(concurrency);
            ExecutorService testExecutor = Executors.newFixedThreadPool(concurrency);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < concurrency; t++) {
                int threadId = t;
                futures.add(
                        testExecutor.submit(
                                () -> {
                                    try {
                                        barrier.await();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    Random random = new Random(threadId);
                                    for (int i = 0; i < queriesPerThread; i++) {
                                        runRandomQuery(ref, random, reader);
                                    }
                                }));
            }

            testExecutor.shutdown();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        } finally {
            readerExecutor.shutdown();
        }
    }

    private GlobalIndexIOMeta writeDataWithIndexer(
            BTreeGlobalIndexer indexer, List<Pair<Object, Long>> subData) throws IOException {
        GlobalIndexParallelWriter indexWriter = indexer.createWriter(fileWriter);
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

    /**
     * Regression test for deadlock when using {@link SemaphoredDelegatingExecutor}. Before the fix,
     * {@code LazyFilteredBTreeReader.visitParallel} submitted tasks to the executor (acquiring
     * permits), and the visitor inside {@code BTreeIndexReader.supplyResult} submitted nested tasks
     * to the same executor (needing more permits), causing deadlock when permits were exhausted.
     */
    @TestTemplate
    public void testNoDeadlockWithSemaphoredExecutor() throws Exception {
        List<GlobalIndexIOMeta> written = writeData();

        // Permit count (2) is smaller than file count (10), which would have triggered
        // the deadlock before the fix: Level 1 tasks hold permits while Level 2 tasks
        // wait for permits from the same pool.
        ExecutorService baseExecutor = Executors.newFixedThreadPool(4);
        ExecutorService semaphoredExecutor =
                new SemaphoredDelegatingExecutor(baseExecutor, 2, false);

        try (GlobalIndexReader reader =
                globalIndexer.createReader(fileReader, written, semaphoredExecutor)) {
            FieldRef ref = new FieldRef(1, "testField", dataType);

            Random random = new Random(42);
            for (int i = 0; i < 10; i++) {
                int idx = random.nextInt(dataNum);
                Object literal = data.get(idx).getKey();

                GlobalIndexResult result = reader.visitEqual(ref, literal).join().get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) == 0));

                result = reader.visitLessThan(ref, literal).join().get();
                assertResult(result, filter(obj -> comparator.compare(obj, literal) < 0));

                List<Object> inLiterals = new ArrayList<>();
                for (int j = 0; j < 5; j++) {
                    inLiterals.add(data.get(random.nextInt(dataNum)).getKey());
                }
                TreeSet<Object> inSet = new TreeSet<>(comparator);
                inSet.addAll(inLiterals);
                result = reader.visitIn(ref, inLiterals).join().get();
                assertResult(result, filter(inSet::contains));
            }
        } finally {
            baseExecutor.shutdown();
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void runRandomQuery(FieldRef ref, Random random, GlobalIndexReader reader) {
        int queryType = random.nextInt(11);
        int idx = random.nextInt(dataNum);
        Object literal = data.get(idx).getKey();
        GlobalIndexResult result;

        switch (queryType) {
            case 0: // equal
                if (literal == null) {
                    return;
                }
                result = reader.visitEqual(ref, literal).join().get();
                assertResult(
                        result,
                        filter(obj -> obj != null && comparator.compare(obj, literal) == 0));
                break;
            case 1: // lessThan
                if (literal == null) {
                    return;
                }
                result = reader.visitLessThan(ref, literal).join().get();
                assertResult(
                        result, filter(obj -> obj != null && comparator.compare(obj, literal) < 0));
                break;
            case 2: // greaterOrEqual
                if (literal == null) {
                    return;
                }
                result = reader.visitGreaterOrEqual(ref, literal).join().get();
                assertResult(
                        result,
                        filter(obj -> obj != null && comparator.compare(obj, literal) >= 0));
                break;
            case 3: // lessOrEqual
                if (literal == null) {
                    return;
                }
                result = reader.visitLessOrEqual(ref, literal).join().get();
                assertResult(
                        result,
                        filter(obj -> obj != null && comparator.compare(obj, literal) <= 0));
                break;
            case 4: // greaterThan
                if (literal == null) {
                    return;
                }
                result = reader.visitGreaterThan(ref, literal).join().get();
                assertResult(
                        result, filter(obj -> obj != null && comparator.compare(obj, literal) > 0));
                break;
            case 5: // notEqual
                if (literal == null) {
                    return;
                }
                result = reader.visitNotEqual(ref, literal).join().get();
                assertResult(
                        result,
                        filter(obj -> obj != null && comparator.compare(obj, literal) != 0));
                break;
            case 6: // between
                if (literal == null) {
                    return;
                }
                int toIdx = Math.min(idx + random.nextInt(200) + 1, dataNum - 1);
                Object toLiteral = data.get(toIdx).getKey();
                if (toLiteral == null) {
                    return;
                }
                result = reader.visitBetween(ref, literal, toLiteral).join().get();
                assertResult(
                        result,
                        filter(
                                obj ->
                                        obj != null
                                                && comparator.compare(obj, toLiteral) <= 0
                                                && comparator.compare(obj, literal) >= 0));
                break;
            case 7: // in — many sub-queries hitting cache concurrently
                {
                    List<Object> inLiterals = new ArrayList<>();
                    for (int j = 0; j < 20; j++) {
                        Object l = data.get(random.nextInt(dataNum)).getKey();
                        if (l != null) {
                            inLiterals.add(l);
                        }
                    }
                    if (inLiterals.isEmpty()) {
                        return;
                    }
                    TreeSet<Object> inSet = new TreeSet<>(comparator);
                    inSet.addAll(inLiterals);
                    result = reader.visitIn(ref, inLiterals).join().get();
                    assertResult(result, filter(obj -> obj != null && inSet.contains(obj)));
                    break;
                }
            case 8: // notIn
                {
                    List<Object> notInLiterals = new ArrayList<>();
                    for (int j = 0; j < 20; j++) {
                        Object l = data.get(random.nextInt(dataNum)).getKey();
                        if (l != null) {
                            notInLiterals.add(l);
                        }
                    }
                    if (notInLiterals.isEmpty()) {
                        return;
                    }
                    TreeSet<Object> notInSet = new TreeSet<>(comparator);
                    notInSet.addAll(notInLiterals);
                    result = reader.visitNotIn(ref, notInLiterals).join().get();
                    assertResult(result, filter(obj -> obj != null && !notInSet.contains(obj)));
                    break;
                }
            case 9: // isNull — tests LazyField<nullBitmap> concurrent init
                result = reader.visitIsNull(ref).join().get();
                assertResult(result, filter(Objects::isNull));
                break;
            case 10: // isNotNull
                result = reader.visitIsNotNull(ref).join().get();
                assertResult(result, filter(Objects::nonNull));
                break;
            default:
                break;
        }
    }
}
