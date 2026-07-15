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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PrimaryKeyVectorBucketSearch}. */
class PrimaryKeyVectorBucketSearchTest {

    @Test
    void testSearchesUncoveredFilesConcurrently() throws Exception {
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        PkVectorDataFileReader reader1 = blockingReader(started, release);
        PkVectorDataFileReader reader2 = blockingReader(started, release);
        when(readerFactory.create(data1)).thenReturn(reader1);
        when(readerFactory.create(data2)).thenReturn(reader2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            CompletableFuture<PrimaryKeyVectorBucketSearch.Result> future =
                    new PrimaryKeyVectorBucketSearch(
                                    readerFactory,
                                    null,
                                    Collections.emptyMap(),
                                    "l2",
                                    GlobalIndexSearchMode.FULL)
                            .searchAsync(
                                    new PkVectorBucketIndexState(
                                            7, "test-vector-ann", Collections.emptyList()),
                                    Arrays.asList(data1, data2),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    new float[] {0, 0},
                                    1,
                                    1,
                                    executor);

            try {
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                release.countDown();
            }
            assertThat(future.get(5, TimeUnit.SECONDS).exactCandidates()).isEmpty();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testStartsAnnSegmentsBeforeWaitingForResults() throws Exception {
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        IndexFileMeta ann1 = segment("ann-1", data1);
        IndexFileMeta ann2 = segment("ann-2", data2);
        PkVectorAnnSegmentSearcher annSearcher = mock(PkVectorAnnSegmentSearcher.class);
        CompletableFuture<List<PkVectorSearchResult>> future1 = new CompletableFuture<>();
        CompletableFuture<List<PkVectorSearchResult>> future2 = new CompletableFuture<>();
        CountDownLatch started = new CountDownLatch(2);
        when(annSearcher.searchAsync(
                        org.mockito.ArgumentMatchers.any(IndexFileMeta.class),
                        org.mockito.ArgumentMatchers.any(PrimaryKeyIndexSourceMeta.class),
                        org.mockito.ArgumentMatchers.any(float[].class),
                        org.mockito.ArgumentMatchers.eq(1),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap()),
                        org.mockito.ArgumentMatchers.eq(
                                new java.util.HashSet<>(Arrays.asList("data-1", "data-2"))),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap()),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap())))
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            return invocation
                                            .<IndexFileMeta>getArgument(0)
                                            .fileName()
                                            .equals("ann-1")
                                    ? future1
                                    : future2;
                        });

        CompletableFuture<PrimaryKeyVectorBucketSearch.Result> resultFuture =
                new PrimaryKeyVectorBucketSearch(
                                mock(PkVectorDataFileReader.Factory.class),
                                annSearcher,
                                Collections.emptyMap(),
                                "l2",
                                GlobalIndexSearchMode.FAST)
                        .searchAsync(
                                new PkVectorBucketIndexState(
                                        7, "test-vector-ann", Arrays.asList(ann1, ann2)),
                                Arrays.asList(data1, data2),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                new float[] {0, 0},
                                1,
                                1,
                                Runnable::run);

        assertThat(started.getCount()).isZero();
        future1.complete(Collections.singletonList(new PkVectorSearchResult("data-1", 0, 2F)));
        future2.complete(Collections.singletonList(new PkVectorSearchResult("data-2", 0, 1F)));

        assertThat(resultFuture.get(5, TimeUnit.SECONDS).indexedCandidates())
                .extracting(PkVectorSearchResult::dataFileName)
                .containsExactly("data-2");
    }

    @Test
    void testFastModeSkipsExactFallback() throws Exception {
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);

        List<PkVectorSearchResult> results =
                new PrimaryKeyVectorBucketSearch(
                                readerFactory,
                                null,
                                Collections.emptyMap(),
                                "l2",
                                GlobalIndexSearchMode.FAST)
                        .search(
                                new PkVectorBucketIndexState(
                                        7, "test-vector-ann", Collections.emptyList()),
                                Collections.singletonList(data),
                                Collections.emptyMap(),
                                new float[] {0, 0},
                                1);

        assertThat(results).isEmpty();
        verify(readerFactory, never()).create(data);
    }

    @Test
    void testRejectsNonPositiveLimitForEmptyBucket() {
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PrimaryKeyVectorBucketSearch search =
                new PrimaryKeyVectorBucketSearch(
                        readerFactory,
                        null,
                        Collections.emptyMap(),
                        "l2",
                        GlobalIndexSearchMode.FULL);

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                search.search(
                                        new PkVectorBucketIndexState(
                                                7, "test-vector-ann", Collections.emptyList()),
                                        Collections.emptyList(),
                                        Collections.emptyMap(),
                                        new float[] {0, 0},
                                        0))
                .withMessageContaining("positive");
    }

    @Test
    void testSeparatesAnnAndExactFallbackWithoutRescanningCoveredFiles() throws Exception {
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        IndexFileMeta ann = segment("ann", data1);
        PkVectorBucketIndexState state =
                new PkVectorBucketIndexState(7, "test-vector-ann", Collections.singletonList(ann));
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader reader1 = reader(new float[][] {{5, 0}, {6, 0}});
        PkVectorDataFileReader reader2 = reader(new float[][] {{1, 0}, {3, 0}});
        when(readerFactory.create(data1)).thenReturn(reader1);
        when(readerFactory.create(data2)).thenReturn(reader2);
        PkVectorAnnSegmentSearcher annSearcher = mock(PkVectorAnnSegmentSearcher.class);
        Map<String, DeletionVector> deletionVectors = Collections.emptyMap();
        Map<String, String> searchOptions = Collections.singletonMap("nprobes", "8");
        when(annSearcher.searchAsync(
                        org.mockito.ArgumentMatchers.eq(ann),
                        org.mockito.ArgumentMatchers.any(PrimaryKeyIndexSourceMeta.class),
                        org.mockito.ArgumentMatchers.any(float[].class),
                        org.mockito.ArgumentMatchers.eq(2),
                        org.mockito.ArgumentMatchers.eq(deletionVectors),
                        org.mockito.ArgumentMatchers.eq(
                                new java.util.HashSet<>(Arrays.asList("data-1", "data-2"))),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap()),
                        org.mockito.ArgumentMatchers.eq(searchOptions)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Collections.singletonList(
                                        new PkVectorSearchResult("data-1", 1, 0.5F))));

        PrimaryKeyVectorBucketSearch.Result results =
                new PrimaryKeyVectorBucketSearch(
                                readerFactory,
                                annSearcher,
                                searchOptions,
                                "l2",
                                GlobalIndexSearchMode.FULL)
                        .search(
                                state,
                                Arrays.asList(data1, data2),
                                deletionVectors,
                                new float[] {0, 0},
                                2,
                                2);

        assertThat(results.indexedCandidates())
                .extracting(
                        PkVectorSearchResult::dataFileName,
                        PkVectorSearchResult::rowPosition,
                        PkVectorSearchResult::distance)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("data-1", 1L, 0.5F));
        assertThat(results.exactCandidates())
                .extracting(
                        PkVectorSearchResult::dataFileName,
                        PkVectorSearchResult::rowPosition,
                        PkVectorSearchResult::distance)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("data-2", 0L, 1F),
                        org.assertj.core.groups.Tuple.tuple("data-2", 1L, 9F));
        verify(readerFactory, never()).create(data1);
    }

    @Test
    void testSearchesActivePartOfAnnWithInactiveSource() throws Exception {
        DataFileMeta retired = dataFile("retired");
        DataFileMeta active = dataFile("active");
        IndexFileMeta ann = segment("ann", Arrays.asList(retired, active));
        PkVectorAnnSegmentSearcher annSearcher = mock(PkVectorAnnSegmentSearcher.class);
        Map<String, DeletionVector> deletionVectors = Collections.emptyMap();
        when(annSearcher.searchAsync(
                        org.mockito.ArgumentMatchers.eq(ann),
                        org.mockito.ArgumentMatchers.any(PrimaryKeyIndexSourceMeta.class),
                        org.mockito.ArgumentMatchers.any(float[].class),
                        org.mockito.ArgumentMatchers.eq(1),
                        org.mockito.ArgumentMatchers.eq(deletionVectors),
                        org.mockito.ArgumentMatchers.eq(Collections.singleton("active")),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap()),
                        org.mockito.ArgumentMatchers.eq(Collections.emptyMap())))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Collections.singletonList(
                                        new PkVectorSearchResult("active", 0, 1F))));
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);

        List<PkVectorSearchResult> results =
                new PrimaryKeyVectorBucketSearch(
                                readerFactory,
                                annSearcher,
                                Collections.emptyMap(),
                                "l2",
                                GlobalIndexSearchMode.FULL)
                        .search(
                                new PkVectorBucketIndexState(
                                        7, "test-vector-ann", Collections.singletonList(ann)),
                                Collections.singletonList(active),
                                deletionVectors,
                                new float[] {0, 0},
                                1);

        assertThat(results)
                .extracting(PkVectorSearchResult::dataFileName)
                .containsExactly("active");
        verify(readerFactory, never()).create(active);
    }

    @Test
    void testExactFallbackMergesFilesAndAppliesDeletionVectors() throws Exception {
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader reader1 = reader(new float[][] {{0, 0}, {2, 0}});
        PkVectorDataFileReader reader2 = reader(new float[][] {{1, 0}, null});
        when(readerFactory.create(data1)).thenReturn(reader1);
        when(readerFactory.create(data2)).thenReturn(reader2);
        BitmapDeletionVector data1Deletes = new BitmapDeletionVector();
        data1Deletes.delete(0);
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        deletionVectors.put("data-1", data1Deletes);
        Map<String, List<Range>> rowRangesByFile = new HashMap<>();
        rowRangesByFile.put("data-1", Collections.singletonList(new Range(1, 1)));
        rowRangesByFile.put("data-2", Collections.singletonList(new Range(1, 1)));

        PrimaryKeyVectorBucketSearch.Result results =
                new PrimaryKeyVectorBucketSearch(
                                readerFactory,
                                null,
                                Collections.emptyMap(),
                                "l2",
                                GlobalIndexSearchMode.DETAIL)
                        .search(
                                new PkVectorBucketIndexState(
                                        7, "test-vector-ann", Collections.emptyList()),
                                Arrays.asList(data1, data2),
                                deletionVectors,
                                rowRangesByFile,
                                new float[] {0, 0},
                                2,
                                2);

        assertThat(results.exactCandidates())
                .extracting(
                        PkVectorSearchResult::dataFileName,
                        PkVectorSearchResult::rowPosition,
                        PkVectorSearchResult::distance)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("data-1", 1L, 4F));
    }

    private static PkVectorDataFileReader reader(float[][] vectors) throws IOException {
        PkVectorDataFileReader reader = mock(PkVectorDataFileReader.class);
        when(reader.dimension()).thenReturn(2);
        when(reader.rowCount()).thenReturn((long) vectors.length);
        final int[] position = {0};
        when(reader.readNextVector(org.mockito.ArgumentMatchers.any(float[].class)))
                .thenAnswer(
                        invocation -> {
                            float[] vector = vectors[position[0]++];
                            if (vector == null) {
                                return false;
                            }
                            System.arraycopy(
                                    vector, 0, invocation.getArgument(0), 0, vector.length);
                            return true;
                        });
        return reader;
    }

    private static PkVectorDataFileReader blockingReader(
            CountDownLatch started, CountDownLatch release) throws Exception {
        PkVectorDataFileReader reader = mock(PkVectorDataFileReader.class);
        when(reader.dimension()).thenReturn(2);
        when(reader.rowCount()).thenReturn(2L);
        when(reader.readNextVector(org.mockito.ArgumentMatchers.any(float[].class)))
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "Timed out waiting for concurrent search.");
                            }
                            return false;
                        });
        return reader;
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                2,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }

    private static IndexFileMeta segment(String fileName, DataFileMeta source) {
        return segment(fileName, Collections.singletonList(source));
    }

    private static IndexFileMeta segment(String fileName, List<DataFileMeta> sources) {
        long rowCount = sources.stream().mapToLong(DataFileMeta::rowCount).sum();
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                sources.stream()
                                        .map(
                                                source ->
                                                        new PrimaryKeyIndexSourceFile(
                                                                source.fileName(),
                                                                source.rowCount()))
                                        .collect(java.util.stream.Collectors.toList()))
                        .serialize();
        return new IndexFileMeta(
                "test-vector-ann",
                fileName,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, 7, null, new byte[] {1}, sourceMeta),
                null);
    }
}
