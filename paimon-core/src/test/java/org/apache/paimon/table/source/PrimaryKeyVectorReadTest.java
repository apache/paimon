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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests global candidate merging for primary-key vector search. */
class PrimaryKeyVectorReadTest {

    @Test
    void testSnapshotParametersSurviveSerializationAndBoundGlobalCandidates() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("global-index.future-vector.index-columns", "vector");
        options.put("future-vector.refine_factor", "invalid");
        options.put("test-vector-ann.refine_factor", "3");
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        PrimaryKeyVectorRead read =
                new PrimaryKeyVectorRead(
                        table,
                        new DataField(1, "vector", new VectorType(2, new FloatType())),
                        new float[] {0, 0},
                        1,
                        Collections.emptyMap());
        BucketVectorSearchSplit indexed = mock(BucketVectorSearchSplit.class);
        DataFileMeta dataFile = mock(DataFileMeta.class);
        when(dataFile.fileName()).thenReturn("data");
        when(dataFile.rowCount()).thenReturn(2L);
        when(dataFile.level()).thenReturn(1);
        when(dataFile.fileSource()).thenReturn(Optional.of(FileSource.COMPACT));
        DataSplit dataSplit = mock(DataSplit.class);
        when(dataSplit.dataFiles()).thenReturn(Collections.singletonList(dataFile));
        when(indexed.dataSplit()).thenReturn(dataSplit);
        IndexFileMeta payload = payload("test-vector-ann", "data");
        IndexFileMeta stale = payload("obsolete-vector", "removed");
        when(indexed.payloadFiles()).thenReturn(Arrays.asList(payload, stale));
        BucketVectorSearchSplit raw = mock(BucketVectorSearchSplit.class);

        PrimaryKeyVectorRead.SearchParameters parameters =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(
                                read.searchParameters(Arrays.asList(indexed, raw))),
                        getClass().getClassLoader());
        PrimaryKeyVectorRead.SearchResult result =
                read.mergeSearchResults(
                        Collections.singletonList(
                                new PrimaryKeyVectorRead.SearchResult(
                                        Arrays.asList(
                                                candidate(0, "data", 0, 1F),
                                                candidate(1, "data", 0, 2F),
                                                candidate(2, "data", 0, 3F),
                                                candidate(3, "data", 0, 4F)),
                                        Collections.emptyList())),
                        parameters);
        assertThat(result.indexedCandidates())
                .extracting(PrimaryKeyVectorRead.Candidate::bucket)
                .containsExactly(0, 1, 2);
        // Configuration is used only when the entire plan has no index payloads.
        assertThatThrownBy(() -> read.searchParameters(Collections.singletonList(raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid vector refine factor");
        when(indexed.payloadFiles()).thenReturn(Collections.singletonList(stale));
        assertThatThrownBy(() -> read.searchParameters(Collections.singletonList(indexed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid vector refine factor");
        when(indexed.payloadFiles())
                .thenReturn(Arrays.asList(payload, payload("another-vector", "data")));
        assertThatThrownBy(() -> read.searchParameters(Collections.singletonList(indexed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot mix index types");
    }

    @Test
    void testStartsBucketsBeforeWaitingForResults() throws Exception {
        BucketVectorSearchSplit split1 = mock(BucketVectorSearchSplit.class);
        BucketVectorSearchSplit split2 = mock(BucketVectorSearchSplit.class);
        CompletableFuture<PrimaryKeyVectorRead.SearchResult> future1 = new CompletableFuture<>();
        CompletableFuture<PrimaryKeyVectorRead.SearchResult> future2 = new CompletableFuture<>();
        CountDownLatch started = new CountDownLatch(2);
        TestingPrimaryKeyVectorRead read =
                new TestingPrimaryKeyVectorRead(split1, future1, split2, future2, started);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<PrimaryKeyVectorRead.SearchResult> resultFuture =
                    executor.submit(
                            () ->
                                    read.searchBuckets(
                                            Arrays.asList(split1, split2),
                                            read.searchParameters(Arrays.asList(split1, split2))));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            future1.complete(
                    new PrimaryKeyVectorRead.SearchResult(
                            Collections.singletonList(candidate(0, "data-1", 0, 2F)),
                            Collections.emptyList()));
            future2.complete(
                    new PrimaryKeyVectorRead.SearchResult(
                            Collections.singletonList(candidate(1, "data-2", 0, 1F)),
                            Collections.emptyList()));

            assertThat(resultFuture.get(5, TimeUnit.SECONDS).indexedCandidates())
                    .extracting(PrimaryKeyVectorRead.Candidate::dataFileName)
                    .containsExactly("data-2");
        } finally {
            future1.completeExceptionally(new RuntimeException("Test cleanup."));
            future2.completeExceptionally(new RuntimeException("Test cleanup."));
            executor.shutdownNow();
        }
    }

    @Test
    void testMergesGlobalTopKWithDeterministicTies() {
        List<PrimaryKeyVectorRead.Candidate> candidates =
                Arrays.asList(
                        candidate(1, "file-c", 0, 2F),
                        candidate(1, "file-b", 1, 1F),
                        candidate(0, "file-a", 2, 1F));

        List<PrimaryKeyVectorRead.Candidate> result = PrimaryKeyVectorRead.topK(candidates, 2);

        assertThat(result)
                .extracting(
                        PrimaryKeyVectorRead.Candidate::bucket,
                        PrimaryKeyVectorRead.Candidate::dataFileName,
                        PrimaryKeyVectorRead.Candidate::rowPosition)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(0, "file-a", 2L),
                        org.assertj.core.groups.Tuple.tuple(1, "file-b", 1L));
    }

    private static PrimaryKeyVectorRead.Candidate candidate(
            int bucket, String fileName, long position, float distance) {
        return new PrimaryKeyVectorRead.Candidate(
                BinaryRow.EMPTY_ROW, bucket, fileName, position, distance);
    }

    private static IndexFileMeta payload(String type, String sourceFile) {
        byte[] source =
                new PrimaryKeyIndexSourceMeta(
                                1,
                                Collections.singletonList(
                                        new PrimaryKeyIndexSourceFile(sourceFile, 2)))
                        .serialize();
        return new IndexFileMeta(
                type,
                type + ".index",
                100,
                2,
                new GlobalIndexMeta(0, 1, 1, null, null, source),
                null);
    }

    private static FileStoreTable table() {
        Map<String, String> options = new HashMap<>();
        options.put("fields.vector.pk-vector.index.type", "test-vector-ann");
        options.put("fields.vector.pk-vector.distance.metric", "l2");
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        when(table.options()).thenReturn(options);
        return table;
    }

    private static class TestingPrimaryKeyVectorRead extends PrimaryKeyVectorRead {

        private final BucketVectorSearchSplit split1;
        private final CompletableFuture<SearchResult> future1;
        private final BucketVectorSearchSplit split2;
        private final CompletableFuture<SearchResult> future2;
        private final CountDownLatch started;

        private TestingPrimaryKeyVectorRead(
                BucketVectorSearchSplit split1,
                CompletableFuture<SearchResult> future1,
                BucketVectorSearchSplit split2,
                CompletableFuture<SearchResult> future2,
                CountDownLatch started) {
            super(
                    table(),
                    new DataField(1, "vector", new VectorType(2, new FloatType())),
                    new float[] {0, 0},
                    1,
                    Collections.emptyMap());
            this.split1 = split1;
            this.future1 = future1;
            this.split2 = split2;
            this.future2 = future2;
            this.started = started;
        }

        @Override
        protected SearchContext createSearchContext(SearchParameters parameters) {
            return null;
        }

        @Override
        protected CompletableFuture<SearchResult> searchAsync(
                BucketVectorSearchSplit split, SearchContext context) {
            started.countDown();
            if (split == split1) {
                return future1;
            }
            assertThat(split).isSameAs(split2);
            return future2;
        }
    }
}
