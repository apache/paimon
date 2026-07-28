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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyFullTextSearchSplit;
import org.apache.paimon.table.source.PrimaryKeySearchPosition;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Tests payload-local full-text search and cross-payload score merging. */
class PrimaryKeyFullTextBucketSearchTest {

    @Test
    void testFiltersDeletedRowsAndSelectsGlobalScores() {
        PrimaryKeyFullTextSearchSplit split = split();
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);
        Map<String, DeletionVector> deletionVectors = Collections.singletonMap("a", deletionVector);
        Map<String, AtomicInteger> closes = new HashMap<>();

        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        payload -> {
                            String source =
                                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                            .sourceFile()
                                            .fileName();
                            closes.put(source, new AtomicInteger());
                            if (source.equals("a")) {
                                return reader(scores(0, 10F, 1, 9F, 2, 8F), closes.get(source));
                            }
                            return reader(scores(0, 4F), closes.get(source));
                        });

        List<PrimaryKeySearchPosition> result =
                search.search(split, deletionVectors, "content", "hello", 3);

        assertThat(result)
                .extracting(
                        PrimaryKeySearchPosition::dataFileName,
                        PrimaryKeySearchPosition::rowPosition)
                .containsExactly(tuple("a", 1L), tuple("a", 2L), tuple("b", 0L));
        assertThat(result).extracting(PrimaryKeySearchPosition::score).containsExactly(9F, 8F, 4F);
        assertThat(closes.get("a")).hasValue(1);
        assertThat(closes.get("b")).hasValue(1);
    }

    @Test
    void testOrdersEachPayloadByScore() {
        AtomicInteger closes = new AtomicInteger();
        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        payload -> reader(scores(0, 1F, 2, 10F), closes));
        PrimaryKeyFullTextSearchSplit split =
                new PrimaryKeyFullTextSearchSplit(
                        dataSplit(Collections.singletonList(dataFile("a"))),
                        Collections.singletonList(payload("a", "index-a")),
                        Collections.emptyList());

        List<List<PrimaryKeySearchPosition>> rankings =
                search.searchRankings(split, Collections.emptyMap(), "content", "hello", 2);

        assertThat(rankings).hasSize(1);
        assertThat(rankings.get(0))
                .extracting(PrimaryKeySearchPosition::rowPosition, PrimaryKeySearchPosition::score)
                .containsExactly(tuple(2L, 10F), tuple(0L, 1F));
        assertThat(closes).hasValue(1);
    }

    @Test
    void testMapsMultiSourceRowsAndShiftsDeletionVectors() {
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(1);
        AtomicInteger closes = new AtomicInteger();
        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        ignored -> reader(scores(0, 10F, 3, 9F, 4, 8F, 5, 7F), closes));
        PrimaryKeyFullTextSearchSplit split =
                new PrimaryKeyFullTextSearchSplit(
                        dataSplit(Arrays.asList(dataFile("a"), dataFile("b"))),
                        Collections.singletonList(payload(Arrays.asList("a", "b"), "index-ab")),
                        Collections.emptyList());

        List<List<PrimaryKeySearchPosition>> rankings =
                search.searchRankings(
                        split,
                        Collections.singletonMap("b", deletionVector),
                        "content",
                        "hello",
                        4);

        assertThat(rankings).hasSize(1);
        assertThat(rankings.get(0))
                .extracting(
                        PrimaryKeySearchPosition::dataFileName,
                        PrimaryKeySearchPosition::rowPosition,
                        PrimaryKeySearchPosition::score)
                .containsExactly(tuple("a", 0L, 10F), tuple("b", 0L, 9F), tuple("b", 2L, 7F));
        assertThat(closes).hasValue(1);
    }

    @Test
    void testBuildsDeletionVectorIncludeWithoutEnumeratingLiveRows() {
        long rowCount = 1_000_000_000L;
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(1);
        AtomicInteger closes = new AtomicInteger();
        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        ignored -> reader(Collections.emptyMap(), closes));
        PrimaryKeyFullTextSearchSplit split =
                new PrimaryKeyFullTextSearchSplit(
                        dataSplit(Collections.singletonList(dataFile("large", rowCount))),
                        Collections.singletonList(payload("large", "index-large", rowCount)),
                        Collections.emptyList());

        assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () ->
                        search.searchRankings(
                                split,
                                Collections.singletonMap("large", deletionVector),
                                "content",
                                "hello",
                                1));
        assertThat(closes).hasValue(1);
    }

    @Test
    void testClosesReaderAfterFailedSearch() {
        AtomicInteger closes = new AtomicInteger();
        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        payload ->
                                new FullTextOnlyReader() {
                                    @Override
                                    public CompletableFuture<Optional<ScoredGlobalIndexResult>>
                                            visitFullTextSearch(FullTextSearch fullTextSearch) {
                                        CompletableFuture<Optional<ScoredGlobalIndexResult>>
                                                failed = new CompletableFuture<>();
                                        failed.completeExceptionally(new IOException("broken"));
                                        return failed;
                                    }

                                    @Override
                                    public void close() {
                                        closes.incrementAndGet();
                                    }
                                });

        assertThatThrownBy(
                        () ->
                                search.search(
                                        new PrimaryKeyFullTextSearchSplit(
                                                dataSplit(Collections.singletonList(dataFile("a"))),
                                                Collections.singletonList(payload("a", "index-a")),
                                                Collections.emptyList()),
                                        Collections.emptyMap(),
                                        "content",
                                        "hello",
                                        1))
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("broken");
        assertThat(closes).hasValue(1);
    }

    private static GlobalIndexReader reader(Map<Long, Float> scores, AtomicInteger closeCounter) {
        return new FullTextOnlyReader() {
            @Override
            public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
                    FullTextSearch fullTextSearch) {
                RoaringNavigableMap64 rows = new RoaringNavigableMap64();
                RoaringNavigableMap64 include = fullTextSearch.includeRowIds();
                for (Long row : scores.keySet()) {
                    if (include == null || include.contains(row)) {
                        rows.add(row);
                    }
                }
                return CompletableFuture.completedFuture(
                        Optional.of(ScoredGlobalIndexResult.create(rows, scores::get)));
            }

            @Override
            public void close() {
                closeCounter.incrementAndGet();
            }
        };
    }

    private abstract static class FullTextOnlyReader implements GlobalIndexReader {

        private CompletableFuture<Optional<GlobalIndexResult>> emptyResult() {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
                FieldRef fieldRef, Object literal) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
                FieldRef fieldRef, List<Object> literals) {
            return emptyResult();
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
                FieldRef fieldRef, List<Object> literals) {
            return emptyResult();
        }
    }

    private static Map<Long, Float> scores(Object... pairs) {
        Map<Long, Float> scores = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            scores.put(((Number) pairs[i]).longValue(), ((Number) pairs[i + 1]).floatValue());
        }
        return scores;
    }

    private static PrimaryKeyFullTextSearchSplit split() {
        List<DataFileMeta> dataFiles = Arrays.asList(dataFile("a"), dataFile("b"));
        return new PrimaryKeyFullTextSearchSplit(
                dataSplit(dataFiles),
                Arrays.asList(payload("a", "index-a"), payload("b", "index-b")),
                Collections.emptyList());
    }

    private static DataSplit dataSplit(List<DataFileMeta> dataFiles) {
        return DataSplit.builder()
                .withSnapshot(11)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withTotalBuckets(1)
                .withDataFiles(dataFiles)
                .build();
    }

    private static DataFileMeta dataFile(String name) {
        return dataFile(name, 3);
    }

    private static DataFileMeta dataFile(String name, long rowCount) {
        return DataFileMeta.create(
                name,
                100,
                rowCount,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                1,
                Collections.emptyList(),
                0L,
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }

    private static IndexFileMeta payload(String source, String name) {
        return payload(Collections.singletonList(source), name);
    }

    private static IndexFileMeta payload(String source, String name, long rowCount) {
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(1, new PrimaryKeyIndexSourceFile(source, rowCount))
                        .serialize();
        return new IndexFileMeta(
                "full-text",
                name,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, 7, null, null, sourceMeta),
                null);
    }

    private static IndexFileMeta payload(List<String> sources, String name) {
        List<PrimaryKeyIndexSourceFile> sourceFiles = new java.util.ArrayList<>();
        for (String source : sources) {
            sourceFiles.add(new PrimaryKeyIndexSourceFile(source, 3));
        }
        byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(1, sourceFiles).serialize();
        long rowCount = 3L * sourceFiles.size();
        return new IndexFileMeta(
                "full-text",
                name,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, 7, null, null, sourceMeta),
                null);
    }
}
