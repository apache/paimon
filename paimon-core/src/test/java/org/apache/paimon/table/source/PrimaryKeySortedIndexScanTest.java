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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.globalindex.bitmap.MultiValueGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pksorted.PkSortedBucketIndexState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests source-backed sorted-index planning in file-local row-position space. */
class PrimaryKeySortedIndexScanTest {

    @Test
    void testStartsIndependentGroupsBeforeWaitingForResults() throws Exception {
        DataSplit firstSplit = dataSplit(11, 0, dataFile("data-1", 4));
        DataSplit secondSplit = dataSplit(11, 1, dataFile("data-2", 4));
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Arrays.asList(firstSplit, secondSplit),
                        Collections.singletonList(definition),
                        Arrays.asList(
                                payloadEntry(0, payload("btree-0", "data-1", 4, "btree", 7, 4)),
                                payloadEntry(1, payload("btree-1", "data-2", 4, "btree", 7, 4))));
        assertThat(plan.files()).hasSize(2);
        assertThat(plan.files()).allSatisfy(file -> assertThat(file.group(7)).isPresent());

        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        CompletableFuture<Optional<GlobalIndexResult>> firstResult = new CompletableFuture<>();
        CompletableFuture<Optional<GlobalIndexResult>> secondResult = new CompletableFuture<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        GlobalIndexReader firstReader = mock(GlobalIndexReader.class);
        when(firstReader.visitEqual(any(), eq(42)))
                .thenAnswer(
                        ignored -> {
                            firstStarted.countDown();
                            return firstResult;
                        });
        GlobalIndexReader secondReader = mock(GlobalIndexReader.class);
        when(secondReader.visitEqual(any(), eq(42)))
                .thenAnswer(
                        ignored -> {
                            secondStarted.countDown();
                            return secondResult;
                        });
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<PrimaryKeySortedIndexScan.EvaluatedPlan> evaluated =
                    executor.submit(
                            () ->
                                    PrimaryKeySortedIndexScan.evaluate(
                                            plan,
                                            rowType,
                                            predicate,
                                            Collections.singletonList(definition),
                                            (file,
                                                    ignoredDefinition,
                                                    ignoredPayloads,
                                                    ignoredTotalRowCount) ->
                                                    file.dataFile().fileName().equals("data-1")
                                                            ? firstReader
                                                            : secondReader));

            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            boolean secondStartedBeforeFirstCompleted = secondStarted.await(1, TimeUnit.SECONDS);
            firstResult.complete(Optional.of(GlobalIndexResult.createEmpty()));
            assertThat(secondStarted.await(5, TimeUnit.SECONDS))
                    .as("the second index group should eventually start")
                    .isTrue();
            secondResult.complete(Optional.of(GlobalIndexResult.createEmpty()));
            assertThat(evaluated.get(5, TimeUnit.SECONDS).files()).hasSize(2);
            verify(firstReader).close();
            verify(secondReader).close();

            assertThat(secondStartedBeforeFirstCompleted)
                    .as("the second index group should start before the first result completes")
                    .isTrue();
        } finally {
            firstResult.completeExceptionally(new RuntimeException("Test cleanup."));
            secondResult.completeExceptionally(new RuntimeException("Test cleanup."));
            executor.shutdownNow();
        }
    }

    @Test
    void testPayloadStateIsBuiltOncePerBucketAndDefinition() {
        DataSplit split =
                dataSplit(
                        11, 0, dataFile("data-1", 4), dataFile("data-2", 4), dataFile("data-3", 4));
        List<PrimaryKeyIndexDefinition> definitions =
                Arrays.asList(
                        definition(
                                7,
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                PrimaryKeyIndexDefinition.Family.BTREE),
                        definition(
                                8,
                                BitmapGlobalIndexerFactory.IDENTIFIER,
                                PrimaryKeyIndexDefinition.Family.BITMAP));
        List<PrimaryKeyIndexSourceFile> sources =
                Arrays.asList(
                        new PrimaryKeyIndexSourceFile("data-1", 4),
                        new PrimaryKeyIndexSourceFile("data-2", 4),
                        new PrimaryKeyIndexSourceFile("data-3", 4));
        List<IndexManifestEntry> entries =
                Arrays.asList(
                        payloadEntry(0, payload("btree-level", sources, "btree", 7, 12)),
                        payloadEntry(0, payload("bitmap-level", sources, "bitmap", 8, 12)));

        try (MockedStatic<PkSortedBucketIndexState> states =
                mockStatic(
                        PkSortedBucketIndexState.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
            PrimaryKeySortedIndexScan.Plan plan =
                    PrimaryKeySortedIndexScan.plan(
                            11, Collections.singletonList(split), definitions, entries);

            assertThat(plan.files()).hasSize(3);
            assertThat(plan.files())
                    .allSatisfy(
                            file -> {
                                assertThat(file.group(7)).isPresent();
                                assertThat(file.group(8)).isPresent();
                            });
            states.verify(
                    times(1),
                    () ->
                            PkSortedBucketIndexState.fromActiveDataFiles(
                                    eq(7), eq("btree"), anyList(), anyList()));
            states.verify(
                    times(1),
                    () ->
                            PkSortedBucketIndexState.fromActiveDataFiles(
                                    eq(8), eq("bitmap"), anyList(), anyList()));
        }
    }

    @Test
    void testSnapshotScopedGroupPlanning() {
        DataSplit split = dataSplit(11, 0, dataFile("data-1", 4));
        List<PrimaryKeyIndexDefinition> definitions =
                Arrays.asList(
                        definition(
                                7,
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                PrimaryKeyIndexDefinition.Family.BTREE),
                        definition(
                                8,
                                BitmapGlobalIndexerFactory.IDENTIFIER,
                                PrimaryKeyIndexDefinition.Family.BITMAP));
        List<IndexManifestEntry> entries =
                Arrays.asList(
                        payloadEntry(0, payload("btree", "data-1", 4, "btree", 7, 4)),
                        payloadEntry(1, payload("wrong-bucket", "data-1", 4, "btree", 7, 4)),
                        payloadEntry(0, payload("wrong-field", "data-1", 4, "btree", 9, 4)),
                        payloadEntry(0, payload("wrong-type", "data-1", 4, "bitmap", 7, 4)),
                        payloadEntry(0, payload("wrong-source", "data-2", 4, "bitmap", 8, 4)),
                        payloadEntry(0, ordinaryPayload("ordinary", "btree", 7, 4)));

        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11, Collections.singletonList(split), definitions, entries);

        assertThat(plan.snapshotId()).isEqualTo(11);
        assertThat(plan.files()).hasSize(1);
        PrimaryKeySortedIndexScan.FilePlan file = plan.files().get(0);
        assertThat(file.dataFile().fileName()).isEqualTo("data-1");
        assertThat(file.group(7)).isPresent();
        assertThat(file.group(7).get().payloads())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("btree");
        assertThat(file.group(8)).isEmpty();
    }

    @Test
    void testDuplicateLevelPayloadsFallBackWithoutCreatingReader() {
        DataSplit split = dataSplit(11, 0, dataFile("data-1", 4));
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Arrays.asList(
                                payloadEntry(0, payload("btree-0", "data-1", 4, "btree", 7, 2)),
                                payloadEntry(0, payload("btree-1", "data-1", 4, "btree", 7, 2))));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        RoaringNavigableMap64 positions = new RoaringNavigableMap64();
        positions.add(3);
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), eq(42)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(GlobalIndexResult.create(positions))));
        AtomicInteger readersCreated = new AtomicInteger();

        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, payloads, ignoredTotalRowCount) -> {
                            readersCreated.incrementAndGet();
                            return reader;
                        });

        assertThat(readersCreated).hasValue(0);
        assertThat(evaluated.files()).hasSize(1);
        assertThat(evaluated.files().get(0).result()).isEmpty();
    }

    @Test
    void testReadMergedSourceGroupInFileLocalPositions() throws IOException {
        DataFileMeta first = dataFile("data-1", 2);
        DataFileMeta second = dataFile("data-2", 3);
        DataFileMeta third = dataFile("data-3", 4);
        DataSplit split = dataSplit(11, 0, true, third, first, second);
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.BTREE);
        IndexFileMeta mergedPayload =
                payload(
                        "btree-merged",
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 2),
                                new PrimaryKeyIndexSourceFile("data-2", 3),
                                new PrimaryKeyIndexSourceFile("data-3", 4)),
                        "btree",
                        7,
                        9);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(payloadEntry(0, mergedPayload)));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        AtomicInteger readersCreated = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        CountingRoaringNavigableMap64 groupPositions = new CountingRoaringNavigableMap64();
        groupPositions.add(1);
        groupPositions.add(2);
        groupPositions.add(4);
        groupPositions.add(6);
        groupPositions.add(7);
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), eq(42)))
                .thenAnswer(
                        ignored -> {
                            queries.incrementAndGet();
                            return completedResult(groupPositions);
                        });

        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, payloads, totalRowCount) -> {
                            readersCreated.incrementAndGet();
                            assertThat(payloads).containsExactly(mergedPayload);
                            assertThat(totalRowCount).isEqualTo(9);
                            return reader;
                        });
        PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);

        assertThat(readersCreated).hasValue(1);
        assertThat(queries).hasValue(1);
        assertThat(groupPositions.iteratedPositions()).isEqualTo(5);
        verify(reader, times(1)).close();
        assertThat(result.splits()).hasSize(3);
        assertThat(result.splits()).allMatch(IndexedSplit.class::isInstance);
        IndexedSplit thirdSplit = (IndexedSplit) result.splits().get(0);
        assertThat(thirdSplit.dataSplit().dataFiles()).containsExactly(third);
        assertThat(thirdSplit.rowRanges()).containsExactly(new Range(1, 2));
        IndexedSplit firstSplit = (IndexedSplit) result.splits().get(1);
        assertThat(firstSplit.dataSplit().dataFiles()).containsExactly(first);
        assertThat(firstSplit.rowRanges()).containsExactly(new Range(1, 1));
        IndexedSplit secondSplit = (IndexedSplit) result.splits().get(2);
        assertThat(secondSplit.dataSplit().dataFiles()).containsExactly(second);
        assertThat(secondSplit.rowRanges()).containsExactly(new Range(0, 0), new Range(2, 2));
    }

    @Test
    void testArrayContainsIsCachedAndLocalized() throws IOException {
        DataFileMeta first = dataFile("data-1", 2);
        DataFileMeta second = dataFile("data-2", 3);
        DataSplit split = dataSplit(11, 0, first, second);
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        MultiValueGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.MULTI_VALUE);
        IndexFileMeta mergedPayload =
                payload(
                        "multivalue-merged",
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 2),
                                new PrimaryKeyIndexSourceFile("data-2", 3)),
                        "multivalue",
                        7,
                        5);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(payloadEntry(0, mergedPayload)));
        RowType rowType = RowType.of(new DataField(7, "tags", DataTypes.ARRAY(DataTypes.INT())));
        Predicate predicate = new PredicateBuilder(rowType).arrayContains(0, 42);
        AtomicInteger queries = new AtomicInteger();
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitArrayContains(any(), eq(42)))
                .thenAnswer(
                        ignored -> {
                            queries.incrementAndGet();
                            return completedResult(1, 2, 4);
                        });

        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, payloads, totalRowCount) -> {
                            assertThat(payloads).containsExactly(mergedPayload);
                            assertThat(totalRowCount).isEqualTo(5);
                            return reader;
                        });

        assertThat(queries).hasValue(1);
        assertThat(evaluated.files()).hasSize(2);
        assertThat(evaluated.files().get(0).result()).isPresent();
        assertThat(evaluated.files().get(0).result().get().results()).containsExactly(1L);
        assertThat(evaluated.files().get(1).result()).isPresent();
        assertThat(evaluated.files().get(1).result().get().results()).containsExactly(0L, 2L);
        verify(reader).close();
    }

    @Test
    void testArraySetPredicatesAreCachedAndLocalized() throws IOException {
        DataFileMeta first = dataFile("data-1", 2);
        DataFileMeta second = dataFile("data-2", 3);
        DataSplit split = dataSplit(11, 0, first, second);
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        MultiValueGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.MULTI_VALUE);
        IndexFileMeta mergedPayload =
                payload(
                        "multivalue-merged",
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 2),
                                new PrimaryKeyIndexSourceFile("data-2", 3)),
                        "multivalue",
                        7,
                        5);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(payloadEntry(0, mergedPayload)));
        RowType rowType = RowType.of(new DataField(7, "tags", DataTypes.ARRAY(DataTypes.INT())));
        List<Object> literals = Arrays.asList(42, 43);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(
                        builder.arraysOverlap(0, literals), builder.arrayContainsAll(0, literals));
        AtomicInteger queries = new AtomicInteger();
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitArraysOverlap(any(), eq(literals)))
                .thenAnswer(
                        ignored -> {
                            queries.incrementAndGet();
                            return completedResult(1, 2, 4);
                        });
        when(reader.visitArrayContainsAll(any(), eq(literals)))
                .thenAnswer(
                        ignored -> {
                            queries.incrementAndGet();
                            return completedResult(1, 4);
                        });

        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, payloads, totalRowCount) -> {
                            assertThat(payloads).containsExactly(mergedPayload);
                            assertThat(totalRowCount).isEqualTo(5);
                            return reader;
                        });

        assertThat(queries).hasValue(2);
        assertThat(evaluated.files()).hasSize(2);
        assertThat(evaluated.files().get(0).result()).isPresent();
        assertThat(evaluated.files().get(0).result().get().results()).containsExactly(1L);
        assertThat(evaluated.files().get(1).result()).isPresent();
        assertThat(evaluated.files().get(1).result().get().results()).containsExactly(2L);
        verify(reader).close();
    }

    @Test
    void testPerFileBooleanFallbackSemantics() {
        DataSplit split = dataSplit(11, 0, dataFile("data-1", 4));
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(
                                payloadEntry(0, payload("btree-0", "data-1", 4, "btree", 7, 4))));
        RowType rowType =
                RowType.of(
                        new DataField(7, "f7", DataTypes.INT()),
                        new DataField(8, "f8", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(rowType);
        GlobalIndexReader reader = readerWithPositions(2);

        PrimaryKeySortedIndexScan.EvaluatedPlan andResult =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        PredicateBuilder.and(builder.equal(0, 42), builder.equal(1, 99)),
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, ignoredPayloads, ignoredTotalRowCount) ->
                                reader);
        PrimaryKeySortedIndexScan.EvaluatedPlan orResult =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        PredicateBuilder.or(builder.equal(0, 42), builder.equal(1, 99)),
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, ignoredPayloads, ignoredTotalRowCount) ->
                                reader);

        assertThat(andResult.files().get(0).result()).isPresent();
        assertThat(andResult.files().get(0).result().get().results()).containsExactly(2L);
        assertThat(orResult.files().get(0).result()).isEmpty();
    }

    @Test
    void testReaderFailureFallsBackForCompleteLevel() {
        DataSplit split = dataSplit(11, 0, dataFile("data-1", 4), dataFile("data-2", 4));
        PrimaryKeyIndexDefinition definition =
                definition(
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        PrimaryKeyIndexDefinition.Family.BTREE);
        IndexFileMeta mergedPayload =
                payload(
                        "btree-level",
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 4),
                                new PrimaryKeyIndexSourceFile("data-2", 4)),
                        "btree",
                        7,
                        8);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(payloadEntry(0, mergedPayload)));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        GlobalIndexReader failedReader = mock(GlobalIndexReader.class);
        when(failedReader.visitEqual(any(), eq(42)))
                .thenThrow(new RuntimeException("corrupt index"));

        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (file, ignoredDefinition, ignoredPayloads, ignoredTotalRowCount) ->
                                failedReader);

        assertThat(evaluated.files()).hasSize(2);
        assertThat(evaluated.files().get(0).result()).isEmpty();
        assertThat(evaluated.files().get(1).result()).isEmpty();
    }

    private static PrimaryKeyIndexDefinition definition(
            int fieldId, String indexType, PrimaryKeyIndexDefinition.Family family) {
        return new PrimaryKeyIndexDefinition(
                "f" + fieldId, fieldId, indexType, new Options(), family);
    }

    private static GlobalIndexReader readerWithPositions(long... rowPositions) {
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), any())).thenReturn(completedResult(rowPositions));
        return reader;
    }

    private static CompletableFuture<Optional<GlobalIndexResult>> completedResult(
            long... rowPositions) {
        RoaringNavigableMap64 positions = new RoaringNavigableMap64();
        for (long rowPosition : rowPositions) {
            positions.add(rowPosition);
        }
        return completedResult(positions);
    }

    private static CompletableFuture<Optional<GlobalIndexResult>> completedResult(
            RoaringNavigableMap64 positions) {
        return CompletableFuture.completedFuture(Optional.of(GlobalIndexResult.create(positions)));
    }

    private static class CountingRoaringNavigableMap64 extends RoaringNavigableMap64 {

        private final AtomicInteger iteratedPositions = new AtomicInteger();

        @Override
        public Iterator<Long> iterator() {
            Iterator<Long> wrapped = super.iterator();
            return new Iterator<Long>() {
                @Override
                public boolean hasNext() {
                    return wrapped.hasNext();
                }

                @Override
                public Long next() {
                    iteratedPositions.incrementAndGet();
                    return wrapped.next();
                }
            };
        }

        int iteratedPositions() {
            return iteratedPositions.get();
        }
    }

    private static DataSplit dataSplit(long snapshotId, int bucket, DataFileMeta... files) {
        return dataSplit(snapshotId, bucket, false, files);
    }

    private static DataSplit dataSplit(
            long snapshotId, int bucket, boolean rawConvertible, DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(bucket)
                .withBucketPath("bucket-" + bucket)
                .withTotalBuckets(2)
                .withDataFiles(Arrays.asList(files))
                .isStreaming(false)
                .rawConvertible(rawConvertible)
                .build();
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        rowCount,
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
                        null)
                .upgrade(1);
    }

    private static IndexManifestEntry payloadEntry(int bucket, IndexFileMeta payload) {
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, bucket, payload);
    }

    private static IndexFileMeta payload(
            String fileName,
            String sourceName,
            long sourceRowCount,
            String indexType,
            int fieldId,
            long payloadRowCount) {
        return payload(
                fileName,
                Collections.singletonList(
                        new PrimaryKeyIndexSourceFile(sourceName, sourceRowCount)),
                indexType,
                fieldId,
                payloadRowCount);
    }

    private static IndexFileMeta payload(
            String fileName,
            List<PrimaryKeyIndexSourceFile> sourceFiles,
            String indexType,
            int fieldId,
            long payloadRowCount) {
        byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(1, sourceFiles).serialize();
        long sourceRowCount = 0;
        for (PrimaryKeyIndexSourceFile sourceFile : sourceFiles) {
            sourceRowCount = Math.addExact(sourceRowCount, sourceFile.rowCount());
        }
        return new IndexFileMeta(
                indexType,
                fileName,
                100,
                payloadRowCount,
                new GlobalIndexMeta(
                        0, sourceRowCount - 1, fieldId, null, new byte[] {1}, sourceMeta),
                null);
    }

    private static IndexFileMeta ordinaryPayload(
            String fileName, String indexType, int fieldId, long rowCount) {
        return new IndexFileMeta(
                indexType,
                fileName,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, fieldId, null, new byte[] {1}, null),
                null);
    }
}
