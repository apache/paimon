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

package org.apache.paimon.globalindex;

import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GlobalIndexEvaluator}. */
class GlobalIndexEvaluatorTest {

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static RowType rowType() {
        return new RowType(
                Arrays.asList(
                        new DataField(0, "a", DataTypes.INT()),
                        new DataField(1, "b", DataTypes.INT()),
                        new DataField(2, "c", DataTypes.INT())));
    }

    private static GlobalIndexResult resultOf(long... rowIds) {
        RoaringNavigableMap64 bm = new RoaringNavigableMap64();
        for (long id : rowIds) {
            bm.add(id);
        }
        return GlobalIndexResult.create(bm);
    }

    private static GlobalIndexReader readerReturning(GlobalIndexResult result) {
        return new StubGlobalIndexReader(result);
    }

    @Test
    void testSingleFieldSequential() {
        RowType rowType = rowType();
        GlobalIndexResult expected = resultOf(1, 2, 3);
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Collections.singletonList(readerReturning(expected)));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.equal(0, 42);

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L);
        evaluator.close();
    }

    @Test
    void testAndParallelMultipleFields() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3, 4, 5);
        GlobalIndexResult resultB = resultOf(3, 4, 5, 6, 7);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 42), builder.equal(1, 99));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 3L, 4L, 5L);
        evaluator.close();
    }

    @Test
    void testOrParallelMultipleFields() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2);
        GlobalIndexResult resultB = resultOf(3, 4);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.or(builder.equal(0, 42), builder.equal(1, 99));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L, 4L);
        evaluator.close();
    }

    @Test
    void testOrReturnsEmptyWhenChildUnsupported() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> {
                            if (fieldId == 0) {
                                return Collections.singletonList(readerReturning(resultA));
                            }
                            return Collections.emptyList();
                        });

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.or(builder.equal(0, 42), builder.equal(1, 99));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isEmpty();
        evaluator.close();
    }

    @Test
    void testAndWithEmptyResultShortCircuits() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3);
        GlobalIndexResult resultB = resultOf(10, 11);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 42), builder.equal(1, 99));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertThat(result.get().results().isEmpty()).isTrue();
        evaluator.close();
    }

    @Test
    void testParallelUsesMultipleThreads() {
        executor = Executors.newFixedThreadPool(3);
        RowType rowType = rowType();

        ConcurrentHashMap<String, Boolean> threadNames = new ConcurrentHashMap<>();

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        new StubGlobalIndexReader(resultOf(fieldId, fieldId + 10)) {
                                            @Override
                                            public CompletableFuture<Optional<GlobalIndexResult>>
                                                    visitEqual(FieldRef fieldRef, Object literal) {
                                                return CompletableFuture.supplyAsync(
                                                        () -> {
                                                            threadNames.put(
                                                                    Thread.currentThread()
                                                                            .getName(),
                                                                    true);
                                                            return Optional.ofNullable(result);
                                                        },
                                                        executor);
                                            }
                                        }));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2), builder.equal(2, 3));

        evaluator.evaluate(predicate);

        assertThat(threadNames.size()).isGreaterThan(1);
        evaluator.close();
    }

    @Test
    void testNullExecutorFallsBackToSequential() {
        RowType rowType = rowType();

        AtomicInteger callCount = new AtomicInteger();

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> {
                            callCount.incrementAndGet();
                            return Collections.singletonList(
                                    readerReturning(resultOf(fieldId, fieldId + 10)));
                        });

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertThat(callCount.get()).isEqualTo(2);
        evaluator.close();
    }

    @Test
    void testNestedAndPredicateDoesNotDeadlockWithSmallPool() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3, 4, 5);
        GlobalIndexResult resultB = resultOf(3, 4, 5, 6, 7);
        GlobalIndexResult resultC = resultOf(4, 5, 8, 9);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);
        fieldResults.put(2, resultC);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        // and(a, b, c) builds as and(and(a, b), c) — nested binary tree
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2), builder.equal(2, 3));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        // intersection of {1..5}, {3..7}, {4,5,8,9} -> {4,5}
        assertBitmapContainsExactly(result.get().results(), 4L, 5L);
        evaluator.close();
    }

    @Test
    void testNestedOrPredicateDoesNotDeadlockWithSmallPool() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2);
        GlobalIndexResult resultB = resultOf(3, 4);
        GlobalIndexResult resultC = resultOf(5, 6);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);
        fieldResults.put(2, resultC);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        // or(a, b, c) builds as or(or(a, b), c) — nested binary tree
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 2), builder.equal(2, 3));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        // union of {1,2}, {3,4}, {5,6}
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L, 4L, 5L, 6L);
        evaluator.close();
    }

    @Test
    void testMixedNestedPredicateDoesNotDeadlockWithSmallPool() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3, 4, 5);
        GlobalIndexResult resultB = resultOf(3, 4, 5, 6, 7);
        GlobalIndexResult resultC = resultOf(1, 2, 3, 10, 11);
        // Field c used for second OR child - distinct from field a/b

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);
        fieldResults.put(2, resultC);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        // AND(OR(a, b), OR(a, c)) — mixed nesting, different compound types
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(
                        PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 2)),
                        PredicateBuilder.or(builder.equal(0, 3), builder.equal(2, 4)));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        // OR(a, b) = union({1..5}, {3..7}) = {1..7}
        // OR(a, c) = union({1..5}, {1,2,3,10,11}) = {1,2,3,4,5,10,11}
        // AND = intersection = {1,2,3,4,5}
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L, 4L, 5L);
        evaluator.close();
    }

    @Test
    void testDeepMixedNestedPredicateDoesNotDeadlockWithSmallPool() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3, 4, 5);
        GlobalIndexResult resultB = resultOf(2, 3, 4, 5, 6);
        GlobalIndexResult resultC = resultOf(3, 4, 5, 6, 7);

        ConcurrentHashMap<Integer, GlobalIndexResult> fieldResults = new ConcurrentHashMap<>();
        fieldResults.put(0, resultA);
        fieldResults.put(1, resultB);
        fieldResults.put(2, resultC);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Collections.singletonList(
                                        readerReturning(fieldResults.get(fieldId))));

        // AND(OR(AND(a, b), c), OR(AND(a, c), b)) — deep mixed nesting
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(
                        PredicateBuilder.or(
                                PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2)),
                                builder.equal(2, 3)),
                        PredicateBuilder.or(
                                PredicateBuilder.and(builder.equal(0, 4), builder.equal(2, 5)),
                                builder.equal(1, 6)));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        // OR(AND(a,b), c): AND(a,b)={2,3,4,5}, c={3..7} => union={2,3,4,5,6,7}
        // OR(AND(a,c), b): AND(a,c)={3,4,5}, b={2..6} => union={2,3,4,5,6}
        // top AND: intersection = {2,3,4,5,6}
        assertBitmapContainsExactly(result.get().results(), 2L, 3L, 4L, 5L, 6L);
        evaluator.close();
    }

    @Test
    void testSameFieldPredicatesAccessedConcurrently() {
        executor = Executors.newFixedThreadPool(4);
        RowType rowType = rowType();

        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        GlobalIndexReader concurrencyDetectingReader =
                new StubGlobalIndexReader(resultOf(1, 2, 3, 4, 5)) {
                    @Override
                    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
                            FieldRef fieldRef, Object literal) {
                        return CompletableFuture.supplyAsync(
                                () -> {
                                    int c = concurrency.incrementAndGet();
                                    maxConcurrency.updateAndGet(cur -> Math.max(cur, c));
                                    try {
                                        Thread.sleep(50);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                    concurrency.decrementAndGet();
                                    return Optional.ofNullable(result);
                                },
                                executor);
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Collections.singletonList(concurrencyDetectingReader));

        // AND(a=1, a=2, a=3) — readers dispatch internally, concurrency comes from reader
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 1), builder.equal(0, 2), builder.equal(0, 3));

        evaluator.evaluate(predicate);

        assertThat(maxConcurrency.get()).isGreaterThan(1);
        evaluator.close();
    }

    @Test
    void testMultipleReadersPerFieldCombinedWithAnd() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult readerResult1 = resultOf(1, 2, 3, 4, 5);
        GlobalIndexResult readerResult2 = resultOf(3, 4, 5, 6, 7);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId ->
                                Arrays.asList(
                                        readerReturning(readerResult1),
                                        readerReturning(readerResult2)));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.equal(0, 42);

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        // Multiple readers for same field are combined with AND (intersection)
        assertBitmapContainsExactly(result.get().results(), 3L, 4L, 5L);
        evaluator.close();
    }

    @Test
    void testShorterIndexPaddedToLongestRangeNotDroppedByAnd() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        // Field c (id 2) is an extra column of two multi-column indexes:
        //  - a short index covering rows [0,4] that matches the predicate at {1,3}
        //  - a long index covering rows [0,9] that matches the predicate at {1,3,7,8}
        // The evaluator AND-s both readers for the leaf. Padding the short index over its
        // unindexed tail (5..9) with an all-hit reader keeps the long index's tail matches.
        GlobalIndexReader shortIndexPadded =
                new UnionGlobalIndexReader(
                        Arrays.asList(
                                readerReturning(resultOf(1, 3)),
                                new ConstantGlobalIndexReader(
                                        GlobalIndexResult.fromRange(new Range(5, 9)))));
        GlobalIndexReader longIndex = readerReturning(resultOf(1, 3, 7, 8));

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Arrays.asList(shortIndexPadded, longIndex));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Optional<GlobalIndexResult> result = evaluator.evaluate(builder.equal(2, 42));

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 3L, 7L, 8L);
        evaluator.close();
    }

    @Test
    void testShorterIndexWithoutPaddingDropsTailUnderAnd() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        // Same setup as above but WITHOUT padding the short index: AND drops the tail matches
        // {7,8}, which is exactly the bug the padding prevents.
        GlobalIndexReader shortIndex = readerReturning(resultOf(1, 3));
        GlobalIndexReader longIndex = readerReturning(resultOf(1, 3, 7, 8));

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, fieldId -> Arrays.asList(shortIndex, longIndex));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Optional<GlobalIndexResult> result = evaluator.evaluate(builder.equal(2, 42));

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 3L);
        evaluator.close();
    }

    @Test
    void testNonFieldLeafPredicateDoesNotThrow() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Collections.singletonList(readerReturning(resultA)));

        // Manually build AND(alwaysTrue, a=1) to bypass PredicateBuilder simplification
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                new CompoundPredicate(
                        And.INSTANCE,
                        Arrays.asList(PredicateBuilder.alwaysTrue(), builder.equal(0, 42)));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L);
        evaluator.close();
    }

    @Test
    void testIsNullAndIsNotNullSameFieldIsEmptyNotPruned() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        // A reader where IS NULL matches the null rows {3,7} and IS NOT NULL matches the
        // complementary non-null rows {1,2,4,5}. "a IS NULL AND a IS NOT NULL" must be the
        // intersection (empty). The old prune treated IS NULL as a constraining sibling and
        // dropped IS NOT NULL, wrongly yielding the null rows {3,7}.
        GlobalIndexReader nullAwareReader =
                new StubGlobalIndexReader(null) {
                    @Override
                    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(
                            FieldRef fieldRef) {
                        return CompletableFuture.completedFuture(Optional.of(resultOf(3, 7)));
                    }

                    @Override
                    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(
                            FieldRef fieldRef) {
                        return CompletableFuture.completedFuture(Optional.of(resultOf(1, 2, 4, 5)));
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Collections.singletonList(nullAwareReader));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.isNull(0), builder.isNotNull(0));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertThat(result.get().results().isEmpty()).isTrue();
        evaluator.close();
    }

    @Test
    void testRedundantIsNotNullStillPrunedWithNullRejectingSibling() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();
        AtomicBoolean isNotNullVisited = new AtomicBoolean();

        // "a = 42 AND a IS NOT NULL": a = 42 is null-rejecting, so IS NOT NULL is redundant and
        // pruned. An unsupported Optional.empty() child is ignored by AND evaluation, so the
        // result alone cannot prove pruning; record whether the visitor is invoked.
        GlobalIndexReader equalOnlyReader =
                new StubGlobalIndexReader(resultOf(1, 2, 3)) {
                    @Override
                    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(
                            FieldRef fieldRef) {
                        isNotNullVisited.set(true);
                        // Simulate a reader that cannot serve IS NOT NULL.
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType, fieldId -> Collections.singletonList(equalOnlyReader));

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 42), builder.isNotNull(0));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 1L, 2L, 3L);
        assertThat(isNotNullVisited).isFalse();
        evaluator.close();
    }

    @Test
    void testUnsupportedIsNaNFallsBack() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "a", DataTypes.DOUBLE())));
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> Collections.singletonList(new StubGlobalIndexReader(null)));

        Optional<GlobalIndexResult> result =
                evaluator.evaluate(new PredicateBuilder(rowType).isNaN(0));

        assertThat(result).isEmpty();
        evaluator.close();
    }

    @Test
    void testNotBetweenThroughUnionAndOffset() {
        RowType rowType = rowType();
        GlobalIndexReader delegate =
                new StubGlobalIndexReader(null) {
                    @Override
                    public CompletableFuture<Optional<GlobalIndexResult>> visitNotBetween(
                            FieldRef fieldRef, Object from, Object to) {
                        return CompletableFuture.completedFuture(Optional.of(resultOf(1, 3)));
                    }
                };
        GlobalIndexReader wrapped =
                new UnionGlobalIndexReader(
                        Collections.singletonList(new OffsetGlobalIndexReader(delegate, 10L, 20L)));
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, fieldId -> Collections.singletonList(wrapped));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Optional<GlobalIndexResult> result =
                evaluator.evaluate(builder.between(0, 1, 2).negate().get());

        assertThat(result).isPresent();
        assertBitmapContainsExactly(result.get().results(), 11L, 13L);
        evaluator.close();
    }

    @Test
    void testWrappedUnsupportedRangePredicatesFallBack() {
        RowType rowType = rowType();
        GlobalIndexReader wrapped =
                new UnionGlobalIndexReader(
                        Collections.singletonList(
                                new OffsetGlobalIndexReader(
                                        new StubGlobalIndexReader(null), 10L, 20L)));
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, fieldId -> Collections.singletonList(wrapped));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        assertThat(evaluator.evaluate(builder.between(0, 1, 2).negate().get())).isEmpty();
        assertThat(evaluator.evaluate(builder.between(0, 1, 2))).isEmpty();
        evaluator.close();
    }

    @Test
    void testConstantReaderReturnsFixedResultForIsNaNAndNotBetween() {
        GlobalIndexResult expected = resultOf(1, 2);
        GlobalIndexReader reader = new ConstantGlobalIndexReader(expected);
        FieldRef fieldRef = new FieldRef(0, "a", DataTypes.DOUBLE());

        assertThat(reader.visitIsNaN(fieldRef).join()).contains(expected);
        assertThat(reader.visitNotBetween(fieldRef, 1, 2).join()).contains(expected);
    }

    @Test
    void testNullPredicate() {
        RowType rowType = rowType();
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, fieldId -> Collections.emptyList());

        Optional<GlobalIndexResult> result = evaluator.evaluate(null);

        assertThat(result).isEmpty();
        evaluator.close();
    }

    @Test
    void testUnionReaderClosesRemainingReadersAfterFailure() {
        AtomicBoolean secondClosed = new AtomicBoolean();
        GlobalIndexReader failingReader =
                new StubGlobalIndexReader(null) {
                    @Override
                    public void close() throws IOException {
                        throw new IOException("expected close failure");
                    }
                };
        GlobalIndexReader secondReader =
                new StubGlobalIndexReader(null) {
                    @Override
                    public void close() {
                        secondClosed.set(true);
                    }
                };

        UnionGlobalIndexReader union =
                new UnionGlobalIndexReader(Arrays.asList(failingReader, secondReader));

        assertThatThrownBy(union::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected close failure");
        assertThat(secondClosed).isTrue();
    }

    @Test
    void testUnionReaderReportsVectorSearchDuration() {
        AtomicInteger callbacks = new AtomicInteger();
        GlobalIndexReader reader =
                new StubGlobalIndexReader(null) {
                    @Override
                    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
                            VectorSearch vectorSearch) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                };
        UnionGlobalIndexReader union =
                new UnionGlobalIndexReader(
                        Collections.singletonList(reader), ignored -> callbacks.incrementAndGet());

        union.visitVectorSearch(new VectorSearch(new float[] {1}, 1, "test")).join();

        assertThat(callbacks).hasValue(1);
    }

    private static void assertBitmapContainsExactly(
            RoaringNavigableMap64 bitmap, long... expected) {
        assertThat(bitmap.getLongCardinality()).isEqualTo(expected.length);
        for (long val : expected) {
            assertThat(bitmap.contains(val)).isTrue();
        }
    }

    private static class StubGlobalIndexReader implements GlobalIndexReader {

        protected final GlobalIndexResult result;

        StubGlobalIndexReader(GlobalIndexResult result) {
            this.result = result;
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.ofNullable(result));
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
            return CompletableFuture.completedFuture(Optional.ofNullable(result));
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
                FieldRef fieldRef, Object literal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
                FieldRef fieldRef, List<Object> literals) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
                FieldRef fieldRef, List<Object> literals) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() throws IOException {}
    }
}
