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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        return GlobalIndexResult.create(
                () -> {
                    RoaringNavigableMap64 bm = new RoaringNavigableMap64();
                    for (long id : rowIds) {
                        bm.add(id);
                    }
                    return bm;
                });
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
                        rowType,
                        fieldId -> Collections.singletonList(readerReturning(expected)),
                        null);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                        },
                        executor);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                                            public Optional<GlobalIndexResult> visitEqual(
                                                    FieldRef fieldRef, Object literal) {
                                                threadNames.put(
                                                        Thread.currentThread().getName(), true);
                                                return super.visitEqual(fieldRef, literal);
                                            }
                                        }),
                        executor);

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
                        },
                        null);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
                                        readerReturning(fieldResults.get(fieldId))),
                        executor);

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
    void testSameFieldPredicatesNotAccessedConcurrently() {
        executor = Executors.newFixedThreadPool(4);
        RowType rowType = rowType();

        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        GlobalIndexReader concurrencyDetectingReader =
                new StubGlobalIndexReader(resultOf(1, 2, 3, 4, 5)) {
                    @Override
                    public Optional<GlobalIndexResult> visitEqual(
                            FieldRef fieldRef, Object literal) {
                        int c = concurrency.incrementAndGet();
                        maxConcurrency.updateAndGet(cur -> Math.max(cur, c));
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        concurrency.decrementAndGet();
                        return super.visitEqual(fieldRef, literal);
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> Collections.singletonList(concurrencyDetectingReader),
                        executor);

        // AND(a=1, a=2, a=3) — all same field, must not run concurrently
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 1), builder.equal(0, 2), builder.equal(0, 3));

        evaluator.evaluate(predicate);

        assertThat(maxConcurrency.get()).isEqualTo(1);
        evaluator.close();
    }

    @Test
    void testMixedNestedSameFieldNotAccessedConcurrently() {
        executor = Executors.newFixedThreadPool(4);
        RowType rowType = rowType();

        AtomicInteger concurrencyA = new AtomicInteger(0);
        AtomicInteger maxConcurrencyA = new AtomicInteger(0);

        GlobalIndexReader concurrencyDetectingReaderA =
                new StubGlobalIndexReader(resultOf(1, 2, 3, 4, 5)) {
                    @Override
                    public Optional<GlobalIndexResult> visitEqual(
                            FieldRef fieldRef, Object literal) {
                        int c = concurrencyA.incrementAndGet();
                        maxConcurrencyA.updateAndGet(cur -> Math.max(cur, c));
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        concurrencyA.decrementAndGet();
                        return super.visitEqual(fieldRef, literal);
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> {
                            if (fieldId == 0) {
                                return Collections.singletonList(concurrencyDetectingReaderA);
                            }
                            return Collections.singletonList(
                                    readerReturning(resultOf(1, 2, 3, 4, 5)));
                        },
                        executor);

        // AND(OR(a=1, b=2), OR(a=3, c=4)) — field a appears in both OR subtrees
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(
                        PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 2)),
                        PredicateBuilder.or(builder.equal(0, 3), builder.equal(2, 4)));

        evaluator.evaluate(predicate);

        assertThat(maxConcurrencyA.get()).isEqualTo(1);
        evaluator.close();
    }

    @Test
    void testLazyResultNotMaterializedConcurrently() {
        executor = Executors.newFixedThreadPool(4);
        RowType rowType = rowType();

        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        GlobalIndexReader lazyReader =
                new StubGlobalIndexReader(null) {
                    @Override
                    public Optional<GlobalIndexResult> visitEqual(
                            FieldRef fieldRef, Object literal) {
                        return Optional.of(
                                GlobalIndexResult.create(
                                        () -> {
                                            int c = concurrency.incrementAndGet();
                                            maxConcurrency.updateAndGet(cur -> Math.max(cur, c));
                                            try {
                                                Thread.sleep(50);
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                            }
                                            concurrency.decrementAndGet();
                                            RoaringNavigableMap64 bm = new RoaringNavigableMap64();
                                            bm.add(1);
                                            bm.add(2);
                                            bm.add(3);
                                            return bm;
                                        }));
                    }
                };

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> {
                            if (fieldId == 0) {
                                return Collections.singletonList(lazyReader);
                            }
                            return Collections.singletonList(
                                    readerReturning(resultOf(1, 2, 3, 4, 5)));
                        },
                        executor);

        // AND(OR(a=1, b=2), OR(a=3, c=4)) — field a in both OR subtrees
        // lazy results for field a must not be materialized concurrently
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                PredicateBuilder.and(
                        PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 2)),
                        PredicateBuilder.or(builder.equal(0, 3), builder.equal(2, 4)));

        evaluator.evaluate(predicate);

        assertThat(maxConcurrency.get()).isEqualTo(1);
        evaluator.close();
    }

    @Test
    void testNonFieldLeafPredicateDoesNotThrow() {
        executor = Executors.newFixedThreadPool(2);
        RowType rowType = rowType();

        GlobalIndexResult resultA = resultOf(1, 2, 3);

        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        rowType,
                        fieldId -> Collections.singletonList(readerReturning(resultA)),
                        executor);

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
    void testNullPredicate() {
        RowType rowType = rowType();
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(rowType, fieldId -> Collections.emptyList(), null);

        Optional<GlobalIndexResult> result = evaluator.evaluate(null);

        assertThat(result).isEmpty();
        evaluator.close();
    }

    private static void assertBitmapContainsExactly(
            RoaringNavigableMap64 bitmap, long... expected) {
        assertThat(bitmap.getLongCardinality()).isEqualTo(expected.length);
        for (long val : expected) {
            assertThat(bitmap.contains(val)).isTrue();
        }
    }

    private static class StubGlobalIndexReader implements GlobalIndexReader {

        private final GlobalIndexResult result;

        StubGlobalIndexReader(GlobalIndexResult result) {
            this.result = result;
        }

        @Override
        public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
            return Optional.ofNullable(result);
        }

        @Override
        public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
            return Optional.ofNullable(result);
        }

        @Override
        public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return Optional.empty();
        }

        @Override
        public void close() {}
    }
}
