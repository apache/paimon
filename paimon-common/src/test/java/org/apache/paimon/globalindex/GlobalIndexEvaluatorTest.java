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
                        fieldId -> {
                            threadNames.put(Thread.currentThread().getName(), true);
                            return Collections.singletonList(
                                    readerReturning(resultOf(fieldId, fieldId + 10)));
                        },
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
                        });

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 2));

        Optional<GlobalIndexResult> result = evaluator.evaluate(predicate);

        assertThat(result).isPresent();
        assertThat(callCount.get()).isEqualTo(2);
        evaluator.close();
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
