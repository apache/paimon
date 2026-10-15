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

import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PhysicalBucketPredicate}. */
class PhysicalBucketPredicateTest {

    private static final int[] BOUNDARIES = {
        Integer.MIN_VALUE,
        Integer.MIN_VALUE + 1,
        -2,
        -1,
        0,
        1,
        2,
        Integer.MAX_VALUE - 1,
        Integer.MAX_VALUE
    };

    @Test
    void testComparisons() {
        for (int value : BOUNDARIES) {
            assertMatches(PhysicalBucketPredicate.equal(value), b -> b == value);
            assertMatches(PhysicalBucketPredicate.lessThan(value), b -> b < value);
            assertMatches(PhysicalBucketPredicate.lessOrEqual(value), b -> b <= value);
            assertMatches(PhysicalBucketPredicate.greaterThan(value), b -> b > value);
            assertMatches(PhysicalBucketPredicate.greaterOrEqual(value), b -> b >= value);
        }
        assertThat(PhysicalBucketPredicate.lessThan(Integer.MIN_VALUE))
                .isEqualTo(PhysicalBucketPredicate.none());
        assertThat(PhysicalBucketPredicate.greaterThan(Integer.MAX_VALUE))
                .isEqualTo(PhysicalBucketPredicate.none());
        assertThat(PhysicalBucketPredicate.lessOrEqual(Integer.MAX_VALUE))
                .isEqualTo(PhysicalBucketPredicate.all());
        assertThat(PhysicalBucketPredicate.greaterOrEqual(Integer.MIN_VALUE))
                .isEqualTo(PhysicalBucketPredicate.all());
    }

    @Test
    void testAllAndNone() {
        assertMatches(PhysicalBucketPredicate.all(), b -> true);
        assertMatches(PhysicalBucketPredicate.none(), b -> false);
        assertThat(PhysicalBucketPredicate.all().negate())
                .isEqualTo(PhysicalBucketPredicate.none());
        assertThat(PhysicalBucketPredicate.none().negate())
                .isEqualTo(PhysicalBucketPredicate.all());
        assertThat(PhysicalBucketPredicate.none().mayContain(Integer.MIN_VALUE, Integer.MAX_VALUE))
                .isFalse();
    }

    @Test
    void testInNormalizesAndCopiesInput() {
        int[] buckets = {2, -2, 1, 2, -1, 0, Integer.MIN_VALUE, Integer.MAX_VALUE};
        PhysicalBucketPredicate predicate = PhysicalBucketPredicate.in(buckets);
        Arrays.fill(buckets, 100);
        assertMatches(
                predicate,
                b -> (b >= -2 && b <= 2) || b == Integer.MIN_VALUE || b == Integer.MAX_VALUE);
        assertThat(predicate)
                .isEqualTo(
                        PhysicalBucketPredicate.greaterOrEqual(-2)
                                .intersect(PhysicalBucketPredicate.lessOrEqual(2))
                                .union(PhysicalBucketPredicate.equal(Integer.MIN_VALUE))
                                .union(PhysicalBucketPredicate.equal(Integer.MAX_VALUE)))
                .hasSameHashCodeAs(
                        PhysicalBucketPredicate.in(
                                Integer.MAX_VALUE, 0, 1, -1, 2, -2, Integer.MIN_VALUE));
        assertThat(PhysicalBucketPredicate.in()).isEqualTo(PhysicalBucketPredicate.none());
    }

    @Test
    void testSetOperationsAgainstEnumeration() {
        Random random = new Random(49271);
        for (int iteration = 0; iteration < 100; iteration++) {
            int[] leftValues = random.ints(30, -20, 21).toArray();
            int[] rightValues = random.ints(30, -20, 21).toArray();
            PhysicalBucketPredicate left = PhysicalBucketPredicate.in(leftValues);
            PhysicalBucketPredicate right = PhysicalBucketPredicate.in(rightValues);
            IntPredicate inLeft = b -> Arrays.stream(leftValues).anyMatch(v -> v == b);
            IntPredicate inRight = b -> Arrays.stream(rightValues).anyMatch(v -> v == b);
            assertMatches(left.intersect(right), b -> inLeft.test(b) && inRight.test(b));
            assertMatches(left.union(right), b -> inLeft.test(b) || inRight.test(b));
            assertMatches(left.negate(), b -> !inLeft.test(b));
            assertThat(left.union(left.negate())).isEqualTo(PhysicalBucketPredicate.all());
            assertThat(left.intersect(left.negate())).isEqualTo(PhysicalBucketPredicate.none());
            assertThat(left.negate().negate()).isEqualTo(left);
            assertThat(left.union(right).negate())
                    .isEqualTo(left.negate().intersect(right.negate()));
            assertThat(left.intersect(right).negate())
                    .isEqualTo(left.negate().union(right.negate()));
        }
    }

    @Test
    void testRangesAgainstEnumeration() {
        PhysicalBucketPredicate predicate =
                PhysicalBucketPredicate.in(-10, -2, 3, 9)
                        .union(
                                PhysicalBucketPredicate.greaterOrEqual(-6)
                                        .intersect(PhysicalBucketPredicate.lessOrEqual(-4)));
        for (int min = -15; min <= 15; min++) {
            for (int max = min; max <= 15; max++) {
                boolean expected =
                        IntStream.rangeClosed(min, max)
                                .anyMatch(
                                        b ->
                                                b == -10
                                                        || b == -2
                                                        || b == 3
                                                        || b == 9
                                                        || (b >= -6 && b <= -4));
                assertThat(predicate.mayContain(min, max))
                        .as("range [%s, %s]", min, max)
                        .isEqualTo(expected);
            }
        }
        assertThat(predicate.mayContain(Integer.MIN_VALUE, Integer.MAX_VALUE)).isTrue();
        assertThat(predicate.mayContain(Integer.MIN_VALUE, -11)).isFalse();
        assertThat(predicate.mayContain(10, Integer.MAX_VALUE)).isFalse();
        // Invalid statistics cannot prove that a manifest is disjoint.
        assertThat(predicate.mayContain(2, 1)).isTrue();
    }

    @Test
    void testComplementAtIntegerBoundaries() {
        PhysicalBucketPredicate endpoints =
                PhysicalBucketPredicate.in(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertMatches(endpoints.negate(), b -> b != Integer.MIN_VALUE && b != Integer.MAX_VALUE);
        assertThat(endpoints.negate().mayContain(Integer.MIN_VALUE, Integer.MIN_VALUE)).isFalse();
        assertThat(endpoints.negate().mayContain(Integer.MAX_VALUE, Integer.MAX_VALUE)).isFalse();
        assertThat(endpoints.negate().negate()).isEqualTo(endpoints);
        assertThat(
                        PhysicalBucketPredicate.lessThan(0)
                                .union(PhysicalBucketPredicate.greaterOrEqual(0)))
                .isEqualTo(PhysicalBucketPredicate.all());
    }

    @Test
    void testSerialization() throws Exception {
        List<PhysicalBucketPredicate> predicates = new ArrayList<>();
        predicates.add(PhysicalBucketPredicate.all());
        predicates.add(PhysicalBucketPredicate.none());
        predicates.add(PhysicalBucketPredicate.in(-2, 0, 10, Integer.MAX_VALUE).negate());
        predicates.add(
                PhysicalBucketPredicate.lessThan(-3)
                        .union(PhysicalBucketPredicate.greaterOrEqual(5))
                        .intersect(PhysicalBucketPredicate.equal(Integer.MIN_VALUE).negate()));
        for (PhysicalBucketPredicate predicate : predicates) {
            PhysicalBucketPredicate restored =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(predicate),
                            getClass().getClassLoader());
            assertThat(restored).isEqualTo(predicate).hasSameHashCodeAs(predicate);
            assertMatches(restored, predicate::test);
            for (int min : BOUNDARIES) {
                for (int max : BOUNDARIES) {
                    assertThat(restored.mayContain(min, max))
                            .isEqualTo(predicate.mayContain(min, max));
                }
            }
        }
    }

    private static void assertMatches(PhysicalBucketPredicate actual, IntPredicate expected) {
        IntStream.concat(Arrays.stream(BOUNDARIES), IntStream.rangeClosed(-20, 20))
                .forEach(
                        b ->
                                assertThat(actual.test(b))
                                        .as("bucket %s", b)
                                        .isEqualTo(expected.test(b)));
    }
}
