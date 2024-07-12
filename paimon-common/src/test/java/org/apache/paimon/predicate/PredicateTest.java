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

package org.apache.paimon.predicate;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.predicate.SimpleColStatsTestUtils.test;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Predicate}s. */
public class PredicateTest {

    @Test
    public void testEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 6, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.notEqual(0, 5));
    }

    @Test
    public void testEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notEqual(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 6, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(5, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.equal(0, 5));
    }

    @Test
    public void testNotEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notEqual(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testGreater() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterThan(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(6))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 4, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 6, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessOrEqual(0, 5));
    }

    @Test
    public void testGreaterNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterThan(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 4, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterOrEqual(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(6))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 4, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 6, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessThan(0, 5));
    }

    @Test
    public void testGreaterOrEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.greaterOrEqual(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 4, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testLess() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessThan(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(6))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(5, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(4, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterOrEqual(0, 5));
    }

    @Test
    public void testLessNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessThan(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testLessOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessOrEqual(0, 5);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(5))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(6))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(5, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(4, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterThan(0, 5));
    }

    @Test
    public void testLessOrEqualNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.lessOrEqual(0, null);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testIsNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.isNull(0);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(true);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(5, 7, 1L)}))
                .isEqualTo(true);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNotNull(0));
    }

    @Test
    public void testIsNotNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.isNotNull(0);

        assertThat(predicate.test(GenericRow.of(4))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(5, 7, 1L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(null, null, 3L)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNull(0));
    }

    @Test
    public void testIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.in(0, Arrays.asList(1, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.in(0, Arrays.asList(1, null, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notIn(0, Arrays.asList(1, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 1, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(3, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 3, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.notIn(0, Arrays.asList(1, null, 3));
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 1, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(3, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testEndsWith() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new VarCharType()));
        Predicate predicate = builder.endsWith(0, fromString("bcc"));
        GenericRow row = GenericRow.of(fromString("aabbcc"));

        GenericRow max = GenericRow.of(fromString("aaba"));
        GenericRow min = GenericRow.of(fromString("aabb"));
        Integer[] nullCount = {null};
        assertThat(predicate.test(row)).isEqualTo(true);
        assertThat(predicate.test(10, min, max, new GenericArray(nullCount))).isEqualTo(true);
    }

    @Test
    public void testLargeIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.in(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(null);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.in(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeNotIn() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.notIn(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 1, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(3, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 3, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(true);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(29, 32, 0L)}))
                .isEqualTo(true);
    }

    @Test
    public void testLargeNotInNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        literals.add(1);
        literals.add(null);
        literals.add(3);
        for (int i = 10; i < 30; i++) {
            literals.add(i);
        }
        Predicate predicate = builder.notIn(0, literals);
        assertThat(predicate).isInstanceOf(LeafPredicate.class);

        assertThat(predicate.test(GenericRow.of(1))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(2))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(false);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 1, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(3, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(1, 3, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(0, 5, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(6, 7, 0L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 1, new SimpleColStats[] {new SimpleColStats(null, null, 1L)}))
                .isEqualTo(false);
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(29, 32, 0L)}))
                .isEqualTo(false);
    }

    @Test
    public void testAnd() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate predicate = PredicateBuilder.and(builder.equal(0, 3), builder.equal(1, 5));

        assertThat(predicate.test(GenericRow.of(4, 5))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3, 6))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3, 5))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(null, 5))).isEqualTo(false);

        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(3, 6, 0L), new SimpleColStats(4, 6, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(3, 6, 0L), new SimpleColStats(6, 8, 0L)
                                }))
                .isEqualTo(false);
        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(6, 7, 0L), new SimpleColStats(4, 6, 0L)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.or(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @Test
    public void testOr() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate predicate = PredicateBuilder.or(builder.equal(0, 3), builder.equal(1, 5));

        assertThat(predicate.test(GenericRow.of(4, 6))).isEqualTo(false);
        assertThat(predicate.test(GenericRow.of(3, 6))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(3, 5))).isEqualTo(true);
        assertThat(predicate.test(GenericRow.of(null, 5))).isEqualTo(true);

        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(3, 6, 0L), new SimpleColStats(4, 6, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(3, 6, 0L), new SimpleColStats(6, 8, 0L)
                                }))
                .isEqualTo(true);
        assertThat(
                        test(
                                predicate,
                                3,
                                new SimpleColStats[] {
                                    new SimpleColStats(6, 7, 0L), new SimpleColStats(8, 10, 0L)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.and(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @Test
    public void testUnknownStats() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.equal(0, 5);

        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(null, null, 3L)}))
                .isEqualTo(false);

        // unknown stats, we don't know, likely to hit
        assertThat(test(predicate, 3, new SimpleColStats[] {new SimpleColStats(null, null, 4L)}))
                .isEqualTo(true);
    }

    @Test
    public void testPredicateToString() {
        PredicateBuilder builder1 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p1 = builder1.equal(0, 5);
        assertThat(p1.toString()).isEqualTo("Equal(f0, 5)");

        PredicateBuilder builder2 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p2 = builder2.greaterThan(0, 5);
        assertThat(p2.toString()).isEqualTo("GreaterThan(f0, 5)");

        PredicateBuilder builder3 = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate p3 = PredicateBuilder.and(builder3.equal(0, 3), builder3.equal(1, 5));
        assertThat(p3.toString()).isEqualTo("And([Equal(f0, 3), Equal(f1, 5)])");

        PredicateBuilder builder4 = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate p4 = PredicateBuilder.or(builder4.equal(0, 3), builder4.equal(1, 5));
        assertThat(p4.toString()).isEqualTo("Or([Equal(f0, 3), Equal(f1, 5)])");

        PredicateBuilder builder5 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p5 = builder5.isNotNull(0);
        assertThat(p5.toString()).isEqualTo("IsNotNull(f0)");

        PredicateBuilder builder6 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p6 = builder6.in(0, Arrays.asList(1, null, 3, 4));
        assertThat(p6.toString())
                .isEqualTo(
                        "Or([Or([Or([Equal(f0, 1), Equal(f0, null)]), Equal(f0, 3)]), Equal(f0, 4)])");

        PredicateBuilder builder7 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p7 = builder7.notIn(0, Arrays.asList(1, null, 3, 4));
        assertThat(p7.toString())
                .isEqualTo(
                        "And([And([And([NotEqual(f0, 1), NotEqual(f0, null)]), NotEqual(f0, 3)]), NotEqual(f0, 4)])");

        PredicateBuilder builder8 = new PredicateBuilder(RowType.of(new IntType()));
        List<Object> literals = new ArrayList<>();
        for (int i = 1; i <= 21; i++) {
            literals.add(i);
        }
        Predicate p8 = builder8.in(0, literals);
        assertThat(p8.toString())
                .isEqualTo(
                        "In(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21])");

        PredicateBuilder builder9 = new PredicateBuilder(RowType.of(new IntType()));
        Predicate p9 = builder9.notIn(0, literals);
        assertThat(p9.toString())
                .isEqualTo(
                        "NotIn(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21])");
    }
}
