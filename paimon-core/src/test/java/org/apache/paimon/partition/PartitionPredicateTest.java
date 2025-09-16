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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionPredicate}. */
public class PartitionPredicateTest {

    @Test
    public void testNoPartition() {
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(RowType.of(), Collections.singletonList(EMPTY_ROW));
        assertThat(predicate).isNull();
    }

    @Test
    public void testPartition() {
        RowType type = DataTypes.ROW(DataTypes.INT(), DataTypes.INT());
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate predicate =
                or(
                        and(builder.equal(0, 3), builder.equal(1, 5)),
                        and(builder.equal(0, 4), builder.equal(1, 6)));

        PartitionPredicate p1 = PartitionPredicate.fromPredicate(type, predicate);
        PartitionPredicate p2 =
                PartitionPredicate.fromMultiple(
                        type, Arrays.asList(createPart(3, 5), createPart(4, 6)));

        assertThat(validate(p1, p2, createPart(3, 4))).isFalse();
        assertThat(validate(p1, p2, createPart(3, 5))).isTrue();
        assertThat(validate(p1, p2, createPart(4, 6))).isTrue();
        assertThat(validate(p1, p2, createPart(4, 5))).isFalse();

        assertThat(
                        validate(
                                p1,
                                new SimpleColStats[] {
                                    new SimpleColStats(4, 8, 0L), new SimpleColStats(10, 12, 0L)
                                }))
                .isFalse();
        assertThat(
                        validate(
                                p2,
                                new SimpleColStats[] {
                                    new SimpleColStats(4, 8, 0L), new SimpleColStats(10, 12, 0L)
                                }))
                .isFalse();
        assertThat(
                        validate(
                                p2,
                                new SimpleColStats[] {
                                    new SimpleColStats(6, 8, 0L), new SimpleColStats(10, 12, 0L)
                                }))
                .isFalse();

        assertThat(
                        validate(
                                p1,
                                new SimpleColStats[] {
                                    new SimpleColStats(4, 8, 0L), new SimpleColStats(5, 12, 0L)
                                }))
                .isTrue();
        assertThat(
                        validate(
                                p2,
                                new SimpleColStats[] {
                                    new SimpleColStats(4, 8, 0L), new SimpleColStats(5, 12, 0L)
                                }))
                .isTrue();

        assertThat(
                        validate(
                                p1,
                                new SimpleColStats[] {
                                    new SimpleColStats(1, 2, 0L), new SimpleColStats(2, 3, 0L)
                                }))
                .isFalse();
        assertThat(
                        validate(
                                p2,
                                new SimpleColStats[] {
                                    new SimpleColStats(1, 2, 0L), new SimpleColStats(2, 3, 0L)
                                }))
                .isFalse();
    }

    @Test
    public void testPartitionWithMultiFields() {
        RowType type = DataTypes.ROW(DataTypes.INT(), DataTypes.INT());
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(type, Collections.singletonList(createPart(3, 4)));

        assertThat(
                        validate(
                                predicate,
                                new SimpleColStats[] {
                                    new SimpleColStats(2, 2, 0L), new SimpleColStats(4, 4, 0L)
                                }))
                .isFalse();
        assertThat(
                        validate(
                                predicate,
                                new SimpleColStats[] {
                                    new SimpleColStats(2, 4, 0L), new SimpleColStats(4, 4, 0L)
                                }))
                .isTrue();
    }

    private boolean validate(
            PartitionPredicate predicate1, PartitionPredicate predicate2, BinaryRow part) {
        boolean ret = predicate1.test(part);
        assertThat(predicate2.test(part)).isEqualTo(ret);
        return ret;
    }

    private boolean validate(PartitionPredicate predicate, SimpleColStats[] fieldStats) {
        Object[] min = new Object[fieldStats.length];
        Object[] max = new Object[fieldStats.length];
        Long[] nullCounts = new Long[fieldStats.length];
        for (int i = 0; i < fieldStats.length; i++) {
            min[i] = fieldStats[i].min();
            max[i] = fieldStats[i].max();
            nullCounts[i] = fieldStats[i].nullCount();
        }
        return predicate.test(
                3, GenericRow.of(min), GenericRow.of(max), BinaryArray.fromLongArray(nullCounts));
    }

    private static BinaryRow createPart(int i, int j) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, i);
        writer.writeInt(1, j);
        writer.complete();
        return row;
    }

    @Test
    public void testExtractEqualityPartitionSpecWithAllEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month = 12 AND day = 25
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate equalityPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.equal(2, 25));
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromPredicate(type, equalityPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);

        assertThat(result).isNotNull();
        assertThat(result).hasSize(3);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.get("month")).isEqualTo("12");
        assertThat(result.get("day")).isEqualTo("25");
    }

    @Test
    public void testExtractEqualityPartitionSpecWithLeadingConsecutiveEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month = 12 AND day > 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.greaterThan(2, 15));
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromPredicate(type, mixedPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);

        assertThat(result).isNotNull();
        assertThat(result).hasSize(2);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.get("month")).isEqualTo("12");
        assertThat(result.containsKey("day")).isFalse();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithFirstPartitionKeyEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month > 6 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.greaterThan(1, 6)),
                        builder.equal(2, 15));
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromPredicate(type, mixedPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);
        assertThat(result).isNotNull();
        assertThat(result).hasSize(1);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.containsKey("month")).isFalse();
        assertThat(result.containsKey("day")).isFalse();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithNoLeadingEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year > 2020 AND month = 12 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.greaterThan(0, 2020), builder.equal(1, 12)),
                        builder.equal(2, 15));
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromPredicate(type, mixedPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithNonEqualityPredicateWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month");

        // Create predicate: year > 2020 AND month > 6
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate nonEqualityPredicate =
                PredicateBuilder.and(builder.greaterThan(0, 2020), builder.greaterThan(1, 6));
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromPredicate(type, nonEqualityPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testExtractLeadingEqualityPartitionSpecWhenOnlyAndWithOrPredicate() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month");

        // Create predicate: year = 2023 OR year = 2024
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate orPredicate = PredicateBuilder.or(builder.equal(0, 2023), builder.equal(0, 2024));
        PartitionPredicate partitionPredicate = PartitionPredicate.fromPredicate(type, orPredicate);

        Map<String, String> result =
                partitionPredicate.extractLeadingEqualityPartitionSpecWhenOnlyAnd(partitionKeys);

        assertThat(result).isNull();
    }
}
