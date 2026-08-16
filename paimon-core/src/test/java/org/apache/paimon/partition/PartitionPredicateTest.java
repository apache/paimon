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
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

    // --- Tests for inclusive partition predicate extraction ---

    @Test
    public void testSplitOrOfAndExtractsPartitionPredicate() {
        // (pt = 1 AND b = 5) OR (pt = 2 AND b = 8) => extract (pt=1 OR pt=2)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("pt");
        PredicateBuilder builder = new PredicateBuilder(tableType);

        Predicate filter =
                PredicateBuilder.or(
                        PredicateBuilder.and(builder.equal(0, 1), builder.equal(2, 5)),
                        PredicateBuilder.and(builder.equal(0, 2), builder.equal(2, 8)));

        Pair<Optional<PartitionPredicate>, List<Predicate>> result =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        filter, tableType, partitionKeys);

        // Should extract partition predicate (pt=1 OR pt=2)
        assertThat(result.getLeft()).isPresent();
        // Original expression stays in nonPartitionFilters for row-level filtering
        assertThat(result.getRight()).hasSize(1);
        assertThat(result.getRight().get(0)).isEqualTo(filter);

        PartitionPredicate partPred = result.getLeft().get();
        assertThat(partPred.test(createSingleFieldPart(1))).isTrue();
        assertThat(partPred.test(createSingleFieldPart(2))).isTrue();
        assertThat(partPred.test(createSingleFieldPart(3))).isFalse();
    }

    @Test
    public void testSplitOrWithNonPartitionBranchNoExtraction() {
        // (pt = 1 AND b = 5) OR (b = 10) => one OR branch has no partition field
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("pt");
        PredicateBuilder builder = new PredicateBuilder(tableType);

        Predicate filter =
                PredicateBuilder.or(
                        PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 5)),
                        builder.equal(1, 10));

        Pair<Optional<PartitionPredicate>, List<Predicate>> result =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        filter, tableType, partitionKeys);

        assertThat(result.getLeft()).isEmpty();
        assertThat(result.getRight()).hasSize(1);
    }

    @Test
    public void testSplitOrOfAndCombinedWithPurePartitionAnd() {
        // ((pt=1 AND b=5) OR (pt=2 AND b=8)) AND pt > 0
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("pt");
        PredicateBuilder builder = new PredicateBuilder(tableType);

        Predicate orPart =
                PredicateBuilder.or(
                        PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 5)),
                        PredicateBuilder.and(builder.equal(0, 2), builder.equal(1, 8)));
        Predicate filter = PredicateBuilder.and(orPart, builder.greaterThan(0, 0));

        Pair<Optional<PartitionPredicate>, List<Predicate>> result =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        filter, tableType, partitionKeys);

        // Should extract: (pt=1 OR pt=2) AND (pt > 0)
        assertThat(result.getLeft()).isPresent();
        // nonPartitionFilters: the OR expression (it has data fields)
        assertThat(result.getRight()).hasSize(1);
        assertThat(result.getRight().get(0)).isEqualTo(orPart);

        PartitionPredicate partPred = result.getLeft().get();
        assertThat(partPred.test(createSingleFieldPart(1))).isTrue();
        assertThat(partPred.test(createSingleFieldPart(2))).isTrue();
        assertThat(partPred.test(createSingleFieldPart(0))).isFalse();
        assertThat(partPred.test(createSingleFieldPart(3))).isFalse();
    }

    @Test
    public void testSplitOrWithPureNonPartitionBranch() {
        // (pt = 1 OR b = 5) => OR with non-projectable child, no extraction
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("pt");
        PredicateBuilder builder = new PredicateBuilder(tableType);

        Predicate filter = PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 5));

        Pair<Optional<PartitionPredicate>, List<Predicate>> result =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        filter, tableType, partitionKeys);

        assertThat(result.getLeft()).isEmpty();
        assertThat(result.getRight()).hasSize(1);
    }

    @Test
    public void testSplitOrOfAndWithMultiPartitionKeys() {
        // (pt1=1 AND pt2=10 AND b=5) OR (pt1=2 AND pt2=20 AND b=8)
        // => extract (pt1=1 AND pt2=10) OR (pt1=2 AND pt2=20)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        List<String> partitionKeys = Arrays.asList("pt1", "pt2");
        PredicateBuilder builder = new PredicateBuilder(tableType);

        Predicate filter =
                PredicateBuilder.or(
                        PredicateBuilder.and(
                                builder.equal(0, 1), builder.equal(1, 10), builder.equal(2, 5)),
                        PredicateBuilder.and(
                                builder.equal(0, 2), builder.equal(1, 20), builder.equal(2, 8)));

        Pair<Optional<PartitionPredicate>, List<Predicate>> result =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        filter, tableType, partitionKeys);

        assertThat(result.getLeft()).isPresent();
        assertThat(result.getRight()).hasSize(1);

        PartitionPredicate partPred = result.getLeft().get();
        assertThat(partPred.test(createPart(1, 10))).isTrue();
        assertThat(partPred.test(createPart(2, 20))).isTrue();
        assertThat(partPred.test(createPart(1, 20))).isFalse();
        assertThat(partPred.test(createPart(3, 30))).isFalse();
    }

    private static BinaryRow createSingleFieldPart(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
