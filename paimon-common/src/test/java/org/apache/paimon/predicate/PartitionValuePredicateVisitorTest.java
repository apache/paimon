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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionValuePredicateVisitor}. */
public class PartitionValuePredicateVisitorTest {

    // Table schema: (pt INT, a INT, b INT), partition key: pt
    private static final RowType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(0, "pt", DataTypes.INT()),
                    DataTypes.FIELD(1, "a", DataTypes.INT()),
                    DataTypes.FIELD(2, "b", DataTypes.INT()));

    private static final RowType PARTITION_TYPE =
            DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.INT()));

    private static final PredicateBuilder BUILDER = new PredicateBuilder(TABLE_TYPE);

    // ========================== Leaf: partition field ==========================

    @Test
    public void testPartitionEqualMatch() {
        // pt = 1, partition value is pt=1 => AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.equal(0, 1).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testPartitionEqualNoMatch() {
        // pt = 2, partition value is pt=1 => AlwaysFalse
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.equal(0, 2).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());
    }

    @Test
    public void testPartitionGreaterThanMatch() {
        // pt > 0, partition value is pt=1 => AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.greaterThan(0, 0).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testPartitionGreaterThanNoMatch() {
        // pt > 5, partition value is pt=1 => AlwaysFalse
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.greaterThan(0, 5).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());
    }

    @Test
    public void testPartitionLessOrEqualMatch() {
        // pt <= 1, partition value is pt=1 => AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.lessOrEqual(0, 1).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testPartitionBetweenMatch() {
        // pt BETWEEN 0 AND 5, partition value is pt=3 => AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(3));

        Optional<Predicate> result = BUILDER.between(0, 0, 5).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testPartitionBetweenNoMatch() {
        // pt BETWEEN 5 AND 10, partition value is pt=3 => AlwaysFalse
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(3));

        Optional<Predicate> result = BUILDER.between(0, 5, 10).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());
    }

    @Test
    public void testPartitionIsNullNoMatch() {
        // pt IS NULL, partition value is pt=1 => AlwaysFalse
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.isNull(0).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());
    }

    @Test
    public void testPartitionIsNotNullMatch() {
        // pt IS NOT NULL, partition value is pt=1 => AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Optional<Predicate> result = BUILDER.isNotNull(0).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    // ========================== Leaf: non-partition field ==========================

    @Test
    public void testNonPartitionFieldKeptAsIs() {
        // a = 5 is not a partition predicate => kept unchanged
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate original = BUILDER.equal(1, 5);
        Optional<Predicate> result = original.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(original);
    }

    // ========================== Compound: AND ==========================

    @Test
    public void testAndPartitionMatchAndNonPartition() {
        // (pt = 1 AND a = 5), partition value is pt=1
        // => AlwaysTrue simplified away by PredicateBuilder.and(), leaving just a=5
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate and = PredicateBuilder.and(BUILDER.equal(0, 1), BUILDER.equal(1, 5));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(BUILDER.equal(1, 5));
    }

    @Test
    public void testAndPartitionNoMatchAndNonPartition() {
        // (pt = 2 AND a = 5), partition value is pt=1
        // => AlwaysFalse short-circuits the entire AND
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate and = PredicateBuilder.and(BUILDER.equal(0, 2), BUILDER.equal(1, 5));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());
    }

    @Test
    public void testAndAllPartitionMatch() {
        // (pt = 1 AND pt > 0), partition value is pt=1
        // => both AlwaysTrue, simplified to AlwaysTrue
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate and = PredicateBuilder.and(BUILDER.equal(0, 1), BUILDER.greaterThan(0, 0));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    // ========================== Compound: OR ==========================

    @Test
    public void testOrPartitionPredicates() {
        // (pt = 1 OR pt = 2), partition value is pt=1
        // => AlwaysTrue short-circuits the entire OR
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 1), BUILDER.equal(0, 2));
        Optional<Predicate> result = or.visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testOrPartitionMatchAndNonPartition() {
        // (pt = 1 OR a = 5), partition value is pt=1
        // => AlwaysTrue short-circuits the entire OR
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 1), BUILDER.equal(1, 5));
        Optional<Predicate> result = or.visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testOrPartitionNoMatchAndNonPartition() {
        // (pt = 2 OR a = 5), partition value is pt=1
        // => AlwaysFalse filtered out, leaving just a=5
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 2), BUILDER.equal(1, 5));
        Optional<Predicate> result = or.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(BUILDER.equal(1, 5));
    }

    // ========================== Multiple partition keys ==========================

    @Test
    public void testMultiplePartitionFieldsBothMatch() {
        // Table: (pt1 INT, pt2 STRING, a INT), partition keys: (pt1, pt2)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.STRING()),
                        DataTypes.FIELD(2, "a", DataTypes.INT()));
        RowType partitionType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.STRING()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // pt1 = 1 AND pt2 = 'x', partition value is (1, 'x') => both match => AlwaysTrue
        GenericRow partitionRow = GenericRow.of(1, BinaryString.fromString("x"));
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(tableType, partitionType, partitionRow);

        Predicate and =
                PredicateBuilder.and(
                        builder.equal(0, 1), builder.equal(1, BinaryString.fromString("x")));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());
    }

    @Test
    public void testMultiplePartitionFieldsPartialMatch() {
        // pt1 = 1 AND pt2 = 'y', partition value is (1, 'x') => pt1 matches, pt2 doesn't
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.STRING()),
                        DataTypes.FIELD(2, "a", DataTypes.INT()));
        RowType partitionType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.STRING()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        GenericRow partitionRow = GenericRow.of(1, BinaryString.fromString("x"));
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(tableType, partitionType, partitionRow);

        Predicate and =
                PredicateBuilder.and(
                        builder.equal(0, 1), builder.equal(1, BinaryString.fromString("y")));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        // pt2 doesn't match => AlwaysFalse short-circuits the entire AND
        assertAlwaysFalse(result.get());
    }

    // ========================== Partition field not at index 0 ==========================

    @Test
    public void testPartitionFieldNotFirstInTable() {
        // Table: (a INT, pt INT, b INT), partition key: pt (index 1 in table, index 0 in partition)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        RowType partitionType = DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // pt = 3, partition value is pt=3
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(tableType, partitionType, GenericRow.of(3));

        Optional<Predicate> result = builder.equal(1, 3).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysTrue(result.get());

        // pt = 5, partition value is pt=3 => AlwaysFalse
        result = builder.equal(1, 5).visit(visitor);
        assertThat(result).isPresent();
        assertAlwaysFalse(result.get());

        // a = 10 => kept as-is (non-partition field)
        Predicate original = builder.equal(0, 10);
        result = original.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(original);
    }

    // ========================== Nested compound ==========================

    @Test
    public void testNestedAndOr() {
        // ((pt = 1 OR pt = 2) AND a = 5), partition value is pt=1
        // Inner OR: AlwaysTrue short-circuits to AlwaysTrue
        // Outer AND: AlwaysTrue simplified away, leaving just a=5
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 1), BUILDER.equal(0, 2));
        Predicate and = PredicateBuilder.and(or, BUILDER.equal(1, 5));
        Optional<Predicate> result = and.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(BUILDER.equal(1, 5));
    }

    // ========================== Only non-partition predicates ==========================

    @Test
    public void testAllNonPartitionPredicatesUnchanged() {
        // (a = 5 AND b = 10), partition value is pt=1 => kept unchanged
        PartitionValuePredicateVisitor visitor =
                new PartitionValuePredicateVisitor(TABLE_TYPE, PARTITION_TYPE, GenericRow.of(1));

        Predicate original = PredicateBuilder.and(BUILDER.equal(1, 5), BUILDER.equal(2, 10));
        Optional<Predicate> result = original.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(original);
    }

    // ========================== Helpers ==========================

    private static void assertAlwaysTrue(Predicate predicate) {
        assertThat(predicate).isInstanceOf(LeafPredicate.class);
        assertThat(((LeafPredicate) predicate).function()).isEqualTo(AlwaysTrue.INSTANCE);
    }

    private static void assertAlwaysFalse(Predicate predicate) {
        assertThat(predicate).isInstanceOf(LeafPredicate.class);
        assertThat(((LeafPredicate) predicate).function()).isEqualTo(AlwaysFalse.INSTANCE);
    }
}
