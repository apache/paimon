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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Utils for testing with {@link PartitionKeyVisitor}. */
public class PartitionKeyVisitorTest {

    @Test
    public void testSinglePartition() {
        List<String> partitionKeys = Collections.singletonList("year");

        BinaryRow pt1 = buildPartitionRow("2022");
        BinaryRow pt2 = buildPartitionRow("2023");
        BinaryRow pt3 = buildPartitionRow("2024");
        BinaryRow pt4 = buildPartitionRow("2025");
        List<BinaryRow> partitions = Arrays.asList(pt1, pt2, pt3, pt4);

        PartitionKeyVisitor visitor = new PartitionKeyVisitor(partitionKeys, partitions);

        Predicate isNull =
                new LeafPredicate(IsNull.INSTANCE, DataTypes.STRING(), 100, "year", null);
        assertThat(isNull.visit(visitor)).hasSameElementsAs(Collections.emptyList());

        Predicate isNotNull =
                new LeafPredicate(IsNotNull.INSTANCE, DataTypes.STRING(), 100, "year", null);
        assertThat(isNotNull.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2, pt3, pt4));

        Predicate equal =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(equal.visit(visitor)).hasSameElementsAs(Collections.singletonList(pt3));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate notEqual =
                new LeafPredicate(
                        NotEqual.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(notEqual.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate gt =
                new LeafPredicate(
                        GreaterThan.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(gt.visit(visitor)).hasSameElementsAs(Collections.singletonList(pt4));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate ge =
                new LeafPredicate(
                        GreaterOrEqual.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(ge.visit(visitor)).hasSameElementsAs(Arrays.asList(pt3, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate lt =
                new LeafPredicate(
                        LessThan.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(lt.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate le =
                new LeafPredicate(
                        LessOrEqual.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        assertThat(le.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2, pt3));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate in =
                new LeafPredicate(
                        In.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Arrays.asList(
                                BinaryString.fromString("2023"), BinaryString.fromString("2024")));
        assertThat(in.visit(visitor)).hasSameElementsAs(Arrays.asList(pt2, pt3));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate notIn =
                new LeafPredicate(
                        NotIn.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Arrays.asList(
                                BinaryString.fromString("2023"), BinaryString.fromString("2024")));
        assertThat(notIn.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate and = PredicateBuilder.and(Arrays.asList(equal, isNotNull));
        assertThat(and.visit(visitor)).hasSameElementsAs(Collections.singletonList(pt3));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate or = PredicateBuilder.or(Arrays.asList(lt, gt));
        assertThat(or.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate startsWith =
                new LeafPredicate(
                        StartsWith.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("20")));
        assertThat(startsWith.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2, pt3, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(false);

        visitor.setAccurate(true);
        assertThat(visitor.isAccurate()).isEqualTo(true);

        Predicate invalidField =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "month",
                        Collections.singletonList(BinaryString.fromString("12")));
        assertThat(invalidField.visit(visitor))
                .hasSameElementsAs(Arrays.asList(pt1, pt2, pt3, pt4));
        assertThat(visitor.isAccurate()).isEqualTo(false);
    }

    @Test
    public void testMultiPartitions() {
        List<String> partitionKeys = Arrays.asList("year", "region");

        BinaryRow pt1 = buildPartitionRow("2024", 123);
        BinaryRow pt2 = buildPartitionRow("2024", 456);
        BinaryRow pt3 = buildPartitionRow("2025", 123);
        BinaryRow pt4 = buildPartitionRow("2025", 456);
        List<BinaryRow> partitions = Arrays.asList(pt1, pt2, pt3, pt4);

        PartitionKeyVisitor visitor = new PartitionKeyVisitor(partitionKeys, partitions);

        Predicate equalYear2024 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2024")));
        Predicate equalYear2025 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        100,
                        "year",
                        Collections.singletonList(BinaryString.fromString("2025")));
        Predicate equalRegion123 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.INT(),
                        101,
                        "region",
                        Collections.singletonList(123));
        Predicate equalRegion456 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.INT(),
                        101,
                        "region",
                        Collections.singletonList(456));

        assertThat(equalYear2024.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt2));
        assertThat(equalRegion123.visit(visitor)).hasSameElementsAs(Arrays.asList(pt1, pt3));

        Predicate year2024AndRegion123 =
                PredicateBuilder.and(Arrays.asList(equalYear2024, equalRegion123));
        assertThat(year2024AndRegion123.visit(visitor))
                .hasSameElementsAs(Collections.singletonList(pt1));

        Predicate year2024OrRegion456 =
                PredicateBuilder.or(Arrays.asList(equalYear2024, equalRegion456));
        assertThat(year2024OrRegion456.visit(visitor))
                .hasSameElementsAs(Arrays.asList(pt1, pt2, pt4));

        assertThat(
                        PredicateBuilder.or(Arrays.asList(equalYear2024, year2024AndRegion123))
                                .visit(visitor))
                .hasSameElementsAs(Arrays.asList(pt1, pt2));
    }

    private BinaryRow buildPartitionRow(String value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, fromString(value));
        return row;
    }

    private BinaryRow buildPartitionRow(String value1, int value2) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, fromString(value1));
        writer.writeInt(1, value2);
        return row;
    }
}
