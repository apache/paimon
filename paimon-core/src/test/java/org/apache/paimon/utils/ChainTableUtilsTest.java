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

package org.apache.paimon.utils;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test class for {@link org.apache.paimon.utils.ChainTableUtils}. */
public class ChainTableUtilsTest {

    public static final RecordComparator KEY_COMPARATOR =
            (a, b) -> a.getString(0).compareTo(b.getString(0));

    private static String partString(BinaryRow partition, InternalRow.FieldGetter[] getters) {
        StringBuilder builder = new StringBuilder();
        for (InternalRow.FieldGetter getter : getters) {
            builder.append(getter.getFieldOrNull(partition));
        }
        return builder.toString();
    }

    @Test
    public void testFindFirstLatestPartitions() {
        String[] snapshotPartitions = {"20251105", "20251101"};
        String[] deltaPartitions = {"20251102", "20251106"};

        RowType rowType = RowType.builder().field("dt", DataTypes.STRING().notNull()).build();

        BinaryRow snapshotPartition1 = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(snapshotPartition1);
        writer.writeString(0, BinaryString.fromString(snapshotPartitions[0]));
        InternalRow.FieldGetter[] fieldGetters =
                InternalRowUtils.createFieldGetters(rowType.getFieldTypes());

        BinaryRow snapshotPartition2 = new BinaryRow(1);
        writer = new BinaryRowWriter(snapshotPartition2);
        writer.writeString(0, BinaryString.fromString(snapshotPartitions[1]));
        List<BinaryRow> sourcePartitions =
                Stream.of(snapshotPartition1, snapshotPartition2)
                        .sorted(KEY_COMPARATOR)
                        .collect(Collectors.toList());

        BinaryRow deltaPartition1 = new BinaryRow(1);
        writer = new BinaryRowWriter(deltaPartition1);
        writer.writeString(0, BinaryString.fromString(deltaPartitions[0]));

        BinaryRow deltaPartition2 = new BinaryRow(1);
        writer = new BinaryRowWriter(deltaPartition2);
        writer.writeString(0, BinaryString.fromString(deltaPartitions[1]));
        List<BinaryRow> targetPartitions =
                Stream.of(deltaPartition1, deltaPartition2)
                        .sorted(KEY_COMPARATOR)
                        .collect(Collectors.toList());

        Map<BinaryRow, BinaryRow> firstLatestPartitions =
                ChainTableUtils.findFirstLatestPartitions(
                        targetPartitions, sourcePartitions, KEY_COMPARATOR);

        BinaryRow key1 = firstLatestPartitions.get(deltaPartition1);
        Assertions.assertEquals(partString(key1, fieldGetters), snapshotPartitions[1]);

        BinaryRow key2 = firstLatestPartitions.get(deltaPartition2);
        Assertions.assertEquals(partString(key2, fieldGetters), snapshotPartitions[0]);
    }

    @Test
    public void testCreateTriangularPredicate() {
        RowType rowType =
                RowType.builder()
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();
        BinaryRow partitionValue = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partitionValue);
        writer.writeString(0, BinaryString.fromString("20250810"));
        writer.writeString(1, BinaryString.fromString("23"));
        RowDataToObjectArrayConverter partitionConverter =
                new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                ChainTableUtils.createTriangularPredicate(
                        partitionValue,
                        partitionConverter,
                        (Integer i, Object j) -> builder.equal(i, j),
                        (Integer i, Object j) -> builder.lessThan(i, j));
        Assertions.assertTrue(!predicate.test(partitionValue));
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.lessThan(0, partitionValue.getString(0)));
        List<Predicate> subPredicates = new ArrayList<>();
        subPredicates.add(builder.equal(0, partitionValue.getString(0)));
        subPredicates.add(builder.lessThan(1, partitionValue.getString(1)));
        predicates.add(PredicateBuilder.and(subPredicates));
        Predicate expected = PredicateBuilder.or(predicates);
        Assertions.assertTrue(predicate.equals(expected));
    }

    @Test
    public void testCreateLinearPredicate() {
        RowType rowType =
                RowType.builder()
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();
        BinaryRow partitionValue = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partitionValue);
        writer.writeString(0, BinaryString.fromString("20250810"));
        writer.writeString(1, BinaryString.fromString("23"));
        RowDataToObjectArrayConverter partitionConverter =
                new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate =
                ChainTableUtils.createLinearPredicate(
                        partitionValue,
                        partitionConverter,
                        (Integer i, Object j) -> builder.equal(i, j));
        Assertions.assertTrue(predicate.test(partitionValue));
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(0, partitionValue.getString(0)));
        predicates.add(builder.equal(1, partitionValue.getString(1)));
        Predicate expected = PredicateBuilder.and(predicates);
        Assertions.assertTrue(predicate.equals(expected));
    }

    @Test
    public void testGeneratePartitionValues() {
        LinkedHashMap<String, String> partitionValues =
                ChainTableUtils.calPartValues(
                        LocalDateTime.of(2023, 1, 1, 12, 0, 0),
                        Arrays.asList("dt", "hour"),
                        "$dt $hour:00:00",
                        "yyyyMMdd HH:mm:ss");
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                        put("hour", "12");
                    }
                },
                partitionValues);

        partitionValues =
                ChainTableUtils.calPartValues(
                        LocalDateTime.of(2023, 1, 1, 0, 0, 0),
                        Arrays.asList("dt"),
                        "$dt",
                        "yyyyMMdd");
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                    }
                },
                partitionValues);
    }
}
