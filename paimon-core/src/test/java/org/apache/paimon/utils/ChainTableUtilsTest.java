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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.util.Lists;
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

import static org.assertj.core.api.Assertions.assertThat;
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

    /** Extract a string value from a BinaryRow at the given position. */
    private static String getString(BinaryRow row, int pos) {
        return row.getString(pos).toString();
    }

    private static BinaryRow row(List<String> values) {
        BinaryRow row = new BinaryRow(values.size());
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.size(); i++) {
            writer.writeString(i, BinaryString.fromString(values.get(i)));
        }
        writer.complete();
        return row;
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

    // ========================== Tests for findFirstLatestPartitionsWithProjector
    // ==========================

    @Test
    public void testFindFirstLatestPartitionsWithProjectorWithBasicGrouping() {
        // partition keys: (region, date), chain keys: (date)
        // Snapshot: (US,20250720), (US,20250725)
        // Delta:    (US,20250722), (US,20250727)
        // Expected: 20250722 → 20250720, 20250727 → 20250725
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        // Snapshot partitions, pre-sorted by chain field (date)
        BinaryRow snap1 = row(Lists.newArrayList("US", "20250720"));
        BinaryRow snap2 = row(Lists.newArrayList("US", "20250725"));
        List<BinaryRow> snapshotPartitions = Arrays.asList(snap1, snap2);

        // Delta partitions, pre-sorted by chain field (date)
        BinaryRow delta1 = row(Lists.newArrayList("US", "20250722"));
        BinaryRow delta2 = row(Lists.newArrayList("US", "20250727"));
        List<BinaryRow> deltaPartitions = Arrays.asList(delta1, delta2);

        Map<BinaryRow, BinaryRow> mapping =
                ChainTableUtils.findFirstLatestPartitionsWithProjector(
                        deltaPartitions, snapshotPartitions, chainComparator, projector);

        // delta (US,20250722) → snapshot (US,20250720)
        BinaryRow matched1 = mapping.get(delta1);
        assertThat(matched1).isNotNull();
        assertThat(getString(matched1, 0)).isEqualTo("US");
        assertThat(getString(matched1, 1)).isEqualTo("20250720");

        // delta (US,20250727) → snapshot (US,20250725)
        BinaryRow matched2 = mapping.get(delta2);
        assertThat(matched2).isNotNull();
        assertThat(getString(matched2, 0)).isEqualTo("US");
        assertThat(getString(matched2, 1)).isEqualTo("20250725");
    }

    @Test
    public void testFindFirstLatestPartitionsWithProjectorWithNoMatchingSnapshot() {
        // Delta partition is earlier than all snapshots → null mapping
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        List<BinaryRow> snapshotPartitions =
                Arrays.asList(row(Lists.newArrayList("US", "20250725")));
        BinaryRow delta1 = row(Lists.newArrayList("US", "20250720"));
        List<BinaryRow> deltaPartitions = Arrays.asList(delta1);

        Map<BinaryRow, BinaryRow> mapping =
                ChainTableUtils.findFirstLatestPartitionsWithProjector(
                        deltaPartitions, snapshotPartitions, chainComparator, projector);

        // delta 20250720 < snapshot 20250725, no earlier snapshot → null
        assertThat(mapping.get(delta1)).isNull();
    }

    @Test
    public void testFindFirstLatestPartitionsWithProjectorWithEmptySnapshot() {
        // No snapshot partitions at all → all mappings are null
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        BinaryRow delta1 = row(Lists.newArrayList("US", "20250720"));
        BinaryRow delta2 = row(Lists.newArrayList("US", "20250722"));
        List<BinaryRow> deltaPartitions = Arrays.asList(delta1, delta2);

        Map<BinaryRow, BinaryRow> mapping =
                ChainTableUtils.findFirstLatestPartitionsWithProjector(
                        deltaPartitions, new ArrayList<>(), chainComparator, projector);

        for (BinaryRow delta : deltaPartitions) {
            assertThat(mapping.get(delta)).isNull();
        }
    }

    @Test
    public void testFindFirstLatestPartitionsWithProjectorWithMultipleChainFields() {
        // partition keys: (region, dt, hour), chain keys: (dt, hour)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 2);

        // Compare chain partition (dt, hour) lexicographically
        RecordComparator chainComparator =
                (a, b) -> {
                    int cmp = a.getString(0).compareTo(b.getString(0));
                    if (cmp != 0) {
                        return cmp;
                    }
                    return a.getString(1).compareTo(b.getString(1));
                };

        // snapshot: (US, 20250720, 12)
        BinaryRow snap1 = row(Lists.newArrayList("US", "20250720", "12"));
        List<BinaryRow> snapshotPartitions = Arrays.asList(snap1);

        // delta: (US, 20250720, 10), (US, 20250720, 14) — pre-sorted by chain (dt, hour)
        BinaryRow delta1 = row(Lists.newArrayList("US", "20250720", "10"));
        BinaryRow delta2 = row(Lists.newArrayList("US", "20250720", "14"));
        List<BinaryRow> deltaPartitions = Arrays.asList(delta1, delta2);

        Map<BinaryRow, BinaryRow> mapping =
                ChainTableUtils.findFirstLatestPartitionsWithProjector(
                        deltaPartitions, snapshotPartitions, chainComparator, projector);

        // delta (US,20250720,10): chain (20250720,10) < snapshot chain (20250720,12) → null
        assertThat(mapping.get(delta1)).isNull();

        // delta (US,20250720,14): chain (20250720,14) > snapshot chain (20250720,12) → match
        BinaryRow matched = mapping.get(delta2);
        assertThat(matched).isNotNull();
        assertThat(getString(matched, 1)).isEqualTo("20250720");
        assertThat(getString(matched, 2)).isEqualTo("12");
    }

    // ========================== Tests for getDeltaPartitionsWithProjector
    // ==========================

    @Test
    public void testGetDeltaPartitionsWithProjectorWithDailyPartition() {
        // partition keys: (region, date), chain keys: (date)
        // begin: (US, 20250720), end: (US, 20250724)
        // Expected delta: (US,20250721), (US,20250722), (US,20250723), (US,20250724)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$date");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow begin = row(Lists.newArrayList("US", "20250720"));
        BinaryRow end = row(Lists.newArrayList("US", "20250724"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        begin, end, options, chainComparator, projector);

        // (20250720, 20250724]: 4 days
        assertThat(deltas).hasSize(4);
        assertThat(getString(deltas.get(0), 0)).isEqualTo("US");
        assertThat(getString(deltas.get(0), 1)).isEqualTo("20250721");
        assertThat(getString(deltas.get(1), 1)).isEqualTo("20250722");
        assertThat(getString(deltas.get(2), 1)).isEqualTo("20250723");
        assertThat(getString(deltas.get(3), 1)).isEqualTo("20250724");

        // all should have group = US
        for (BinaryRow delta : deltas) {
            assertThat(getString(delta, 0)).isEqualTo("US");
        }
    }

    @Test
    public void testGetDeltaPartitionsWithProjectorWithSameBeginAndEnd() {
        // begin == end → empty result (open-closed interval: (begin, end])
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$date");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow partition = row(Lists.newArrayList("EU", "20250720"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        partition, partition, options, chainComparator, projector);

        assertThat(deltas).isEmpty();
    }

    @Test
    public void testGetDeltaPartitionsWithProjectorWithConsecutiveDays() {
        // begin: (EU, 20250720), end: (EU, 20250721) → only (EU, 20250721)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$date");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow begin = row(Lists.newArrayList("EU", "20250720"));
        BinaryRow end = row(Lists.newArrayList("EU", "20250721"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        begin, end, options, chainComparator, projector);

        assertThat(deltas).hasSize(1);
        assertThat(getString(deltas.get(0), 0)).isEqualTo("EU");
        assertThat(getString(deltas.get(0), 1)).isEqualTo("20250721");
    }

    @Test
    public void testGetDeltaPartitionsWithProjectorWithGroupPreserved() {
        // Verify that group part from beginPartition is preserved for all results
        RowType fullType =
                RowType.builder()
                        .field("country", DataTypes.STRING().notNull())
                        .field("city", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        // chain keys: (date), group keys: (country, city)
        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);
        RecordComparator chainComparator = (a, b) -> a.getString(0).compareTo(b.getString(0));

        Options opts = new Options();
        opts.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$date");
        opts.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        CoreOptions options = new CoreOptions(opts);

        BinaryRow begin = row(Lists.newArrayList("China", "Beijing", "20250720"));
        BinaryRow end = row(Lists.newArrayList("China", "Beijing", "20250723"));

        List<BinaryRow> deltas =
                ChainTableUtils.getDeltaPartitionsWithProjector(
                        begin, end, options, chainComparator, projector);

        assertThat(deltas).hasSize(3);
        for (BinaryRow delta : deltas) {
            assertThat(getString(delta, 0)).isEqualTo("China");
            assertThat(getString(delta, 1)).isEqualTo("Beijing");
        }
        assertThat(getString(deltas.get(0), 2)).isEqualTo("20250721");
        assertThat(getString(deltas.get(1), 2)).isEqualTo("20250722");
        assertThat(getString(deltas.get(2), 2)).isEqualTo("20250723");
    }

    // ========================== Tests for createGroupChainPredicate ==========================

    @Test
    public void testCreateGroupChainPredicateWithOneGroupOneChainLessThan() {
        // partition: (region, date), groupFieldCount=1
        // Expected: region=equal AND date=lessThan
        RowType rowType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("US", "20250726"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate =
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        1,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v));

        // Expected: region='US' AND date < '20250726'
        List<Predicate> expected = new ArrayList<>();
        expected.add(builder.equal(0, BinaryString.fromString("US")));
        expected.add(builder.lessThan(1, BinaryString.fromString("20250726")));
        Predicate expectedPredicate = PredicateBuilder.and(expected);

        assertThat(predicate).isEqualTo(expectedPredicate);

        // Verify: (US, 20250725) should match
        assertThat(predicate.test(row(Lists.newArrayList("US", "20250725")))).isTrue();

        // Verify: (US, 20250726) should NOT match (not less than)
        assertThat(predicate.test(partition)).isFalse();

        // Verify: (EU, 20250725) should NOT match (region mismatch)
        assertThat(predicate.test(row(Lists.newArrayList("EU", "20250725")))).isFalse();
    }

    @Test
    public void testCreateGroupChainPredicateWithCombinedWithLinearForLessOrEqual() {
        // Correct pattern: OR(groupChainPredicate(lessThan), linearPredicate(equal))
        // to achieve partition <= target with group exact match.
        // This is the pattern used in planChainInGroup.
        RowType rowType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("EU", "20250801"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        1,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v)));
        predicates.add(
                ChainTableUtils.createLinearPredicate(
                        partition, converter, (Integer i, Object v) -> builder.equal(i, v)));
        Predicate combined = PredicateBuilder.or(predicates);

        // Exact match → match (via linearPredicate)
        assertThat(combined.test(partition)).isTrue();

        // Earlier date, same region → match (via groupChainPredicate)
        assertThat(combined.test(row(Lists.newArrayList("EU", "20250731")))).isTrue();

        // Later date → no match
        assertThat(combined.test(row(Lists.newArrayList("EU", "20250802")))).isFalse();

        // Different region → no match
        assertThat(combined.test(row(Lists.newArrayList("US", "20250801")))).isFalse();
    }

    @Test
    public void testCreateGroupChainPredicateWithOneGroupTwoChainLessThan() {
        // partition: (region, dt, hour), groupFieldCount=1
        // Expected: region=equal AND triangular(dt, hour) with lessThan
        //   → region='US' AND (dt < '20250810' OR (dt = '20250810' AND hour < '23'))
        RowType rowType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("US", "20250810", "23"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate =
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        1,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v));

        // (US, 20250809, 23) → dt < 20250810 → match
        assertThat(predicate.test(row(Lists.newArrayList("US", "20250809", "23")))).isTrue();

        // (US, 20250810, 22) → dt=20250810 AND hour<23 → match
        assertThat(predicate.test(row(Lists.newArrayList("US", "20250810", "22")))).isTrue();

        // (US, 20250810, 23) → not less than → no match
        assertThat(predicate.test(partition)).isFalse();

        // (EU, 20250809, 23) → region mismatch → no match
        assertThat(predicate.test(row(Lists.newArrayList("EU", "20250809", "23")))).isFalse();
    }

    @Test
    public void testCreateGroupChainPredicateWithOneGroupTwoChainLessOrEqual() {
        // partition: (region, dt, hour), groupFieldCount=1
        // Combine groupChainPredicate(lessThan) OR linearPredicate(equal)
        // to achieve <= semantics with correct multi-chain-field handling.
        RowType rowType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("US", "20250810", "14"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        1,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v)));
        predicates.add(
                ChainTableUtils.createLinearPredicate(
                        partition, converter, (Integer i, Object v) -> builder.equal(i, v)));
        Predicate combined = PredicateBuilder.or(predicates);

        // (US, 20250810, 14) → exact match → match (via linear)
        assertThat(combined.test(partition)).isTrue();

        // (US, 20250810, 13) → same dt, hour < 14 → match (via groupChain)
        assertThat(combined.test(row(Lists.newArrayList("US", "20250810", "13")))).isTrue();

        // (US, 20250809, 23) → dt < 20250810 → match (via groupChain)
        assertThat(combined.test(row(Lists.newArrayList("US", "20250809", "23")))).isTrue();

        // (US, 20250810, 15) → same dt, hour > 14, no exact match → no match
        assertThat(combined.test(row(Lists.newArrayList("US", "20250810", "15")))).isFalse();

        // (US, 20250811, 00) → dt > 20250810 → no match
        assertThat(combined.test(row(Lists.newArrayList("US", "20250811", "00")))).isFalse();

        // (EU, 20250810, 14) → region mismatch → no match
        assertThat(combined.test(row(Lists.newArrayList("EU", "20250810", "14")))).isFalse();
    }

    @Test
    public void testCreateGroupChainPredicateWithZeroGroup() {
        // groupFieldCount=0 → same as createTriangularPredicate
        RowType rowType =
                RowType.builder()
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("20250810", "23"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate groupChainPredicate =
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        0,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v));

        Predicate triangularPredicate =
                ChainTableUtils.createTriangularPredicate(
                        partition,
                        converter,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v));

        // When groupFieldCount=0, createGroupChainPredicate should produce
        // the same result as createTriangularPredicate
        assertThat(groupChainPredicate).isEqualTo(triangularPredicate);
    }

    @Test
    public void testCreateGroupChainPredicateWithTwoGroupOneChain() {
        // partition: (country, city, date), groupFieldCount=2
        // Expected: country=equal AND city=equal AND date=lessThan
        RowType rowType =
                RowType.builder()
                        .field("country", DataTypes.STRING().notNull())
                        .field("city", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        BinaryRow partition = row(Lists.newArrayList("China", "Beijing", "20250801"));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate =
                ChainTableUtils.createGroupChainPredicate(
                        partition,
                        converter,
                        2,
                        (Integer i, Object v) -> builder.equal(i, v),
                        (Integer i, Object v) -> builder.lessThan(i, v));

        // (China, Beijing, 20250731) → match
        assertThat(predicate.test(row(Lists.newArrayList("China", "Beijing", "20250731"))))
                .isTrue();

        // (China, Beijing, 20250801) → not less than → no match
        assertThat(predicate.test(partition)).isFalse();

        // (China, Shanghai, 20250731) → city mismatch → no match
        assertThat(predicate.test(row(Lists.newArrayList("China", "Shanghai", "20250731"))))
                .isFalse();

        // (US, Beijing, 20250731) → country mismatch → no match
        assertThat(predicate.test(row(Lists.newArrayList("US", "Beijing", "20250731")))).isFalse();
    }
}
