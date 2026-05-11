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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions.BucketFunctionType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BucketSelector}. */
public class BucketSelectorTest {

    private static final int NUM_BUCKETS = 10;

    // ========================== Single bucket key, non-partitioned ==========================

    @Test
    public void testEqualPredicate() {
        // k = 5 => should select exactly one bucket
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = pb.equal(0, 5);
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testEqualAndRangePredicate() {
        // k = 5 AND k < 100 => should still select bucket for k=5
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 5), pb.lessThan(0, 100));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testEqualAndInWithOverlap() {
        // k = 5 AND k IN (5, 10) => intersection is {5}, should select one bucket
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 5), pb.in(0, Arrays.asList(5, 10)));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testInAndInWithOverlap() {
        // k IN (1, 5) AND k IN (5, 10) => intersection is {5}, should select one bucket
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate =
                PredicateBuilder.and(pb.in(0, Arrays.asList(1, 5)), pb.in(0, Arrays.asList(5, 10)));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testRedundantEquals() {
        // k = 5 AND k = 5 => redundant, should still select one bucket
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 5), pb.equal(0, 5));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testContradictoryEquals() {
        // k = 5 AND k = 10 => empty intersection, no bucket can match
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 5), pb.equal(0, 10));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        // Empty intersection => Optional.empty() => orElse(true) => all buckets pass
        // (conservative: no filtering when we can't determine)
        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(NUM_BUCKETS);
    }

    @Test
    public void testRangeOnlyFallsBackToFullScan() {
        // k < 100 => no Equal/In to extract, full scan
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        RowType partType = RowType.of();
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = pb.lessThan(0, 100);
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(NUM_BUCKETS);
    }

    // ========================== Multi-field bucket key ==========================

    @Test
    public void testMultiFieldBucketKey() {
        // k1 = 5 AND k2 = 10 => one combination, one bucket
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "k1", DataTypes.INT()),
                        DataTypes.FIELD(1, "k2", DataTypes.INT()));
        RowType partType = RowType.of();
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 5), pb.equal(1, 10));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, rowType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    @Test
    public void testMultiFieldWithIntersection() {
        // k1 IN (1, 5) AND k1 IN (5, 10) AND k2 = 3 => k1={5}, k2={3} => one combination
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "k1", DataTypes.INT()),
                        DataTypes.FIELD(1, "k2", DataTypes.INT()));
        RowType partType = RowType.of();
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate =
                PredicateBuilder.and(
                        pb.in(0, Arrays.asList(1, 5)),
                        pb.in(0, Arrays.asList(5, 10)),
                        pb.equal(1, 3));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, rowType);

        Set<Integer> selected = selectedBuckets(selector, BinaryRow.EMPTY_ROW, NUM_BUCKETS);
        assertThat(selected).hasSize(1);
    }

    // ========================== Partitioned table ==========================

    @Test
    public void testPartitionedTableWithBucketFilter() {
        // Table: (pt INT, k INT), partition: (pt), bucket key: (k)
        // Predicate: pt = 1 AND k = 5
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "k", DataTypes.INT()));
        RowType partType = DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.INT()));
        RowType bucketKeyType = DataTypes.ROW(DataTypes.FIELD(0, "k", DataTypes.INT()));
        PredicateBuilder pb = new PredicateBuilder(rowType);

        Predicate predicate = PredicateBuilder.and(pb.equal(0, 1), pb.equal(1, 5));
        BucketSelector selector =
                new BucketSelector(
                        predicate, BucketFunctionType.DEFAULT, rowType, partType, bucketKeyType);

        // For partition pt=1 (matching), bucket filtering should work
        BinaryRow partition1 =
                new org.apache.paimon.data.serializer.InternalRowSerializer(partType)
                        .toBinaryRow(org.apache.paimon.data.GenericRow.of(1))
                        .copy();
        Set<Integer> selected1 = selectedBuckets(selector, partition1, NUM_BUCKETS);
        assertThat(selected1).hasSize(1);

        // For partition pt=2 (not matching), predicate becomes AlwaysFalse
        // => no bucket key values extracted => full scan (conservative)
        BinaryRow partition2 =
                new org.apache.paimon.data.serializer.InternalRowSerializer(partType)
                        .toBinaryRow(org.apache.paimon.data.GenericRow.of(2))
                        .copy();
        Set<Integer> selected2 = selectedBuckets(selector, partition2, NUM_BUCKETS);
        assertThat(selected2).hasSize(NUM_BUCKETS);
    }

    // ========================== Helpers ==========================

    private static Set<Integer> selectedBuckets(
            BucketSelector selector, BinaryRow partition, int numBuckets) {
        Set<Integer> selected = new HashSet<>();
        for (int b = 0; b < numBuckets; b++) {
            if (selector.test(partition, b, numBuckets)) {
                selected.add(b);
            }
        }
        return selected;
    }
}
