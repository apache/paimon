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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions.BucketFunctionType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.BucketSelector;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.PhysicalBucketPredicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests physical bucket filtering at manifest, sidecar and entry level. */
class BucketFilterTest {

    @Test
    void testPhysicalRangesDoNotRequireTotalBuckets() {
        BucketFilter filter = filter(PhysicalBucketPredicate.in(-2, 2, 8));
        for (Integer total : new Integer[] {null, -1, 0, 4, 10}) {
            assertThat(filter.mayContain(manifest(3, 7, total))).isFalse();
            assertThat(filter.mayContain(manifest(-2, 1, total))).isTrue();
            assertThat(filter.mayContain(manifest(2, 8, total))).isTrue();
            assertThat(filter.mayContain(manifest(8, 9, total))).isTrue();
        }
        assertThat(filter.mayContain(-2, -1)).isTrue();
        assertThat(filter.mayContain(2, 4)).isTrue();
        assertThat(filter.mayContain(5, 10)).isFalse();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, -2, -1)).isTrue();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 2, 10)).isTrue();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 5, 10)).isFalse();
    }

    @Test
    void testUnknownOrInvalidStatisticsAreKept() {
        BucketFilter filter = filter(PhysicalBucketPredicate.none());
        assertThat(filter.mayContain(manifest(null, null, null))).isTrue();
        assertThat(filter.mayContain(manifest(null, 4, 10))).isTrue();
        assertThat(filter.mayContain(manifest(0, null, 10))).isTrue();
        assertThat(filter.mayContain(manifest(4, 0, 10))).isTrue();
        assertThat(filter.mayContain(manifest(0, 4, null))).isFalse();
    }

    @Test
    void testOrdinaryFilterStillWorksWithoutRangePruning() {
        BucketFilter filter = new BucketFilter(false, null, bucket -> bucket == 2, null);
        assertThat(filter.mayContain(manifest(3, 7, 10))).isTrue();
        assertThat(filter.mayContain(3, 10)).isFalse();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 2, 10)).isTrue();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 3, 10)).isFalse();
    }

    @Test
    void testSpecifiedAndRealBucketsRemainConstraints() {
        BucketFilter filter = new BucketFilter(true, 2, PhysicalBucketPredicate.in(-2, 2, 8), null);
        assertThat(filter.mayContain(manifest(-2, -2, 10))).isFalse();
        assertThat(filter.mayContain(manifest(8, 8, 10))).isFalse();
        assertThat(filter.mayContain(manifest(2, 2, 10))).isTrue();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, -2, 10)).isFalse();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 8, 10)).isFalse();
        assertThat(filter.test(BinaryRow.EMPTY_ROW, 2, 10)).isTrue();
        BucketFilter real = new BucketFilter(true, null, PhysicalBucketPredicate.all(), null);
        assertThat(real.mayContain(-2, -1)).isFalse();
        assertThat(real.mayContain(manifest(-2, -1, null))).isFalse();
    }

    @Test
    void testPhysicalFilterCombinesWithBucketSelector() {
        RowType keyType = RowType.of(DataTypes.INT());
        BucketSelector selector =
                new BucketSelector(
                        new PredicateBuilder(keyType).equal(0, 5),
                        BucketFunctionType.DEFAULT,
                        keyType,
                        RowType.of(),
                        keyType);
        for (int total : new int[] {4, 10}) {
            int selected = 0;
            while (!selector.test(BinaryRow.EMPTY_ROW, selected, total)) {
                selected++;
            }
            int other = (selected + 1) % total;
            BucketFilter both =
                    new BucketFilter(
                            false, null, PhysicalBucketPredicate.in(selected, other), selector);
            assertThat(both.mayContain(manifest(selected, selected, total))).isTrue();
            assertThat(both.mayContain(manifest(other, other, total))).isFalse();
            assertThat(both.mayContain(other, total)).isFalse();
            assertThat(both.test(BinaryRow.EMPTY_ROW, other, total)).isFalse();
            assertThat(both.test(BinaryRow.EMPTY_ROW, selected, total)).isTrue();

            BucketFilter disjoint =
                    new BucketFilter(false, null, PhysicalBucketPredicate.equal(other), selector);
            assertThat(disjoint.mayContain(manifest(selected, selected, total))).isFalse();
            // Unknown totals disable only the total-aware constraint, not the physical one.
            assertThat(disjoint.mayContain(manifest(selected, selected, null))).isFalse();
            assertThat(disjoint.mayContain(manifest(other, other, null))).isTrue();
            assertThat(disjoint.mayContain(manifest(-2, -1, null))).isFalse();
        }
    }

    @Test
    void testPartitionDependentFilterIsNotEvaluatedOnSidecars() {
        BucketFilter filter =
                new BucketFilter(
                        false,
                        null,
                        PhysicalBucketPredicate.equal(2),
                        (partition, bucket, total) -> partition.getInt(0) == 1);
        assertThat(filter.mayContain(2, 10)).isTrue();
        assertThat(filter.mayContain(3, 10)).isFalse();
        assertThat(filter.mayContain(manifest(2, 2, 10))).isTrue();
        assertThat(filter.mayContain(manifest(3, 3, 10))).isFalse();
    }

    private static BucketFilter filter(PhysicalBucketPredicate predicate) {
        return new BucketFilter(false, null, predicate, null);
    }

    private static ManifestFileMeta manifest(Integer min, Integer max, Integer total) {
        return new ManifestFileMeta(
                "manifest", 1, 1, 1, EMPTY_STATS, 0, min, max, null, null, null, null, total, null);
    }
}
