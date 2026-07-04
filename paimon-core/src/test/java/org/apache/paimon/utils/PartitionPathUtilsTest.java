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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.utils.PartitionPathUtils.mightMatch;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionPathUtils#mightMatch}. */
class PartitionPathUtilsTest {

    private final RowType partitionType =
            RowType.builder()
                    .field("year", DataTypes.INT())
                    .field("month", DataTypes.INT())
                    .build();
    private final PredicateBuilder builder = new PredicateBuilder(partitionType);

    @Test
    void testNullPredicate() {
        assertThat(mightMatch(null, 0, 0, GenericRow.of(2024, 5))).isTrue();
    }

    @Test
    void testBoundLeaf() {
        Predicate p = builder.equal(0, 2024);
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2024, null))).isTrue();
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2023, null))).isFalse();
    }

    @Test
    void testLeafBeyondMaxIdxIsPossiblyTrue() {
        // month leaf (idx 1), but only year (idx 0) is bound -> cannot decide -> possibly true
        Predicate p = builder.equal(1, 12);
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2024, null))).isTrue();
    }

    @Test
    void testLeafBelowMinIdxIsPossiblyTrue() {
        // year leaf (idx 0) belongs to the scan-path equality prefix (minIdx=1): treated as already
        // satisfied and never read, even if values[0] does not match.
        Predicate p = builder.equal(0, 2024);
        assertThat(mightMatch(p, 1, 1, GenericRow.of(2023, 5))).isTrue();
    }

    @Test
    void testOrShortCircuit() {
        Predicate p = PredicateBuilder.or(builder.equal(0, 2024), builder.equal(0, 2025));
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2025, null))).isTrue();
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2023, null))).isFalse();
    }

    @Test
    void testAndShortCircuit() {
        Predicate p = PredicateBuilder.and(builder.equal(0, 2024), builder.equal(1, 6));
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2024, 6))).isTrue();
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2024, 7))).isFalse();
        // month not yet known (maxIdx=0) -> And not provably false -> possibly true
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2024, null))).isTrue();
        // year already false -> prune regardless of month
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2023, null))).isFalse();
    }

    @Test
    void testNestedCrossFieldOr() {
        // (year = 2024 AND month < 6) OR (year = 2025 AND month >= 6)
        Predicate p =
                PredicateBuilder.or(
                        PredicateBuilder.and(builder.equal(0, 2024), builder.lessThan(1, 6)),
                        PredicateBuilder.and(builder.equal(0, 2025), builder.greaterOrEqual(1, 6)));

        // year level (only idx 0 bound): keep 2024 and 2025, prune others
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2024, null))).isTrue();
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2025, null))).isTrue();
        assertThat(mightMatch(p, 0, 0, GenericRow.of(2023, null))).isFalse();

        // month level (both bound): exact
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2024, 5))).isTrue();
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2024, 6))).isFalse();
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2025, 6))).isTrue();
        assertThat(mightMatch(p, 0, 1, GenericRow.of(2025, 5))).isFalse();
    }
}
