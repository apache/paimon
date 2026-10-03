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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionPredicateHelper}. */
public class PartitionPredicateHelperTest {

    private static final RowType PARTITION_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"dt", "region"});
    private static final List<String> PARTITION_KEYS = Arrays.asList("dt", "region");
    private static final String DEFAULT_PARTITION = "__DEFAULT_PARTITION__";

    private final PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

    @Test
    public void testPartitionsTableFilterEqualAndIn() {
        Predicate filter = partitionsTableFilter(builder.equal(0, str("dt=20260410/region=hz")));
        assertThat(filter.test(GenericRow.of(20260410, str("hz")))).isTrue();
        assertThat(filter.test(GenericRow.of(20260410, str("sh")))).isFalse();

        filter =
                partitionsTableFilter(
                        builder.in(
                                0,
                                Arrays.asList(
                                        str("dt=1/region=hz"),
                                        str("dt=2/region=" + DEFAULT_PARTITION))));
        assertThat(filter.test(GenericRow.of(1, str("hz")))).isTrue();
        assertThat(filter.test(GenericRow.of(2, null))).isTrue();
        assertThat(filter.test(GenericRow.of(2, str("hz")))).isFalse();

        // more than 20 literals stay a single IN leaf
        List<Object> literals = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            literals.add(str("dt=" + i + "/region=hz"));
        }
        filter = partitionsTableFilter(builder.in(0, literals));
        assertThat(filter.test(GenericRow.of(24, str("hz")))).isTrue();
        assertThat(filter.test(GenericRow.of(25, str("hz")))).isFalse();

        // conjuncts on other columns are left to the row filter
        filter =
                partitionsTableFilter(
                        PredicateBuilder.and(
                                builder.greaterThan(1, 0L),
                                builder.equal(0, str("dt=1/region=hz"))));
        assertThat(filter.test(GenericRow.of(1, str("hz")))).isTrue();
        assertThat(filter.test(GenericRow.of(3, str("hz")))).isFalse();
    }

    @Test
    public void testPartitionsTableFilterSkipsUnsafePredicates() {
        assertThat(partitionsTableFilter(null)).isNull();
        assertThat(partitionsTableFilter(builder.greaterThan(1, 0L))).isNull();
        // range predicates compare the whole partition string, not the typed values
        assertThat(partitionsTableFilter(builder.lessThan(0, str("dt=2/region=hz")))).isNull();
        assertThat(
                        partitionsTableFilter(
                                PredicateBuilder.or(
                                        builder.equal(0, str("dt=1/region=hz")),
                                        builder.greaterThan(1, 0L))))
                .isNull();
        // values that do not render back to the literal
        assertThat(partitionsTableFilter(builder.equal(0, str("dt=01/region=hz")))).isNull();
        assertThat(partitionsTableFilter(builder.equal(0, str("dt=x/region=hz")))).isNull();
        // a value containing '/', a missing key and a different key order
        assertThat(partitionsTableFilter(builder.equal(0, str("dt=1/region=a/b")))).isNull();
        assertThat(partitionsTableFilter(builder.equal(0, str("dt=1")))).isNull();
        assertThat(partitionsTableFilter(builder.equal(0, str("region=hz/dt=1")))).isNull();
        // one unsafe IN literal disables pruning for all of them
        assertThat(
                        partitionsTableFilter(
                                builder.in(
                                        0,
                                        Arrays.asList(
                                                str("dt=1/region=hz"), str("dt=01/region=hz")))))
                .isNull();
    }

    private static Predicate partitionsTableFilter(Predicate predicate) {
        return PartitionPredicateHelper.partitionsTableFilter(
                predicate, PARTITION_KEYS, PARTITION_TYPE, DEFAULT_PARTITION);
    }

    private static BinaryString str(String s) {
        return BinaryString.fromString(s);
    }
}
