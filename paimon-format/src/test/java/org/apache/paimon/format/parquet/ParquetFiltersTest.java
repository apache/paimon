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

package org.apache.paimon.format.parquet;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetFiltersTest {

    @Test
    public void testLong() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));

        test(builder.isNull(0), "eq(long1, null)", true);

        test(builder.isNotNull(0), "noteq(long1, null)", true);

        test(builder.lessThan(0, 5L), "lt(long1, 5)", true);

        test(builder.greaterThan(0, 5L), "gt(long1, 5)", true);

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                "or(or(eq(long1, 1), eq(long1, 2)), eq(long1, 3))",
                true);

        test(builder.between(0, 1L, 3L), "and(gteq(long1, 1), lteq(long1, 3))", true);

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                "and(and(noteq(long1, 1), noteq(long1, 2)), noteq(long1, 3))",
                true);

        test(
                builder.in(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                "",
                false);

        test(
                builder.notIn(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                "",
                false);
    }

    @Test
    public void testString() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "string1", new VarCharType()))));
        test(builder.isNull(0), "eq(string1, null)", true);

        test(builder.isNotNull(0), "noteq(string1, null)", true);

        test(
                builder.in(0, Arrays.asList("1", "2", "3")),
                "or(or(eq(string1, Binary{\"1\"}), eq(string1, Binary{\"2\"})), eq(string1, Binary{\"3\"}))",
                true);
        test(
                builder.notIn(0, Arrays.asList("1", "2", "3")),
                "and(and(noteq(string1, Binary{\"1\"}), noteq(string1, Binary{\"2\"})), noteq(string1, Binary{\"3\"}))",
                true);
    }

    private void test(Predicate predicate, String expected, boolean canPushDown) {
        FilterCompat.Filter filter = ParquetFilters.convert(PredicateBuilder.splitAnd(predicate));
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate().toString()).isEqualTo(expected);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
    }
}
