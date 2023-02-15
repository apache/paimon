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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.format.orc.filter.OrcFilters;
import org.apache.flink.table.store.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowType;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Tests for {@link OrcPredicateFunctionVisitor}. */
public class OrcFilterConverterTest {

    @Test
    public void testApplyPredicate() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));
        test(builder.isNull(0), new OrcFilters.IsNull("long1", PredicateLeaf.Type.LONG));
        test(
                builder.isNotNull(0),
                new OrcFilters.Not(new OrcFilters.IsNull("long1", PredicateLeaf.Type.LONG)));
        test(builder.equal(0, 10L), new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 10));
        test(
                builder.notEqual(0, 10L),
                new OrcFilters.Not(new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 10)));
        test(
                builder.lessThan(0, 10L),
                new OrcFilters.LessThan("long1", PredicateLeaf.Type.LONG, 10));
        test(
                builder.lessOrEqual(0, 10L),
                new OrcFilters.LessThanEquals("long1", PredicateLeaf.Type.LONG, 10));
        test(
                builder.greaterThan(0, 10L),
                new OrcFilters.Not(
                        new OrcFilters.LessThanEquals("long1", PredicateLeaf.Type.LONG, 10)));
        test(
                builder.greaterOrEqual(0, 10L),
                new OrcFilters.Not(new OrcFilters.LessThan("long1", PredicateLeaf.Type.LONG, 10)));

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                new OrcFilters.Or(
                        new OrcFilters.Or(
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 1),
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 2)),
                        new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 3)));

        test(
                builder.between(0, 1L, 3L),
                new OrcFilters.And(
                        new OrcFilters.Not(
                                new OrcFilters.LessThan("long1", PredicateLeaf.Type.LONG, 1)),
                        new OrcFilters.LessThanEquals("long1", PredicateLeaf.Type.LONG, 3)));

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                new OrcFilters.And(
                        new OrcFilters.And(
                                new OrcFilters.Not(
                                        new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 1)),
                                new OrcFilters.Not(
                                        new OrcFilters.Equals(
                                                "long1", PredicateLeaf.Type.LONG, 2))),
                        new OrcFilters.Not(
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 3))));

        assertThat(
                        builder.in(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(OrcPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();

        assertThat(
                        builder.notIn(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(OrcPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();
    }

    private void test(Predicate predicate, OrcFilters.Predicate orcPredicate) {
        assertThat(predicate.visit(OrcPredicateFunctionVisitor.VISITOR).get())
                .hasToString(orcPredicate.toString());
    }
}
