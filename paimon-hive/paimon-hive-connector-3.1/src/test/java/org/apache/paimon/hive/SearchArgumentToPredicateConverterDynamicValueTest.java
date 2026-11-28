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

package org.apache.paimon.hive;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.LiteralDelegate;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl.PredicateLeafImpl;
import org.apache.hadoop.hive.ql.plan.DynamicValue.NoDynamicValuesException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for handling unavailable Hive dynamic values in the Hive 3.1 connector. */
public class SearchArgumentToPredicateConverterDynamicValueTest {

    private static final List<String> COLUMN_NAMES = Arrays.asList("f_int", "f_bigint");
    private static final List<DataType> COLUMN_TYPES =
            Arrays.asList(DataTypes.INT(), DataTypes.BIGINT());
    private static final PredicateBuilder BUILDER =
            new PredicateBuilder(
                    RowType.of(
                            COLUMN_TYPES.toArray(new DataType[0]),
                            COLUMN_NAMES.toArray(new String[0])));

    @Test
    public void testUnavailableDynamicValue() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startAnd()
                        .between("f_bigint", PredicateLeaf.Type.LONG, 100L, 200L)
                        .lessThanEquals("f_int", PredicateLeaf.Type.LONG, 10L)
                        .end()
                        .build();
        sarg.getLeaves()
                .set(
                        0,
                        new PredicateLeafImpl(
                                PredicateLeaf.Operator.BETWEEN,
                                PredicateLeaf.Type.LONG,
                                "f_bigint",
                                null,
                                Arrays.asList(
                                        new UnavailableDynamicValue(),
                                        new UnavailableDynamicValue())));

        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(sarg, COLUMN_NAMES, COLUMN_TYPES, null);
        Predicate actual = converter.convert().orElse(null);
        assertThat(actual).isEqualTo(BUILDER.lessOrEqual(0, 10));
    }

    private static class UnavailableDynamicValue implements LiteralDelegate {

        @Override
        public Object getLiteral() {
            throw new NoDynamicValuesException("DynamicValueRegistry not available");
        }

        @Override
        public String getId() {
            return "unavailable";
        }

        @Override
        public void setConf(Configuration conf) {}

        @Override
        public Configuration getConf() {
            return null;
        }
    }
}
