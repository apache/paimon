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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.paimon.hive.SearchArgumentToPredicateConverter}. */
public class SearchArgumentToPredicateConverterTest {

    @Test
    public void testLiteral() {
        testLiteral(PredicateLeaf.Type.BOOLEAN, false, DataTypes.BOOLEAN(), false);
        testLiteral(PredicateLeaf.Type.LONG, 10L, DataTypes.TINYINT(), (byte) 10);
        testLiteral(PredicateLeaf.Type.LONG, 100L, DataTypes.SMALLINT(), (short) 100);
        testLiteral(PredicateLeaf.Type.LONG, 1000L, DataTypes.INT(), 1000);
        testLiteral(PredicateLeaf.Type.LONG, 10000L, DataTypes.BIGINT(), 10000L);
        testLiteral(PredicateLeaf.Type.FLOAT, 0.2, DataTypes.FLOAT(), 0.2f);
        testLiteral(PredicateLeaf.Type.FLOAT, 0.2, DataTypes.DOUBLE(), 0.2);
        testLiteral(
                PredicateLeaf.Type.DECIMAL,
                new HiveDecimalWritable(HiveDecimal.create("123456.789")),
                DataTypes.DECIMAL(9, 3),
                Decimal.fromBigDecimal(new BigDecimal("123456.789"), 9, 3));
        testLiteral(
                PredicateLeaf.Type.DECIMAL,
                new HiveDecimalWritable(HiveDecimal.create("123456789123456789.123456789")),
                DataTypes.DECIMAL(27, 9),
                Decimal.fromBigDecimal(new BigDecimal("123456789123456789.123456789"), 27, 9));
        testLiteral(
                PredicateLeaf.Type.STRING,
                "Paimon",
                DataTypes.STRING(),
                BinaryString.fromString("Paimon"));
        testLiteral(PredicateLeaf.Type.DATE, Date.valueOf("1971-01-11"), DataTypes.DATE(), 375);
        testLiteral(
                PredicateLeaf.Type.TIMESTAMP,
                java.sql.Timestamp.valueOf("2022-05-17 16:25:53"),
                DataTypes.TIMESTAMP(3),
                Timestamp.fromSQLTimestamp(java.sql.Timestamp.valueOf("2022-05-17 16:25:53")));
        testLiteral(
                PredicateLeaf.Type.TIMESTAMP,
                java.sql.Timestamp.valueOf("2022-05-17 16:25:53"),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                Timestamp.fromInstant(
                        java.sql.Timestamp.valueOf("2022-05-17 16:25:53").toInstant()));
    }

    private void testLiteral(
            PredicateLeaf.Type hiveType,
            Object hiveLiteral,
            DataType flinkType,
            Object flinkLiteral) {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg = builder.equals("a", hiveType, hiveLiteral).build();
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(
                        sarg,
                        Collections.singletonList("a"),
                        Collections.singletonList(flinkType),
                        null);

        Predicate expected =
                new PredicateBuilder(RowType.of(new DataType[] {flinkType}, new String[] {"a"}))
                        .equal(0, flinkLiteral);
        Predicate actual = converter.convert().orElse(null);
        assertThat(actual).isEqualTo(expected);
    }

    private static final List<String> COLUMN_NAMES = Arrays.asList("f_int", "f_bigint", "f_double");
    private static final List<DataType> COLUMN_TYPES =
            Arrays.asList(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE());
    private static final PredicateBuilder BUILDER =
            new PredicateBuilder(
                    RowType.of(
                            COLUMN_TYPES.toArray(new DataType[0]),
                            COLUMN_NAMES.toArray(new String[0])));

    @Test
    public void testEqual() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg = builder.equals("f_bigint", PredicateLeaf.Type.LONG, 100L).build();
        Predicate expected = BUILDER.equal(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotEqual() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot().equals("f_bigint", PredicateLeaf.Type.LONG, 100L).end().build();
        Predicate expected = BUILDER.notEqual(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testLessThan() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg = builder.lessThan("f_bigint", PredicateLeaf.Type.LONG, 100L).build();
        Predicate expected = BUILDER.lessThan(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testGreaterOrEqual() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .lessThan("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .end()
                        .build();
        Predicate expected = BUILDER.greaterOrEqual(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testLessOrEqual() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.lessThanEquals("f_bigint", PredicateLeaf.Type.LONG, 100L).build();
        Predicate expected = BUILDER.lessOrEqual(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testGreaterThan() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .lessThanEquals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .end()
                        .build();
        Predicate expected = BUILDER.greaterThan(1, 100L);
        assertExpected(sarg, expected);
    }

    @Test
    public void testIn() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.in("f_bigint", PredicateLeaf.Type.LONG, 100L, 200L, 300L).build();
        Predicate expected = BUILDER.in(1, Arrays.asList(100L, 200L, 300L));
        assertExpected(sarg, expected);

        builder = SearchArgumentFactory.newBuilder();
        Object[] literals = new Object[30];
        for (int i = 0; i < literals.length; i++) {
            literals[i] = i * 100L;
        }
        sarg = builder.in("f_bigint", PredicateLeaf.Type.LONG, literals).build();
        expected = BUILDER.in(1, Arrays.asList(literals));
        assertExpected(sarg, expected);
    }

    @Test
    public void testInNull() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.in("f_bigint", PredicateLeaf.Type.LONG, 100L, null, 300L).build();
        Predicate expected = BUILDER.in(1, Arrays.asList(100L, null, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotIn() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .in("f_bigint", PredicateLeaf.Type.LONG, 100L, 200L, 300L)
                        .end()
                        .build();
        Predicate expected = BUILDER.notIn(1, Arrays.asList(100L, 200L, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotInNull() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .in("f_bigint", PredicateLeaf.Type.LONG, 100L, null, 300L)
                        .end()
                        .build();
        Predicate expected = BUILDER.notIn(1, Arrays.asList(100L, null, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testLargeIn() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        Object[] literals = new Object[30];
        literals[0] = null;
        for (int i = 1; i < literals.length; i++) {
            literals[i] = i * 100L;
        }
        SearchArgument sarg = builder.in("f_bigint", PredicateLeaf.Type.LONG, literals).build();
        Predicate expected = BUILDER.in(1, Arrays.asList(literals));
        assertExpected(sarg, expected);
    }

    @Test
    public void testLargeNotIn() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        Object[] literals = new Object[30];
        literals[0] = null;
        for (int i = 1; i < literals.length; i++) {
            literals[i] = i * 100L;
        }
        SearchArgument sarg =
                builder.startNot().in("f_bigint", PredicateLeaf.Type.LONG, literals).end().build();
        Predicate expected = BUILDER.notIn(1, Arrays.asList(literals));
        assertExpected(sarg, expected);
    }

    @Test
    public void testBetween() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.between("f_bigint", PredicateLeaf.Type.LONG, 100L, 200L).build();
        Predicate expected =
                PredicateBuilder.and(BUILDER.greaterOrEqual(1, 100L), BUILDER.lessOrEqual(1, 200L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotBetween() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .between("f_bigint", PredicateLeaf.Type.LONG, 100L, 200L)
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.or(BUILDER.lessThan(1, 100L), BUILDER.greaterThan(1, 200L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testIsNull() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg = builder.isNull("f_bigint", PredicateLeaf.Type.LONG).build();
        Predicate expected = BUILDER.isNull(1);
        assertExpected(sarg, expected);
    }

    @Test
    public void testIsNotNull() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot().isNull("f_bigint", PredicateLeaf.Type.LONG).end().build();
        Predicate expected = BUILDER.isNotNull(1);
        assertExpected(sarg, expected);
    }

    @Test
    public void testOr() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startOr()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 200L)
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 300L)
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.or(
                        BUILDER.equal(1, 100L), BUILDER.equal(1, 200L), BUILDER.equal(1, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotOr() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .startOr()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 200L)
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 300L)
                        .end()
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.and(
                        BUILDER.notEqual(1, 100L),
                        BUILDER.notEqual(1, 200L),
                        BUILDER.notEqual(1, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testAnd() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startAnd()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .end()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 200L)
                        .end()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 300L)
                        .end()
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.and(
                        BUILDER.notEqual(1, 100L),
                        BUILDER.notEqual(1, 200L),
                        BUILDER.notEqual(1, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testNotAnd() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startNot()
                        .startAnd()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .end()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 200L)
                        .end()
                        .startNot()
                        .equals("f_bigint", PredicateLeaf.Type.LONG, 300L)
                        .end()
                        .end()
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.or(
                        BUILDER.equal(1, 100L), BUILDER.equal(1, 200L), BUILDER.equal(1, 300L));
        assertExpected(sarg, expected);
    }

    @Test
    public void testUnsupported() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.nullSafeEquals("f_bigint", PredicateLeaf.Type.LONG, 100L).build();
        assertExpected(sarg, null);
    }

    @Test
    public void testKeepPartialResult() {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        SearchArgument sarg =
                builder.startAnd()
                        .startNot()
                        .nullSafeEquals("f_bigint", PredicateLeaf.Type.LONG, 100L)
                        .end()
                        .lessThanEquals("f_bigint", PredicateLeaf.Type.LONG, 200L)
                        .startNot()
                        .lessThan("f_bigint", PredicateLeaf.Type.LONG, 0L)
                        .end()
                        .end()
                        .build();
        Predicate expected =
                PredicateBuilder.and(BUILDER.lessOrEqual(1, 200L), BUILDER.greaterOrEqual(1, 0L));
        assertExpected(sarg, expected);
    }

    private void assertExpected(SearchArgument sarg, Predicate expected) {
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(sarg, COLUMN_NAMES, COLUMN_TYPES, null);
        Predicate actual = converter.convert().orElse(null);
        assertThat(actual).isEqualTo(expected);
    }
}
