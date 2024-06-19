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

package org.apache.paimon.flink.predicate;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/** test for {@link SimpleSqlPredicateConvertor} . */
class SimpleSqlPredicateConvertorTest {
    RowType rowType;
    PredicateBuilder predicateBuilder;

    SimpleSqlPredicateConvertor simpleSqlPredicateConvertor;

    @BeforeEach
    public void init() throws Exception {
        rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.DATE())
                        .build();
        predicateBuilder = new PredicateBuilder(rowType);
        simpleSqlPredicateConvertor = new SimpleSqlPredicateConvertor(rowType);
    }

    @Test
    public void testEqual() throws Exception {
        {
            //
            // org.apache.calcite.sql.parser.ImmutableSqlParser$Config.withUnquotedCasing(org.apache.calcite.avatica.util.Casing)
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a ='1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.equal(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate(" '2024-05-25' = c");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.equal(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testNotEqual() throws Exception {
        {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a <>'1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.notEqual(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate("'2024-05-25' <> c");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.notEqual(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testLessThan() throws Exception {
        {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a <'1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.lessThan(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate("'2024-05-25' <c ");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.greaterThan(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testLessThanOrEqual() throws Exception {
        {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a <='1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.lessOrEqual(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate("'2024-05-25' <= c");
            Assertions.assertThat(predicate)
                    .isEqualTo(
                            predicateBuilder.greaterOrEqual(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testGreatThan() throws Exception {
        {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a >'1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.greaterThan(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate("'2024-05-25' > c");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.lessThan(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testGreatEqual() throws Exception {
        {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a >='1'");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.greaterOrEqual(predicateBuilder.indexOf("a"), 1));
        }

        {
            Predicate predicate =
                    simpleSqlPredicateConvertor.convertSqlToPredicate(" '2024-05-25' >= c");
            Assertions.assertThat(predicate)
                    .isEqualTo(predicateBuilder.lessOrEqual(predicateBuilder.indexOf("c"), 19868));
        }
    }

    @Test
    public void testIN() throws Exception {
        Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a in ('1','2')");
        List<Object> elements = Lists.newArrayList(1, 2);
        Assertions.assertThat(predicate)
                .isEqualTo(predicateBuilder.in(predicateBuilder.indexOf("a"), elements));
    }

    @Test
    public void testIsNull() throws Exception {
        Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a is null ");
        Assertions.assertThat(predicate)
                .isEqualTo(predicateBuilder.isNull(predicateBuilder.indexOf("a")));
    }

    @Test
    public void testIsNotNull() throws Exception {
        Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate("a is not  null ");
        Assertions.assertThat(predicate)
                .isEqualTo(predicateBuilder.isNotNull(predicateBuilder.indexOf("a")));
    }

    @Test
    public void testAnd() throws Exception {
        Predicate actual =
                simpleSqlPredicateConvertor.convertSqlToPredicate("a is not null and c is null");
        Predicate expected =
                PredicateBuilder.and(
                        predicateBuilder.isNotNull(predicateBuilder.indexOf("a")),
                        predicateBuilder.isNull(predicateBuilder.indexOf("c")));
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testOr() throws Exception {
        Predicate actual =
                simpleSqlPredicateConvertor.convertSqlToPredicate("a is not  null or c is null ");
        Predicate expected =
                PredicateBuilder.or(
                        predicateBuilder.isNotNull(predicateBuilder.indexOf("a")),
                        predicateBuilder.isNull(predicateBuilder.indexOf("c")));
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testNOT() throws Exception {
        Predicate actual = simpleSqlPredicateConvertor.convertSqlToPredicate("not (a is null) ");
        Predicate expected = predicateBuilder.isNull(predicateBuilder.indexOf("a")).negate().get();
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFieldNoFound() {
        Assertions.assertThatThrownBy(
                        () -> simpleSqlPredicateConvertor.convertSqlToPredicate("f =1"))
                .hasMessage("Field `f` not found");
    }

    @Test
    public void testSqlNoSupport() {
        // function not supported
        Assertions.assertThatThrownBy(
                        () ->
                                simpleSqlPredicateConvertor.convertSqlToPredicate(
                                        "substring(f,0,1) =1"))
                .hasMessage("SUBSTRING(`f` FROM 0 FOR 1) or 1 not been supported.");
        // like not supported
        Assertions.assertThatThrownBy(
                        () -> simpleSqlPredicateConvertor.convertSqlToPredicate("b like 'x'"))
                .hasMessage("LIKE not been supported.");
    }
}
