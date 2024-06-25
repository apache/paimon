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

import org.apache.paimon.flink.utils.FlinkCalciteClasses;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/** convert sql to predicate. */
public class SimpleSqlPredicateConvertor {

    private final PredicateBuilder builder;
    private final RowType rowType;

    private final FlinkCalciteClasses calciteClasses;

    public SimpleSqlPredicateConvertor(RowType type) throws Exception {
        this.rowType = type;
        this.builder = new PredicateBuilder(type);
        this.calciteClasses = new FlinkCalciteClasses();
    }

    public Predicate convertSqlToPredicate(String whereSql) throws Exception {
        Object config =
                calciteClasses
                        .configDelegate()
                        .withLex(
                                calciteClasses.sqlParserDelegate().config(),
                                calciteClasses.lexDelegate().java());
        Object sqlParser = calciteClasses.sqlParserDelegate().create(whereSql, config);
        Object sqlBasicCall = calciteClasses.sqlParserDelegate().parseExpression(sqlParser);
        return convert(sqlBasicCall);
    }

    public Predicate convert(Object sqlBasicCall) throws Exception {
        Object operator = calciteClasses.sqlBasicCallDelegate().getOperator(sqlBasicCall);
        Object kind = calciteClasses.sqlOperatorDelegate().getKind(operator);

        if (calciteClasses.sqlOperatorDelegate().instanceOfSqlBinaryOperator(operator)) {
            List<?> operandList =
                    calciteClasses.sqlBasicCallDelegate().getOperandList(sqlBasicCall);
            Object left = operandList.get(0);
            Object right = operandList.get(1);
            if (kind == calciteClasses.sqlKindDelegate().or()) {
                return PredicateBuilder.or(convert(left), convert(right));
            } else if (kind == calciteClasses.sqlKindDelegate().and()) {
                return PredicateBuilder.and(convert(left), convert(right));
            } else if (kind == calciteClasses.sqlKindDelegate().equals()) {
                return visitBiFunction(left, right, builder::equal, builder::equal);
            } else if (kind == calciteClasses.sqlKindDelegate().notEquals()) {
                return visitBiFunction(left, right, builder::notEqual, builder::notEqual);
            } else if (kind == calciteClasses.sqlKindDelegate().lessThan()) {
                return visitBiFunction(left, right, builder::lessThan, builder::greaterThan);
            } else if (kind == calciteClasses.sqlKindDelegate().lessThanOrEqual()) {
                return visitBiFunction(left, right, builder::lessOrEqual, builder::greaterOrEqual);
            } else if (kind == calciteClasses.sqlKindDelegate().greaterThan()) {
                return visitBiFunction(left, right, builder::greaterThan, builder::lessThan);
            } else if (kind == calciteClasses.sqlKindDelegate().greaterThanOrEqual()) {
                return visitBiFunction(left, right, builder::greaterOrEqual, builder::lessOrEqual);
            } else if (kind == calciteClasses.sqlKindDelegate().in()) {
                int index = getFieldIndex(left.toString());
                List<?> elementslist = calciteClasses.sqlNodeListDelegate().getList(right);

                List<Object> list = new ArrayList<>();
                for (Object sqlNode : elementslist) {
                    Object literal =
                            TypeUtils.castFromString(
                                    calciteClasses.sqlLiteralDelegate().toValue(sqlNode),
                                    rowType.getFieldTypes().get(index));
                    list.add(literal);
                }
                return builder.in(index, list);
            }
        } else if (calciteClasses.sqlOperatorDelegate().instanceOfSqlPostfixOperator(operator)) {
            Object child =
                    calciteClasses.sqlBasicCallDelegate().getOperandList(sqlBasicCall).get(0);
            if (kind == calciteClasses.sqlKindDelegate().isNull()) {
                String field = String.valueOf(child);
                return builder.isNull(getFieldIndex(field));
            } else if (kind == calciteClasses.sqlKindDelegate().isNotNull()) {
                String field = String.valueOf(child);
                return builder.isNotNull(getFieldIndex(field));
            }
        } else if (calciteClasses.sqlOperatorDelegate().instanceOfSqlPrefixOperator(operator)) {
            if (kind == calciteClasses.sqlKindDelegate().not()) {
                return convert(
                                calciteClasses
                                        .sqlBasicCallDelegate()
                                        .getOperandList(sqlBasicCall)
                                        .get(0))
                        .negate()
                        .get();
            }
        }

        throw new UnsupportedOperationException(String.format("%s not been supported.", kind));
    }

    public Predicate visitBiFunction(
            Object left,
            Object right,
            BiFunction<Integer, Object, Predicate> visitLeft,
            BiFunction<Integer, Object, Predicate> visitRight)
            throws Exception {
        if (calciteClasses.sqlIndentifierDelegate().instanceOfSqlIdentifier(left)
                && calciteClasses.sqlLiteralDelegate().instanceOfSqlLiteral(right)) {
            int index = getFieldIndex(String.valueOf(left));
            String value = calciteClasses.sqlLiteralDelegate().toValue(right);
            DataType type = rowType.getFieldTypes().get(index);
            return visitLeft.apply(index, TypeUtils.castFromString(value, type));
        } else if (calciteClasses.sqlIndentifierDelegate().instanceOfSqlIdentifier(right)
                && calciteClasses.sqlLiteralDelegate().instanceOfSqlLiteral(left)) {
            int index = getFieldIndex(right.toString());
            return visitRight.apply(
                    index,
                    TypeUtils.castFromString(
                            calciteClasses.sqlLiteralDelegate().toValue(left),
                            rowType.getFieldTypes().get(index)));
        }

        throw new UnsupportedOperationException(
                String.format("%s or %s not been supported.", left, right));
    }

    public int getFieldIndex(String field) {
        int index = builder.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(String.format("Field `%s` not found", field));
        }
        return index;
    }
}
