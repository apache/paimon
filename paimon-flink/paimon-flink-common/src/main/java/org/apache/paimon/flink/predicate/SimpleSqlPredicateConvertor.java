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

import org.apache.paimon.flink.utils.CalciteModule3;
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

    private CalciteModule3 calciteModule3;

    public SimpleSqlPredicateConvertor(RowType type) throws Exception {
        this.rowType = type;
        this.builder = new PredicateBuilder(type);
        calciteModule3 = new CalciteModule3();
    }

    public Predicate convertSqlToPredicate(String whereSql) throws Exception {
        Object config =
                calciteModule3
                        .configDelegate()
                        .withUnquotedCasing(
                                calciteModule3.sqlParserDelegate().config(),
                                calciteModule3.casingDelegate().unchanged());
        Object sqlParser = calciteModule3.sqlParserDelegate().create(whereSql, config);
        Object sqlBasicCall = calciteModule3.sqlParserDelegate().parseExpression(sqlParser);
        return convert(sqlBasicCall);
    }

    public Predicate convert(Object sqlBasicCall) throws Exception {
        Object operator = calciteModule3.sqlBasicCallDelegate().getOperator(sqlBasicCall);
        Object kind = calciteModule3.sqlOperatorDelegate().getKind(operator);

        if (calciteModule3.sqlOperatorDelegate().instanceOfSqlBinaryOperator(operator)) {
            List<?> operandList =
                    calciteModule3.sqlBasicCallDelegate().getOperandList(sqlBasicCall);
            Object left = operandList.get(0);
            Object right = operandList.get(1);
            if (kind == calciteModule3.sqlKindDelegate().or()) {
                return PredicateBuilder.or(convert(left), convert(right));
            } else if (kind == calciteModule3.sqlKindDelegate().and()) {
                return PredicateBuilder.and(convert(left), convert(right));
            } else if (kind == calciteModule3.sqlKindDelegate().equals()) {
                return visitBiFunction(left, right, builder::equal, builder::equal);
            } else if (kind == calciteModule3.sqlKindDelegate().notEquals()) {
                return visitBiFunction(left, right, builder::notEqual, builder::notEqual);
            } else if (kind == calciteModule3.sqlKindDelegate().lessThan()) {
                return visitBiFunction(left, right, builder::lessThan, builder::greaterThan);
            } else if (kind == calciteModule3.sqlKindDelegate().lessThanOrEqual()) {
                return visitBiFunction(left, right, builder::lessOrEqual, builder::greaterOrEqual);
            } else if (kind == calciteModule3.sqlKindDelegate().greaterThan()) {
                return visitBiFunction(left, right, builder::greaterThan, builder::lessThan);
            } else if (kind == calciteModule3.sqlKindDelegate().greaterThanOrEqual()) {
                return visitBiFunction(left, right, builder::greaterOrEqual, builder::lessOrEqual);
            } else if (kind == calciteModule3.sqlKindDelegate().in()) {
                int index = getfieldIndex(left.toString());
                List<?> elementslist = calciteModule3.sqlNodeListDelegate().getList(right);

                List<Object> list = new ArrayList<>();
                for (Object sqlNode : elementslist) {
                    Object literal =
                            TypeUtils.castFromString(
                                    calciteModule3.sqlLiteralDelegate().toValue(sqlNode),
                                    rowType.getFieldTypes().get(index));
                    list.add(literal);
                }
                return builder.in(index, list);
            }
        } else if (calciteModule3.sqlOperatorDelegate().instanceOfSqlPostfixOperator(operator)) {
            Object child =
                    calciteModule3.sqlBasicCallDelegate().getOperandList(sqlBasicCall).get(0);
            if (kind == calciteModule3.sqlKindDelegate().isNull()) {
                String field = String.valueOf(child);
                return builder.isNull(getfieldIndex(field));
            } else if (kind == calciteModule3.sqlKindDelegate().isNotNull()) {
                String field = String.valueOf(child);
                return builder.isNotNull(getfieldIndex(field));
            }
        } else if (calciteModule3.sqlOperatorDelegate().instanceOfSqlPrefixOperator(operator)) {
            if (kind == calciteModule3.sqlKindDelegate().not()) {
                return convert(
                                calciteModule3
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
        if (calciteModule3.sqlIndentifierDelegate().isInstanceOfSqlIdentifier(left)
                && calciteModule3.sqlLiteralDelegate().instanceOfSqlLiteral(right)) {
            int index = getfieldIndex(String.valueOf(left));
            String value = calciteModule3.sqlLiteralDelegate().toValue(right);
            DataType type = rowType.getFieldTypes().get(index);
            return visitLeft.apply(index, TypeUtils.castFromString(value, type));
        } else if (calciteModule3.sqlIndentifierDelegate().isInstanceOfSqlIdentifier(right)
                && calciteModule3.sqlLiteralDelegate().instanceOfSqlLiteral(left)) {
            int index = getfieldIndex(right.toString());
            return visitRight.apply(
                    index,
                    TypeUtils.castFromString(
                            calciteModule3.sqlLiteralDelegate().toValue(left),
                            rowType.getFieldTypes().get(index)));
        }

        throw new UnsupportedOperationException(
                String.format("%s or %s not been supported.", left, right));
    }

    public int getfieldIndex(String field) {
        int index = builder.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(String.format("Field `%s` not found", field));
        }
        return index;
    }
}
