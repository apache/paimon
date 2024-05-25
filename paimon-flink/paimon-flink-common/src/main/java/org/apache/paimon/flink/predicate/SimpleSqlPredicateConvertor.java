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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.calcite.avatica.util.Casing.UNCHANGED;

/** convert sql to predicate. */
public class SimpleSqlPredicateConvertor {

    private final PredicateBuilder builder;
    private final RowType rowType;

    public SimpleSqlPredicateConvertor(RowType type) {
        this.rowType = type;
        this.builder = new PredicateBuilder(type);
    }

    public Predicate convertSqlToPredicate(String conditionSql) throws SqlParseException {
        SqlParser parser =
                SqlParser.create(conditionSql, SqlParser.config().withUnquotedCasing(UNCHANGED));
        SqlNode sqlNode = parser.parseExpression();
        return convert((SqlBasicCall) sqlNode);
    }

    public Predicate convert(SqlBasicCall sqlBasicCall) {
        SqlOperator operator = sqlBasicCall.getOperator();
        SqlKind kind = operator.getKind();
        if (operator instanceof SqlBinaryOperator) {
            List<SqlNode> operandList = sqlBasicCall.getOperandList();
            SqlNode left = operandList.get(0);
            SqlNode right = operandList.get(1);
            switch (kind) {
                case OR:
                    return PredicateBuilder.or(
                            convert((SqlBasicCall) left), convert((SqlBasicCall) right));
                case AND:
                    return PredicateBuilder.and(
                            convert((SqlBasicCall) left), convert((SqlBasicCall) right));
                case EQUALS:
                    return visitBiFunction(left, right, builder::equal, builder::equal);
                case NOT_EQUALS:
                    return visitBiFunction(left, right, builder::notEqual, builder::notEqual);
                case LESS_THAN:
                    return visitBiFunction(left, right, builder::lessThan, builder::greaterThan);
                case LESS_THAN_OR_EQUAL:
                    return visitBiFunction(
                            left, right, builder::lessOrEqual, builder::greaterOrEqual);
                case GREATER_THAN:
                    return visitBiFunction(left, right, builder::greaterThan, builder::lessThan);
                case GREATER_THAN_OR_EQUAL:
                    return visitBiFunction(
                            left, right, builder::greaterOrEqual, builder::lessOrEqual);
                case IN:
                    {
                        int index = getfieldIndex(left.toString());
                        SqlNodeList elementslist = (SqlNodeList) right;

                        List<Object> list = new ArrayList<>();
                        for (SqlNode sqlNode : elementslist) {
                            Object literal =
                                    TypeUtils.castFromString(
                                            ((SqlLiteral) sqlNode).toValue(),
                                            rowType.getFieldTypes().get(index));
                            list.add(literal);
                        }
                        return builder.in(index, list);
                    }
            }
        } else if (operator instanceof SqlPostfixOperator) {
            SqlNode child = sqlBasicCall.getOperandList().get(0);
            switch (kind) {
                case IS_NULL:
                    {
                        String field = String.valueOf(child);
                        return builder.isNull(getfieldIndex(field));
                    }
                case IS_NOT_NULL:
                    String field = String.valueOf(child);
                    return builder.isNotNull(getfieldIndex(field));
            }
        } else if (operator instanceof SqlPrefixOperator) {
            if (kind == SqlKind.NOT) {
                SqlBasicCall child = (SqlBasicCall) sqlBasicCall.getOperandList().get(0);
                return convert(child).negate().get();
            }
        }

        throw new UnsupportedOperationException(String.format("%s not been supported.", kind));
    }

    public Predicate visitBiFunction(
            SqlNode left,
            SqlNode right,
            BiFunction<Integer, Object, Predicate> visitLeft,
            BiFunction<Integer, Object, Predicate> visitRight) {
        if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
            int index = getfieldIndex(left.toString());
            String value = ((SqlLiteral) right).toValue();
            DataType type = rowType.getFieldTypes().get(index);
            return visitLeft.apply(index, TypeUtils.castFromString(value, type));
        } else if (right instanceof SqlIdentifier && left instanceof SqlLiteral) {
            int index = getfieldIndex(right.toString());
            return visitRight.apply(
                    index,
                    TypeUtils.castFromString(
                            ((SqlLiteral) left).toValue(), rowType.getFieldTypes().get(index)));
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
