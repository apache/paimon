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

package org.apache.paimon.format.orc.filter;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.types.DataType;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Convert {@link org.apache.paimon.predicate.Predicate} to {@link OrcFilters.Predicate} for orc.
 */
public class OrcPredicateFunctionVisitor
        implements FunctionVisitor<Optional<OrcFilters.Predicate>> {
    public static final OrcPredicateFunctionVisitor VISITOR = new OrcPredicateFunctionVisitor();

    private OrcPredicateFunctionVisitor() {}

    @Override
    public Optional<OrcFilters.Predicate> visitIsNull(FieldRef fieldRef) {
        PredicateLeaf.Type colType = toOrcType(fieldRef.type());
        if (colType == null) {
            return Optional.empty();
        }

        return Optional.of(new OrcFilters.IsNull(fieldRef.name(), colType));
    }

    @Override
    public Optional<OrcFilters.Predicate> visitIsNotNull(FieldRef fieldRef) {
        Optional<OrcFilters.Predicate> isNull = visitIsNull(fieldRef);
        return isNull.map(OrcFilters.Not::new);
    }

    @Override
    public Optional<OrcFilters.Predicate> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<OrcFilters.Predicate> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<OrcFilters.Predicate> visitLessThan(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, OrcFilters.LessThan::new);
    }

    @Override
    public Optional<OrcFilters.Predicate> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new OrcFilters.Not(
                                new OrcFilters.LessThan(colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<OrcFilters.Predicate> visitNotEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new OrcFilters.Not(
                                new OrcFilters.Equals(colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<OrcFilters.Predicate> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, OrcFilters.LessThanEquals::new);
    }

    @Override
    public Optional<OrcFilters.Predicate> visitEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, OrcFilters.Equals::new);
    }

    @Override
    public Optional<OrcFilters.Predicate> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new OrcFilters.Not(
                                new OrcFilters.LessThanEquals(
                                        colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<OrcFilters.Predicate> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<OrcFilters.Predicate> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<OrcFilters.Predicate> visitAnd(List<Optional<OrcFilters.Predicate>> children) {
        if (children.size() != 2) {
            throw new RuntimeException("Illegal and children: " + children.size());
        }

        Optional<OrcFilters.Predicate> c1 = children.get(0);
        if (!c1.isPresent()) {
            return Optional.empty();
        }
        Optional<OrcFilters.Predicate> c2 = children.get(1);
        return c2.map(value -> new OrcFilters.And(c1.get(), value));
    }

    @Override
    public Optional<OrcFilters.Predicate> visitOr(List<Optional<OrcFilters.Predicate>> children) {
        if (children.size() != 2) {
            throw new RuntimeException("Illegal or children: " + children.size());
        }

        Optional<OrcFilters.Predicate> c1 = children.get(0);
        if (!c1.isPresent()) {
            return Optional.empty();
        }
        Optional<OrcFilters.Predicate> c2 = children.get(1);
        return c2.map(value -> new OrcFilters.Or(c1.get(), value));
    }

    private Optional<OrcFilters.Predicate> convertBinary(
            FieldRef fieldRef,
            Object literal,
            TriFunction<String, PredicateLeaf.Type, Serializable, OrcFilters.Predicate> func) {
        PredicateLeaf.Type litType = toOrcType(fieldRef.type());
        if (litType == null) {
            return Optional.empty();
        }
        // fetch literal and ensure it is serializable
        Object orcObj = toOrcObject(litType, literal);
        // validate that literal is serializable
        return orcObj instanceof Serializable
                ? Optional.of(func.apply(fieldRef.name(), litType, (Serializable) orcObj))
                : Optional.empty();
    }

    @Nullable
    private static Object toOrcObject(PredicateLeaf.Type litType, Object literalObj) {
        if (literalObj == null) {
            return null;
        }

        switch (litType) {
            case STRING:
                return literalObj.toString();
            case DECIMAL:
                return ((Decimal) literalObj).toBigDecimal();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay(((Number) literalObj).longValue()));
            case TIMESTAMP:
                return ((Timestamp) literalObj).toSQLTimestamp();
            default:
                return literalObj;
        }
    }

    @Nullable
    protected static PredicateLeaf.Type toOrcType(DataType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return PredicateLeaf.Type.LONG;
            case FLOAT:
            case DOUBLE:
                return PredicateLeaf.Type.FLOAT;
            case BOOLEAN:
                return PredicateLeaf.Type.BOOLEAN;
            case CHAR:
            case VARCHAR:
                return PredicateLeaf.Type.STRING;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PredicateLeaf.Type.TIMESTAMP;
            case DATE:
                return PredicateLeaf.Type.DATE;
            case DECIMAL:
                return PredicateLeaf.Type.DECIMAL;
            default:
                return null;
        }
    }

    /**
     * Function which takes three arguments.
     *
     * @param <S> type of the first argument
     * @param <T> type of the second argument
     * @param <U> type of the third argument
     * @param <R> type of the return value
     */
    @FunctionalInterface
    private interface TriFunction<S, T, U, R> {

        /**
         * Applies this function to the given arguments.
         *
         * @param s the first function argument
         * @param t the second function argument
         * @param u the third function argument
         * @return the function result
         */
        R apply(S s, T t, U u);
    }
}
