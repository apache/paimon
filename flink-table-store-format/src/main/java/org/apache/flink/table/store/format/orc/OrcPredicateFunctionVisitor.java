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

import org.apache.flink.orc.OrcFilters;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.file.predicate.FieldRef;
import org.apache.flink.table.store.file.predicate.FunctionVisitor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.TriFunction;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Convert {@link org.apache.flink.table.store.file.predicate.Predicate} to {@link
 * OrcFilters.Predicate} for orc.
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
        return Optional.empty();
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

        String colName = fieldRef.name();

        // fetch literal and ensure it is serializable
        Object orcObj = toOrcObject(litType, literal);
        Serializable serializableLiteral;
        // validate that literal is serializable
        if (orcObj instanceof Serializable) {
            serializableLiteral = (Serializable) orcObj;
        } else {
            return Optional.empty();
        }

        return Optional.of(func.apply(colName, litType, serializableLiteral));
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
                return ((DecimalData) literalObj).toBigDecimal();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay(((Number) literalObj).longValue()));
            case TIMESTAMP:
                return ((TimestampData) literalObj).toTimestamp();
            default:
                return literalObj;
        }
    }

    @Nullable
    private static PredicateLeaf.Type toOrcType(LogicalType type) {
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
                return PredicateLeaf.Type.TIMESTAMP;
            case DATE:
                return PredicateLeaf.Type.DATE;
            case DECIMAL:
                return PredicateLeaf.Type.DECIMAL;
            default:
                return null;
        }
    }
}
