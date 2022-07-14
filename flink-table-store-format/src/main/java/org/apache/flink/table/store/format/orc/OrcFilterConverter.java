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
import org.apache.flink.orc.OrcFilters.Predicate;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.file.predicate.CompoundPredicate;
import org.apache.flink.table.store.file.predicate.Equal;
import org.apache.flink.table.store.file.predicate.GreaterOrEqual;
import org.apache.flink.table.store.file.predicate.GreaterThan;
import org.apache.flink.table.store.file.predicate.IsNotNull;
import org.apache.flink.table.store.file.predicate.IsNull;
import org.apache.flink.table.store.file.predicate.LeafFunction;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.LessOrEqual;
import org.apache.flink.table.store.file.predicate.LessThan;
import org.apache.flink.table.store.file.predicate.NotEqual;
import org.apache.flink.table.store.file.predicate.Or;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.function.Function;

/** Utility class that provides helper methods to work with Orc Filter PushDown. */
public class OrcFilterConverter {

    private static final ImmutableMap<
                    Class<? extends LeafFunction>, Function<LeafPredicate, Predicate>>
            FILTERS =
                    new ImmutableMap.Builder<
                                    Class<? extends LeafFunction>,
                                    Function<LeafPredicate, Predicate>>()
                            .put(IsNull.class, OrcFilterConverter::convertIsNull)
                            .put(IsNotNull.class, OrcFilterConverter::convertIsNotNull)
                            .put(
                                    Equal.class,
                                    call -> convertBinary(call, OrcFilterConverter::convertEquals))
                            .put(
                                    NotEqual.class,
                                    call ->
                                            convertBinary(
                                                    call, OrcFilterConverter::convertNotEquals))
                            .put(
                                    GreaterThan.class,
                                    call ->
                                            convertBinary(
                                                    call, OrcFilterConverter::convertGreaterThan))
                            .put(
                                    GreaterOrEqual.class,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilterConverter::convertGreaterThanEquals))
                            .put(
                                    LessThan.class,
                                    call ->
                                            convertBinary(
                                                    call, OrcFilterConverter::convertLessThan))
                            .put(
                                    LessOrEqual.class,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilterConverter::convertLessThanEquals))
                            .build();

    private static Predicate convertIsNull(LeafPredicate predicate) {
        PredicateLeaf.Type colType = toOrcType(predicate.type());
        if (colType == null) {
            return null;
        }

        return new OrcFilters.IsNull(predicate.fieldName(), colType);
    }

    private static Predicate convertIsNotNull(LeafPredicate predicate) {
        Predicate isNull = convertIsNull(predicate);
        if (isNull == null) {
            return null;
        }
        return new OrcFilters.Not(isNull);
    }

    private static Predicate convertOr(CompoundPredicate or) {
        if (or.children().size() != 2) {
            throw new RuntimeException("Illegal or children: " + or.children().size());
        }

        Predicate c1 = toOrcPredicate(or.children().get(0));
        if (c1 == null) {
            return null;
        }
        Predicate c2 = toOrcPredicate(or.children().get(1));
        if (c2 == null) {
            return null;
        }

        return new OrcFilters.Or(c1, c2);
    }

    private static Predicate convertBinary(
            LeafPredicate predicate,
            TriFunction<String, PredicateLeaf.Type, Serializable, Predicate> func) {
        PredicateLeaf.Type litType = toOrcType(predicate.type());
        if (litType == null) {
            return null;
        }

        String colName = predicate.fieldName();

        // fetch literal and ensure it is serializable
        Object orcObj = toOrcObject(litType, predicate.literals().get(0));
        Serializable literal;
        // validate that literal is serializable
        if (orcObj instanceof Serializable) {
            literal = (Serializable) orcObj;
        } else {
            return null;
        }

        return func.apply(colName, litType, literal);
    }

    private static Predicate convertEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.Equals(colName, litType, literal);
    }

    private static Predicate convertNotEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.Not(convertEquals(colName, litType, literal));
    }

    private static Predicate convertGreaterThan(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.Not(new OrcFilters.LessThanEquals(colName, litType, literal));
    }

    private static Predicate convertGreaterThanEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.Not(new OrcFilters.LessThan(colName, litType, literal));
    }

    private static Predicate convertLessThan(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.LessThan(colName, litType, literal);
    }

    private static Predicate convertLessThanEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new OrcFilters.LessThanEquals(colName, litType, literal);
    }

    @Nullable
    public static Predicate toOrcPredicate(
            org.apache.flink.table.store.file.predicate.Predicate expression) {
        if (expression instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) expression;
            if (compound.function().equals(Or.INSTANCE)) {
                return convertOr(compound);
            }
        } else if (expression instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) expression;
            Function<LeafPredicate, Predicate> function = FILTERS.get(leaf.function().getClass());
            if (function == null) {
                return null;
            }
            return function.apply(leaf);
        }
        return null;
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
