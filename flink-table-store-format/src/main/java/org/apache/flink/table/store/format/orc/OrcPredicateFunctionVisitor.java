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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Convert {@link org.apache.flink.table.store.file.predicate.Predicate} to {@link Predicate} for
 * different {@link LeafFunction}.
 */
public class OrcPredicateFunctionVisitor implements OrcPredicateVisitor {
    private static final OrcPredicateFunctionVisitor VISITOR = new OrcPredicateFunctionVisitor();

    private static final ImmutableMap<Class<? extends LeafFunction>, OrcPredicateVisitor>
            PREDICATE_VISITORS =
                    new ImmutableMap.Builder<Class<? extends LeafFunction>, OrcPredicateVisitor>()
                            .put(IsNull.class, convertIsNull())
                            .put(IsNotNull.class, convertIsNotNull())
                            .put(
                                    Equal.class,
                                    convertBinary(OrcPredicateFunctionVisitor::convertEquals))
                            .put(
                                    NotEqual.class,
                                    convertBinary(OrcPredicateFunctionVisitor::convertNotEquals))
                            .put(
                                    GreaterThan.class,
                                    convertBinary(OrcPredicateFunctionVisitor::convertGreaterThan))
                            .put(
                                    GreaterOrEqual.class,
                                    convertBinary(
                                            OrcPredicateFunctionVisitor::convertGreaterThanEquals))
                            .put(
                                    LessThan.class,
                                    convertBinary(OrcPredicateFunctionVisitor::convertLessThan))
                            .put(
                                    LessOrEqual.class,
                                    convertBinary(
                                            OrcPredicateFunctionVisitor::convertLessThanEquals))
                            .build();

    private OrcPredicateFunctionVisitor() {}

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        OrcPredicateVisitor visitor = PREDICATE_VISITORS.get(predicate.function().getClass());
        if (visitor == null) {
            return Optional.empty();
        }
        return predicate.visit(visitor);
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

    public static OrcPredicateFunctionVisitor visitor() {
        return VISITOR;
    }

    private static OrcPredicateVisitor convertIsNull() {
        return predicate -> {
            PredicateLeaf.Type colType = toOrcType(predicate.type());
            if (colType == null) {
                return Optional.empty();
            }

            return Optional.of(new OrcFilters.IsNull(predicate.fieldName(), colType));
        };
    }

    private static OrcPredicateVisitor convertIsNotNull() {
        return predicate -> {
            Optional<Predicate> isNull = PREDICATE_VISITORS.get(IsNull.class).visit(predicate);
            return isNull.map(OrcFilters.Not::new);
        };
    }

    private static OrcPredicateVisitor convertBinary(
            TriFunction<String, PredicateLeaf.Type, Serializable, Predicate> func) {
        return predicate -> {
            PredicateLeaf.Type litType = toOrcType(predicate.type());
            if (litType == null) {
                return Optional.empty();
            }

            String colName = predicate.fieldName();

            // fetch literal and ensure it is serializable
            Object orcObj = toOrcObject(litType, predicate.literals().get(0));
            Serializable literal;
            // validate that literal is serializable
            if (orcObj instanceof Serializable) {
                literal = (Serializable) orcObj;
            } else {
                return Optional.empty();
            }

            return Optional.of(func.apply(colName, litType, literal));
        };
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
}
