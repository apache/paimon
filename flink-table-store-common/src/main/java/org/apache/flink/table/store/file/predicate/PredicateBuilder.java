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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;

/** A utility class to create {@link Predicate} object for common filter conditions. */
public class PredicateBuilder {

    private final RowType rowType;
    private final List<String> fieldNames;

    public PredicateBuilder(RowType rowType) {
        this.rowType = rowType;
        this.fieldNames = rowType.getFieldNames();
    }

    public int indexOf(String field) {
        return fieldNames.indexOf(field);
    }

    public Predicate equal(int idx, Object literal) {
        return leaf(Equal.INSTANCE, idx, literal);
    }

    public Predicate notEqual(int idx, Object literal) {
        return leaf(NotEqual.INSTANCE, idx, literal);
    }

    public Predicate lessThan(int idx, Object literal) {
        return leaf(LessThan.INSTANCE, idx, literal);
    }

    public Predicate lessOrEqual(int idx, Object literal) {
        return leaf(LessOrEqual.INSTANCE, idx, literal);
    }

    public Predicate greaterThan(int idx, Object literal) {
        return leaf(GreaterThan.INSTANCE, idx, literal);
    }

    public Predicate greaterOrEqual(int idx, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, idx, literal);
    }

    public Predicate isNull(int idx) {
        return leaf(IsNull.INSTANCE, idx);
    }

    public Predicate isNotNull(int idx) {
        return leaf(IsNotNull.INSTANCE, idx);
    }

    public Predicate startsWith(int idx, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, idx, patternLiteral);
    }

    public Predicate leaf(NullFalseLeafBinaryFunction function, int idx, Object literal) {
        RowType.RowField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.getType(), idx, field.getName(), singletonList(literal));
    }

    public Predicate leaf(LeafUnaryFunction function, int idx) {
        RowType.RowField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.getType(), idx, field.getName(), Collections.emptyList());
    }

    public Predicate in(int idx, List<Object> literals) {
        if (literals.size() > 20) {
            RowType.RowField field = rowType.getFields().get(idx);
            return new LeafPredicate(In.INSTANCE, field.getType(), idx, field.getName(), literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(idx, literal));
        }
        return or(equals);
    }

    public Predicate notIn(int idx, List<Object> literals) {
        return in(idx, literals).negate().get();
    }

    public Predicate between(int idx, Object includedLowerBound, Object includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(idx, includedLowerBound),
                        lessOrEqual(idx, includedUpperBound)));
    }

    public static Predicate and(Predicate... predicates) {
        return and(Arrays.asList(predicates));
    }

    public static Predicate and(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an AND predicate");
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(And.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    public static Predicate or(Predicate... predicates) {
        return or(Arrays.asList(predicates));
    }

    public static Predicate or(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an OR predicate");
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(Or.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    public static List<Predicate> splitAnd(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(And.INSTANCE, predicate, result);
        return result;
    }

    public static List<Predicate> splitOr(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(Or.INSTANCE, predicate, result);
        return result;
    }

    private static void splitCompound(
            CompoundPredicate.Function function, Predicate predicate, List<Predicate> result) {
        if (predicate instanceof CompoundPredicate
                && ((CompoundPredicate) predicate).function().equals(function)) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                splitCompound(function, child, result);
            }
        } else {
            result.add(predicate);
        }
    }

    public static Object convertJavaObject(LogicalType literalType, Object o) {
        if (o == null) {
            return null;
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
                return o;
            case BIGINT:
                return ((Number) o).longValue();
            case DOUBLE:
                return ((Number) o).doubleValue();
            case TINYINT:
                return ((Number) o).byteValue();
            case SMALLINT:
                return ((Number) o).shortValue();
            case INTEGER:
                return ((Number) o).intValue();
            case FLOAT:
                return ((Number) o).floatValue();
            case VARCHAR:
                return StringData.fromString(o.toString());
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof Timestamp) {
                    localDate = ((Timestamp) o).toLocalDateTime().toLocalDate();
                } else if (o instanceof Date) {
                    localDate = ((Date) o).toLocalDate();
                } else if (o instanceof LocalDate) {
                    localDate = (LocalDate) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected date literal of class " + o.getClass().getName());
                }
                LocalDate epochDay =
                        Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                return (int) ChronoUnit.DAYS.between(epochDay, localDate);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return DecimalData.fromBigDecimal((BigDecimal) o, precision, scale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampData timestampData;
                if (o instanceof Timestamp) {
                    timestampData = TimestampData.fromTimestamp((Timestamp) o);
                } else if (o instanceof Instant) {
                    timestampData = TimestampData.fromInstant((Instant) o);
                } else if (o instanceof LocalDateTime) {
                    timestampData = TimestampData.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException("Unsupported object: " + o);
                }
                return timestampData;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, List<String> inputFields, List<String> pickedFields) {
        return pickTransformFieldMapping(
                predicates, inputFields.stream().mapToInt(pickedFields::indexOf).toArray());
    }

    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, int[] fieldIdxMapping) {
        List<Predicate> pick = new ArrayList<>();
        for (Predicate p : predicates) {
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxMapping);
            mapped.ifPresent(pick::add);
        }
        return pick;
    }

    public static Optional<Predicate> transformFieldMapping(
            Predicate predicate, int[] fieldIdxMapping) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Optional<Predicate> mapped = transformFieldMapping(child, fieldIdxMapping);
                if (mapped.isPresent()) {
                    children.add(mapped.get());
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new CompoundPredicate(compoundPredicate.function(), children));
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            int mapped = fieldIdxMapping[leafPredicate.index()];
            if (mapped >= 0) {
                return Optional.of(
                        new LeafPredicate(
                                leafPredicate.function(),
                                leafPredicate.type(),
                                mapped,
                                leafPredicate.fieldName(),
                                leafPredicate.literals()));
            } else {
                return Optional.empty();
            }
        }
    }

    public static boolean containsFields(Predicate predicate, Set<String> fields) {
        if (predicate instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                if (containsFields(child, fields)) {
                    return true;
                }
            }
            return false;
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            return fields.contains(leafPredicate.fieldName());
        }
    }
}
