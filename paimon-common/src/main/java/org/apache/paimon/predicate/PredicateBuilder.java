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

package org.apache.paimon.predicate;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternal;

/**
 * A utility class to create {@link Predicate} object for common filter conditions.
 *
 * @since 0.4.0
 */
@Public
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

    public Predicate equal(Transform transform, Object literal) {
        return leaf(Equal.INSTANCE, transform, literal);
    }

    public Predicate notEqual(int idx, Object literal) {
        return leaf(NotEqual.INSTANCE, idx, literal);
    }

    public Predicate notEqual(Transform transform, Object literal) {
        return leaf(NotEqual.INSTANCE, transform, literal);
    }

    public Predicate lessThan(int idx, Object literal) {
        return leaf(LessThan.INSTANCE, idx, literal);
    }

    public Predicate lessThan(Transform transform, Object literal) {
        return leaf(LessThan.INSTANCE, transform, literal);
    }

    public Predicate lessOrEqual(int idx, Object literal) {
        return leaf(LessOrEqual.INSTANCE, idx, literal);
    }

    public Predicate lessOrEqual(Transform transform, Object literal) {
        return leaf(LessOrEqual.INSTANCE, transform, literal);
    }

    public Predicate greaterThan(int idx, Object literal) {
        return leaf(GreaterThan.INSTANCE, idx, literal);
    }

    public Predicate greaterThan(Transform transform, Object literal) {
        return leaf(GreaterThan.INSTANCE, transform, literal);
    }

    public Predicate greaterOrEqual(int idx, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, idx, literal);
    }

    public Predicate greaterOrEqual(Transform transform, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, transform, literal);
    }

    public Predicate isNull(int idx) {
        return leaf(IsNull.INSTANCE, idx);
    }

    public Predicate isNull(Transform transform) {
        return leaf(IsNull.INSTANCE, transform);
    }

    public Predicate isNotNull(int idx) {
        return leaf(IsNotNull.INSTANCE, idx);
    }

    public Predicate isNotNull(Transform transform) {
        return leaf(IsNotNull.INSTANCE, transform);
    }

    public Predicate startsWith(int idx, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, idx, patternLiteral);
    }

    public Predicate startsWith(Transform transform, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, transform, patternLiteral);
    }

    public Predicate endsWith(int idx, Object patternLiteral) {
        return leaf(EndsWith.INSTANCE, idx, patternLiteral);
    }

    public Predicate endsWith(Transform transform, Object patternLiteral) {
        return leaf(EndsWith.INSTANCE, transform, patternLiteral);
    }

    public Predicate contains(int idx, Object patternLiteral) {
        return leaf(Contains.INSTANCE, idx, patternLiteral);
    }

    public Predicate contains(Transform transform, Object patternLiteral) {
        return leaf(Contains.INSTANCE, transform, patternLiteral);
    }

    public Predicate like(int idx, Object patternLiteral) {
        Pair<LeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), idx, optimized.getValue());
    }

    public Predicate like(Transform transform, Object patternLiteral) {
        Pair<LeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), transform, optimized.getValue());
    }

    private Predicate leaf(LeafFunction function, int idx, Object literal) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(function, field.type(), idx, field.name(), singletonList(literal));
    }

    private Predicate leaf(LeafFunction function, Transform transform, Object literal) {
        return LeafPredicate.of(transform, function, singletonList(literal));
    }

    private Predicate leaf(LeafUnaryFunction function, int idx) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.type(), idx, field.name(), Collections.emptyList());
    }

    private Predicate leaf(LeafFunction function, Transform transform) {
        return LeafPredicate.of(transform, function, Collections.emptyList());
    }

    public Predicate in(int idx, List<Object> literals) {
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20 || literals.isEmpty()) {
            DataField field = rowType.getFields().get(idx);
            return new LeafPredicate(In.INSTANCE, field.type(), idx, field.name(), literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(idx, literal));
        }
        return or(equals);
    }

    public Predicate in(Transform transform, List<Object> literals) {
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20) {
            return LeafPredicate.of(transform, In.INSTANCE, literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(transform, literal));
        }
        return or(equals);
    }

    public Predicate notIn(int idx, List<Object> literals) {
        return in(idx, literals).negate().get();
    }

    public Predicate between(int idx, Object includedLowerBound, Object includedUpperBound) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                Between.INSTANCE,
                field.type(),
                idx,
                field.name(),
                Arrays.asList(includedLowerBound, includedUpperBound));
    }

    public Predicate between(
            Transform transform, Object includedLowerBound, Object includedUpperBound) {
        return new LeafPredicate(
                transform, Between.INSTANCE, Arrays.asList(includedLowerBound, includedUpperBound));
    }

    public static Predicate alwaysFalse() {
        return new LeafPredicate(NullTransform.INSTANCE, AlwaysFalse.INSTANCE, emptyList());
    }

    public static Predicate alwaysTrue() {
        return new LeafPredicate(NullTransform.INSTANCE, AlwaysTrue.INSTANCE, emptyList());
    }

    public static Predicate and(Predicate... predicates) {
        return and(Arrays.asList(predicates));
    }

    /**
     * Combines predicates with AND logic, applying the following simplifications:
     *
     * <ul>
     *   <li>Filters out always-true predicates (identity element for AND).
     *   <li>Short-circuits to always-false if any child is always-false.
     *   <li>Optimises {@code LessOrEqual + GreaterOrEqual} pairs on the same field into a single
     *       {@link Between} predicate via {@link Between#optimize}.
     *   <li>Unwraps to a single child when only one predicate remains.
     * </ul>
     */
    public static Predicate and(List<Predicate> predicates) {
        Preconditions.checkArgument(
                !predicates.isEmpty(),
                "There must be at least 1 inner predicate to construct an AND predicate");

        // Filter out always-true (identity for AND) and short-circuit on always-false
        List<Predicate> noTruePredicates = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (isAlwaysTrue(predicate)) {
                continue;
            }
            if (isAlwaysFalse(predicate)) {
                return alwaysFalse();
            }
            noTruePredicates.add(predicate);
        }
        if (noTruePredicates.isEmpty()) {
            return alwaysTrue();
        } else if (noTruePredicates.size() == 1) {
            return noTruePredicates.get(0);
        }

        // Optimize by converting LessOrEqual and GreaterOrEqual to Between for same field
        List<Predicate> optimized = Between.optimize(noTruePredicates);

        if (optimized.size() <= 1) {
            return optimized.get(0);
        }

        return new CompoundPredicate(And.INSTANCE, optimized);
    }

    @Nullable
    public static Predicate andNullable(Predicate... predicates) {
        return andNullable(Arrays.asList(predicates));
    }

    @Nullable
    public static Predicate andNullable(List<Predicate> predicates) {
        predicates = predicates.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (predicates.isEmpty()) {
            return null;
        }

        return and(predicates);
    }

    public static Predicate or(Predicate... predicates) {
        return or(Arrays.asList(predicates));
    }

    /**
     * Combines predicates with OR logic, applying the following simplifications:
     *
     * <ul>
     *   <li>Filters out always-false predicates (identity element for OR).
     *   <li>Short-circuits to always-true if any child is always-true.
     *   <li>Unwraps to a single child when only one predicate remains.
     * </ul>
     */
    public static Predicate or(List<Predicate> predicates) {
        Preconditions.checkArgument(
                !predicates.isEmpty(),
                "There must be at least 1 inner predicate to construct an OR predicate");

        // Filter out always-false (identity for OR) and short-circuit on always-true
        List<Predicate> noFalsePredicates = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (isAlwaysFalse(predicate)) {
                continue;
            }
            if (isAlwaysTrue(predicate)) {
                return alwaysTrue();
            }
            noFalsePredicates.add(predicate);
        }
        if (noFalsePredicates.isEmpty()) {
            return alwaysFalse();
        } else if (noFalsePredicates.size() == 1) {
            return noFalsePredicates.get(0);
        }

        return new CompoundPredicate(Or.INSTANCE, noFalsePredicates);
    }

    private static boolean isAlwaysFalse(Predicate predicate) {
        return predicate instanceof LeafPredicate
                && ((LeafPredicate) predicate).function().equals(AlwaysFalse.INSTANCE);
    }

    private static boolean isAlwaysTrue(Predicate predicate) {
        return predicate instanceof LeafPredicate
                && ((LeafPredicate) predicate).function().equals(AlwaysTrue.INSTANCE);
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
            CompoundFunction function, Predicate predicate, List<Predicate> result) {
        if (predicate instanceof CompoundPredicate
                && ((CompoundPredicate) predicate).function().equals(function)) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                splitCompound(function, child, result);
            }
        } else {
            result.add(predicate);
        }
    }

    public static Object convertJavaObject(DataType literalType, Object o) {
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
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(o.toString());
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof java.sql.Timestamp) {
                    localDate = ((java.sql.Timestamp) o).toLocalDateTime().toLocalDate();
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
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime;
                if (o instanceof java.sql.Time) {
                    localTime = ((java.sql.Time) o).toLocalTime();
                } else if (o instanceof java.time.LocalTime) {
                    localTime = (java.time.LocalTime) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected time literal of class " + o.getClass().getName());
                }
                // return millis of a day
                return (int) (localTime.toNanoOfDay() / 1_000_000);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return Decimal.fromBigDecimal((BigDecimal) o, precision, scale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    return Timestamp.fromSQLTimestamp((java.sql.Timestamp) o);
                } else if (o instanceof Instant) {
                    Instant o1 = (Instant) o;
                    LocalDateTime dateTime = o1.atZone(ZoneId.systemDefault()).toLocalDateTime();
                    return Timestamp.fromLocalDateTime(dateTime);
                } else if (o instanceof LocalDateTime) {
                    return Timestamp.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp without timezone ",
                                    o.getClass()));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    java.sql.Timestamp timestamp = (java.sql.Timestamp) o;
                    return Timestamp.fromInstant(timestamp.toInstant());
                } else if (o instanceof Instant) {
                    return Timestamp.fromInstant((Instant) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp with local time zone ",
                                    o.getClass()));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    public static Object convertToJavaObject(DataType dataType, Object o) {
        if (o == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return o;
            case CHAR:
            case VARCHAR:
                return o instanceof BinaryString ? o.toString() : String.valueOf(o);
            case DATE:
                return LocalDate.ofEpochDay(((Number) o).intValue());
            case TIME_WITHOUT_TIME_ZONE:
                long millisOfDay = ((Number) o).intValue();
                return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L);
            case DECIMAL:
                return ((Decimal) o).toBigDecimal();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((Timestamp) o).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp ts = (Timestamp) o;
                long millisecond = ts.getMillisecond();
                int nanoOfMillisecond = ts.getNanoOfMillisecond();
                long epochSecond = Math.floorDiv(millisecond, 1000L);
                int milliOfSecond = (int) Math.floorMod(millisecond, 1000L);
                long nanoAdjustment = milliOfSecond * 1_000_000L + nanoOfMillisecond;
                return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type " + dataType.getTypeRoot().name());
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
        return predicate.visit(PredicateProjectionConverter.fromMapping(fieldIdxMapping));
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
            return fields.containsAll(leafPredicate.fieldNames());
        }
    }

    public static List<Predicate> excludePredicateWithFields(
            @Nullable List<Predicate> predicates, Set<String> fields) {
        if (predicates == null || predicates.isEmpty() || fields.isEmpty()) {
            return predicates;
        }
        return predicates.stream()
                .filter(f -> !containsFields(f, fields))
                .collect(Collectors.toList());
    }

    @Nullable
    public static Predicate partition(
            Map<String, String> map, RowType rowType, String defaultPartValue) {
        Map<String, Object> internalValues = convertSpecToInternal(map, rowType, defaultPartValue);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (Map.Entry<String, Object> entry : internalValues.entrySet()) {
            int idx = fieldNames.indexOf(entry.getKey());
            Object literal = internalValues.get(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    public static Predicate partitions(
            List<Map<String, String>> partitions, RowType rowType, String defaultPartValue) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> PredicateBuilder.partition(p, rowType, defaultPartValue))
                        .toArray(Predicate[]::new));
    }

    public static int[] fieldIdxToPartitionIdx(RowType tableType, List<String> partitionKeys) {
        return tableType.getFieldNames().stream().mapToInt(partitionKeys::indexOf).toArray();
    }
}
