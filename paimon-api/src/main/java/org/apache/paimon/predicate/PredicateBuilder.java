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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

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
        Pair<NullFalseLeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), idx, optimized.getValue());
    }

    public Predicate like(Transform transform, Object patternLiteral) {
        Pair<NullFalseLeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), transform, optimized.getValue());
    }

    private Predicate leaf(LeafFunction function, int idx, Object literal) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(function, field.type(), idx, field.name(), singletonList(literal));
    }

    private Predicate leaf(LeafFunction function, Transform transform, Object literal) {
        return TransformPredicate.of(transform, function, singletonList(literal));
    }

    private Predicate leaf(LeafUnaryFunction function, int idx) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.type(), idx, field.name(), Collections.emptyList());
    }

    private Predicate leaf(LeafFunction function, Transform transform) {
        return TransformPredicate.of(transform, function, Collections.emptyList());
    }

    public Predicate in(int idx, List<Object> literals) {
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20 || literals.size() == 0) {
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
            return TransformPredicate.of(transform, In.INSTANCE, literals);
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
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(idx, includedLowerBound),
                        lessOrEqual(idx, includedUpperBound)));
    }

    public Predicate between(
            Transform transform, Object includedLowerBound, Object includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(transform, includedLowerBound),
                        lessOrEqual(transform, includedUpperBound)));
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
        // TODO: merge PredicateProjectionConverter
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
        } else if (predicate instanceof TransformPredicate) {
            TransformPredicate transformPredicate = (TransformPredicate) predicate;
            List<Object> inputs = transformPredicate.transform.inputs();
            List<Object> newInputs = new ArrayList<>(inputs.size());
            for (Object input : inputs) {
                if (input instanceof FieldRef) {
                    FieldRef fieldRef = (FieldRef) input;
                    int mappedIndex = fieldIdxMapping[fieldRef.index()];
                    if (mappedIndex >= 0) {
                        newInputs.add(new FieldRef(mappedIndex, fieldRef.name(), fieldRef.type()));
                    } else {
                        return Optional.empty();
                    }
                } else {
                    newInputs.add(input);
                }
            }
            return Optional.of(transformPredicate.copyWithNewInputs(newInputs));
        } else {
            return Optional.empty();
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
            TransformPredicate transformPredicate = (TransformPredicate) predicate;
            return fields.containsAll(transformPredicate.fieldNames());
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

    public static int[] fieldIdxToPartitionIdx(RowType tableType, List<String> partitionKeys) {
        return tableType.getFieldNames().stream().mapToInt(partitionKeys::indexOf).toArray();
    }
}
