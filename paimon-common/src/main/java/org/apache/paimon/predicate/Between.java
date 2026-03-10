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

import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** The {@link LeafFunction} to eval between. */
public class Between extends LeafTernaryFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "BETWEEN";

    public static final Between INSTANCE = new Between();

    @JsonCreator
    public Between() {}

    @Override
    public boolean test(DataType type, Object field, Object literal1, Object literal2) {
        return compareLiteral(type, literal1, field) <= 0
                && compareLiteral(type, literal2, field) >= 0;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object literal1,
            Object literal2) {
        // true if [min, max] and [l(0), l(1)] have intersection
        return compareLiteral(type, literal1, max) <= 0 && compareLiteral(type, literal2, min) >= 0;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(NotBetween.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitBetween(fieldRef, literals.get(0), literals.get(1));
    }

    @Override
    public String toJson() {
        return NAME;
    }

    /**
     * Optimize predicates by converting LessOrEqual and GreaterOrEqual to Between for the same
     * field.
     */
    public static List<Predicate> optimize(List<Predicate> predicates) {
        // First, split all AND predicates to get leaf predicates
        List<Predicate> leafPredicates = new ArrayList<>();
        for (Predicate predicate : predicates) {
            leafPredicates.addAll(splitAnd(predicate));
        }

        // Group leaf predicates by their FieldTransform
        Map<FieldTransform, List<LeafPredicate>> predicatesByField = new HashMap<>();
        List<Predicate> otherPredicates = new ArrayList<>();

        for (Predicate predicate : leafPredicates) {
            if (predicate instanceof LeafPredicate) {
                LeafPredicate leafPredicate = (LeafPredicate) predicate;
                if (leafPredicate.transform() instanceof FieldTransform) {
                    FieldTransform fieldTransform = (FieldTransform) leafPredicate.transform();
                    predicatesByField
                            .computeIfAbsent(fieldTransform, k -> new ArrayList<>())
                            .add(leafPredicate);
                } else {
                    otherPredicates.add(predicate);
                }
            } else {
                otherPredicates.add(predicate);
            }
        }

        List<Predicate> result = new ArrayList<>(otherPredicates);
        for (Map.Entry<FieldTransform, List<LeafPredicate>> entry : predicatesByField.entrySet()) {
            FieldTransform field = entry.getKey();
            List<LeafPredicate> fieldPredicates = entry.getValue();
            fieldPredicates = mergeLessAndGreaterToBetween(field, fieldPredicates);
            fieldPredicates = mergeMultipleBetweens(field, fieldPredicates);
            result.addAll(fieldPredicates);
        }

        // return input predicate if nothing changed
        return result.size() == predicates.size() ? predicates : result;
    }

    private static List<LeafPredicate> mergeLessAndGreaterToBetween(
            FieldTransform field, List<LeafPredicate> predicates) {
        List<LeafPredicate> result = new ArrayList<>();
        DataType type = field.outputType();
        LeafPredicate lessOrEqual = null;
        LeafPredicate greaterOrEqual = null;

        for (LeafPredicate leafPredicate : predicates) {
            if (leafPredicate.function() == LessOrEqual.INSTANCE) {
                if (lessOrEqual == null) {
                    lessOrEqual = leafPredicate;
                } else {
                    lessOrEqual =
                            compareLiteral(
                                                    type,
                                                    lessOrEqual.literals().get(0),
                                                    leafPredicate.literals().get(0))
                                            < 0
                                    ? lessOrEqual
                                    : leafPredicate;
                }
            } else if (leafPredicate.function() == GreaterOrEqual.INSTANCE) {
                if (greaterOrEqual == null) {
                    greaterOrEqual = leafPredicate;
                } else {
                    greaterOrEqual =
                            compareLiteral(
                                                    type,
                                                    greaterOrEqual.literals().get(0),
                                                    leafPredicate.literals().get(0))
                                            > 0
                                    ? greaterOrEqual
                                    : leafPredicate;
                }
            } else {
                result.add(leafPredicate);
            }
        }

        // If we have both LessOrEqual and GreaterOrEqual, convert to Between
        if (lessOrEqual != null && greaterOrEqual != null) {
            // Determine which is the lower bound and which is the upper bound
            Object lowerBound = greaterOrEqual.literals().get(0);
            Object upperBound = lessOrEqual.literals().get(0);
            if (compareLiteral(type, lowerBound, upperBound) >= 0) {
                // No valid intersection, keep all original predicates
                result.add(lessOrEqual);
                result.add(greaterOrEqual);
            } else {
                // convert to Between
                LeafPredicate betweenPredicate =
                        LeafPredicate.of(
                                field, Between.INSTANCE, Arrays.asList(lowerBound, upperBound));
                result.add(betweenPredicate);
            }
        } else {
            // Add LessOrEqual and GreaterOrEqual to remaining if no pair found
            if (lessOrEqual != null) {
                result.add(lessOrEqual);
            }
            if (greaterOrEqual != null) {
                result.add(greaterOrEqual);
            }
        }

        return result;
    }

    private static List<LeafPredicate> mergeMultipleBetweens(
            FieldTransform field, List<LeafPredicate> predicates) {
        List<LeafPredicate> results = new ArrayList<>();
        List<LeafPredicate> betweens = new ArrayList<>();
        for (LeafPredicate predicate : predicates) {
            if (predicate.function() == Between.INSTANCE) {
                betweens.add(predicate);
            } else {
                results.add(predicate);
            }
        }
        if (betweens.isEmpty() || betweens.size() == 1) {
            return predicates;
        }

        DataType fieldType = field.outputType();

        // Find the maximum lower bound and minimum upper bound
        Object maxLower = null;
        Object minUpper = null;

        for (LeafPredicate between : betweens) {
            Object lower = between.literals().get(0);
            Object upper = between.literals().get(1);

            if (maxLower == null || compareLiteral(fieldType, lower, maxLower) > 0) {
                maxLower = lower;
            }
            if (minUpper == null || compareLiteral(fieldType, upper, minUpper) < 0) {
                minUpper = upper;
            }
        }

        // Check if intersection is valid
        if (maxLower != null
                && minUpper != null
                && compareLiteral(fieldType, maxLower, minUpper) <= 0) {
            // Valid intersection, create a single Between predicate
            LeafPredicate mergedBetween =
                    LeafPredicate.of(field, Between.INSTANCE, Arrays.asList(maxLower, minUpper));
            results.add(mergedBetween);
        } else {
            // No valid intersection, keep all original predicates unchanged
            results.addAll(betweens);
        }

        return results;
    }
}
