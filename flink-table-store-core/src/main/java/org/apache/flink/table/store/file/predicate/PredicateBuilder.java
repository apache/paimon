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

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** A utility class to create {@link Predicate} object for common filter conditions. */
public class PredicateBuilder {

    public static Predicate equal(int idx, Literal literal) {
        return new LeafPredicate(Equal.INSTANCE, idx, literal);
    }

    public static Predicate notEqual(int idx, Literal literal) {
        return new LeafPredicate(NotEqual.INSTANCE, idx, literal);
    }

    public static Predicate lessThan(int idx, Literal literal) {
        return new LeafPredicate(LessThan.INSTANCE, idx, literal);
    }

    public static Predicate lessOrEqual(int idx, Literal literal) {
        return new LeafPredicate(LessOrEqual.INSTANCE, idx, literal);
    }

    public static Predicate greaterThan(int idx, Literal literal) {
        return new LeafPredicate(GreaterThan.INSTANCE, idx, literal);
    }

    public static Predicate greaterOrEqual(int idx, Literal literal) {
        return new LeafPredicate(GreaterOrEqual.INSTANCE, idx, literal);
    }

    public static Predicate isNull(int idx) {
        return new LeafPredicate(IsNull.INSTANCE, idx, null);
    }

    public static Predicate isNotNull(int idx) {
        return new LeafPredicate(IsNotNull.INSTANCE, idx, null);
    }

    public static Predicate startsWith(int idx, Literal patternLiteral) {
        return new LeafPredicate(StartsWith.INSTANCE, idx, patternLiteral);
    }

    public static Predicate and(Predicate... predicates) {
        return and(Arrays.asList(predicates));
    }

    public static Predicate and(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an AND predicate");
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

    public static Predicate in(int idx, List<Literal> literals) {
        Preconditions.checkArgument(
                literals.size() > 0,
                "There must be at least 1 literal to construct an IN predicate");
        return literals.stream()
                .map(l -> equal(idx, l))
                .reduce((a, b) -> new CompoundPredicate(Or.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    public static Predicate between(
            int idx, Literal includedLowerBound, Literal includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(idx, includedLowerBound),
                        lessOrEqual(idx, includedUpperBound)));
    }

    public static List<Predicate> splitAnd(Predicate predicate) {
        List<Predicate> result = new ArrayList<>();
        splitAnd(predicate, result);
        return result;
    }

    private static void splitAnd(Predicate predicate, List<Predicate> result) {
        if (predicate instanceof CompoundPredicate
                && ((CompoundPredicate) predicate).function().equals(And.INSTANCE)) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                splitAnd(child, result);
            }
        } else {
            result.add(predicate);
        }
    }
}
