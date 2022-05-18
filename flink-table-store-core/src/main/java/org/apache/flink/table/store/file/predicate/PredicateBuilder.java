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

import java.util.List;

/** A utility class to create {@link Predicate} object for common filter conditions. */
public class PredicateBuilder {

    public static Predicate and(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an AND predicate");
        return predicates.stream().reduce(And::new).get();
    }

    public static Predicate or(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an OR predicate");
        return predicates.stream().reduce(Or::new).get();
    }

    public static Predicate in(int idx, List<Literal> literals) {
        Preconditions.checkArgument(
                literals.size() > 0,
                "There must be at least 1 literal to construct an IN predicate");
        return literals.stream().map(l -> (Predicate) new Equal(idx, l)).reduce(Or::new).get();
    }

    public static Predicate between(
            int idx, Literal includedLowerBound, Literal includedUpperBound) {
        return new And(
                new GreaterOrEqual(idx, includedLowerBound),
                new LessOrEqual(idx, includedUpperBound));
    }
}
