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

import org.apache.flink.table.store.file.stats.FieldStats;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with an {@link Literal}. */
public class LeafPredicate implements Predicate {

    private final Function function;
    private final int index;
    private final Literal literal;

    public LeafPredicate(Function function, int index, Literal literal) {
        this.function = function;
        this.index = index;
        this.literal = literal;
    }

    public Function function() {
        return function;
    }

    public int index() {
        return index;
    }

    public Literal literal() {
        return literal;
    }

    @Override
    public boolean test(Object[] values) {
        return function.test(values, index, literal);
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        return function.test(rowCount, fieldStats, index, literal);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate(index, literal);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LeafPredicate)) {
            return false;
        }
        LeafPredicate that = (LeafPredicate) o;
        return Objects.equals(function, that.function)
                && index == that.index
                && Objects.equals(literal, that.literal);
    }

    /** Function to compare a field in the row with an {@link Literal}. */
    public interface Function extends Serializable {

        boolean test(Object[] values, int index, Literal literal);

        boolean test(long rowCount, FieldStats[] fieldStats, int index, Literal literal);

        Optional<Predicate> negate(int index, Literal literal);
    }
}
