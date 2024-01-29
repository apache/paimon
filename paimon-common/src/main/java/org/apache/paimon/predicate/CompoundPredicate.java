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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Non-leaf node in a {@link Predicate} tree. Its evaluation result depends on the results of its
 * children.
 */
public class CompoundPredicate implements Predicate {

    private final Function function;
    private final List<Predicate> children;

    public CompoundPredicate(Function function, List<Predicate> children) {
        this.function = function;
        this.children = children;
    }

    public Function function() {
        return function;
    }

    public List<Predicate> children() {
        return children;
    }

    @Override
    public boolean test(InternalRow row) {
        return function.test(row, children);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        return function.test(rowCount, minValues, maxValues, nullCounts, children);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate(children);
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompoundPredicate)) {
            return false;
        }
        CompoundPredicate that = (CompoundPredicate) o;
        return Objects.equals(function, that.function) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, children);
    }

    @Override
    public String toString() {
        return function + "(" + children + ")";
    }

    /** Evaluate the predicate result based on multiple {@link Predicate}s. */
    public abstract static class Function implements Serializable {

        public abstract boolean test(InternalRow row, List<Predicate> children);

        public abstract boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts,
                List<Predicate> children);

        public abstract Optional<Predicate> negate(List<Predicate> children);

        public abstract <T> T visit(FunctionVisitor<T> visitor, List<T> children);

        @Override
        public int hashCode() {
            return this.getClass().getName().hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
}
