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

package dev.vortex.api.expressions;

import dev.vortex.api.Expression;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents an IS NOT NULL expression that checks whether values are non-null.
 * This expression returns true for non-null values and false for null values.
 */
public final class IsNotNull implements Expression {
    private final Expression child;

    private IsNotNull(Expression child) {
        this.child = child;
    }

    /**
     * Parses an IsNotNull expression from serialized metadata and child expressions.
     * This method is used during deserialization of Vortex expressions.
     *
     * @param metadata the serialized metadata, must be empty for IsNotNull expressions
     * @param children the child expressions, must contain exactly one element
     * @return a new IsNotNull expression parsed from the provided data
     * @throws IllegalArgumentException if the number of children is not exactly one,
     *                                  or if metadata is not empty
     */
    public static IsNotNull parse(byte[] metadata, List<Expression> children) {
        if (children.size() != 1) {
            throw new IllegalArgumentException(
                    "IsNotNull expression must have exactly one child, found: " + children.size());
        }
        if (metadata.length > 0) {
            throw new IllegalArgumentException(
                    "IsNotNull expression must not have metadata, found: " + metadata.length);
        }
        return new IsNotNull(children.get(0));
    }

    /**
     * Creates a new IsNotNull expression that checks non-nullity of the given child expression.
     *
     * @param child the expression to check for non-null values
     * @return a new IsNotNull expression
     */
    public static IsNotNull of(Expression child) {
        return new IsNotNull(child);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        IsNotNull other = (IsNotNull) o;
        return Objects.equals(child, other.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child);
    }

    @Override
    public String id() {
        return "vortex.is_not_null";
    }

    @Override
    public List<Expression> children() {
        return java.util.Collections.singletonList(child);
    }

    @Override
    public Optional<byte[]> metadata() {
        return Optional.of(new byte[] {});
    }

    @Override
    public String toString() {
        return "vortex.is_not_null(" + child + ")";
    }

    /**
     * Returns the child expression that will be checked for non-null values.
     *
     * @return the child expression
     */
    public Expression getChild() {
        return child;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visitIsNotNull(this);
    }
}
