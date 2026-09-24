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

import java.util.List;
import java.util.Optional;

/** A {@link LeafBinaryFunction} to evaluate {@code field not like pattern}. */
public class NotLike extends LeafBinaryFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "NOT_LIKE";

    public static final NotLike INSTANCE = new NotLike();

    @JsonCreator
    private NotLike() {}

    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        return !Like.INSTANCE.test(type, field, patternLiteral);
    }

    @Override
    public boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal) {
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(Like.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitNotLike(fieldRef, literals.get(0));
    }

    @Override
    public String toJson() {
        return NAME;
    }
}
