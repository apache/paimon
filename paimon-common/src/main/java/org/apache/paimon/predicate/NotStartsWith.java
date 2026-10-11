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

/** A {@link LeafBinaryFunction} to evaluate {@code NOT STARTS_WITH(field, literal)}. */
public class NotStartsWith extends LeafBinaryFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "NOT_STARTS_WITH";

    public static final NotStartsWith INSTANCE = new NotStartsWith();

    @JsonCreator
    private NotStartsWith() {}

    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        return !StartsWith.INSTANCE.test(type, field, patternLiteral);
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object patternLiteral) {
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(StartsWith.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitNotStartsWith(fieldRef, literals.get(0));
    }

    @Override
    public String toJson() {
        return NAME;
    }
}
