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

/** A {@link LeafFunction} that always returns {@code true}. Used for AlwaysTrue predicates. */
public class TrueFunction extends LeafFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRUE";

    public static final TrueFunction INSTANCE = new TrueFunction();

    @JsonCreator
    private TrueFunction() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        return true;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(FalseFunction.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException(
                "TrueFunction does not support field-based visitation.");
    }

    @Override
    public String toJson() {
        return NAME;
    }
}
