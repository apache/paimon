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

/** A {@link LeafFunction} that always returns {@code false}. Used for AlwaysFalse predicates. */
public class AlwaysFalse extends LeafFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "FALSE";

    public static final AlwaysFalse INSTANCE = new AlwaysFalse();

    @JsonCreator
    private AlwaysFalse() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        return false;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        return false;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(AlwaysTrue.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException(
                "AlwaysFalse does not support field-based visitation.");
    }

    @Override
    public String toJson() {
        return NAME;
    }
}
