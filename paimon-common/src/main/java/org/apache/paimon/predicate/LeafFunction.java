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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Function to test a field with literals. */
public abstract class LeafFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonCreator
    public static LeafFunction fromJson(String name) throws IOException {
        LeafFunction function = RegistryHolder.REGISTRY.get(name);
        if (function == null) {
            throw new IllegalArgumentException("Could not resolve leaf function '" + name + "'");
        }
        return function;
    }

    /** Holder class for lazy initialization of the registry to avoid class loading deadlock. */
    private static class RegistryHolder {

        private static final Map<String, LeafFunction> REGISTRY = createRegistry();

        private static Map<String, LeafFunction> createRegistry() {
            Map<String, LeafFunction> registry = new HashMap<>();
            registry.put(Equal.NAME, Equal.INSTANCE);
            registry.put(NotEqual.NAME, NotEqual.INSTANCE);
            registry.put(LessThan.NAME, LessThan.INSTANCE);
            registry.put(LessOrEqual.NAME, LessOrEqual.INSTANCE);
            registry.put(GreaterThan.NAME, GreaterThan.INSTANCE);
            registry.put(GreaterOrEqual.NAME, GreaterOrEqual.INSTANCE);
            registry.put(IsNull.NAME, IsNull.INSTANCE);
            registry.put(IsNotNull.NAME, IsNotNull.INSTANCE);
            registry.put(StartsWith.NAME, StartsWith.INSTANCE);
            registry.put(EndsWith.NAME, EndsWith.INSTANCE);
            registry.put(Contains.NAME, Contains.INSTANCE);
            registry.put(Like.NAME, Like.INSTANCE);
            registry.put(In.NAME, In.INSTANCE);
            registry.put(NotIn.NAME, NotIn.INSTANCE);
            registry.put(Between.NAME, Between.INSTANCE);
            registry.put(NotBetween.NAME, NotBetween.INSTANCE);
            registry.put(TrueFunction.NAME, TrueFunction.INSTANCE);
            registry.put(FalseFunction.NAME, FalseFunction.INSTANCE);
            return Collections.unmodifiableMap(registry);
        }
    }

    @JsonValue
    public abstract String toJson();

    public abstract boolean test(DataType type, Object field, List<Object> literals);

    public abstract boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals);

    public abstract Optional<LeafFunction> negate();

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

    public abstract <T> T visit(
            FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals);

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
