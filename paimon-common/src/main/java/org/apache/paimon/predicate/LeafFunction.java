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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/** Function to test a field with literals. */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = LeafFunction.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Equal.class, name = Equal.NAME),
    @JsonSubTypes.Type(value = NotEqual.class, name = NotEqual.NAME),
    @JsonSubTypes.Type(value = LessThan.class, name = LessThan.NAME),
    @JsonSubTypes.Type(value = LessOrEqual.class, name = LessOrEqual.NAME),
    @JsonSubTypes.Type(value = GreaterThan.class, name = GreaterThan.NAME),
    @JsonSubTypes.Type(value = GreaterOrEqual.class, name = GreaterOrEqual.NAME),
    @JsonSubTypes.Type(value = IsNull.class, name = IsNull.NAME),
    @JsonSubTypes.Type(value = IsNotNull.class, name = IsNotNull.NAME),
    @JsonSubTypes.Type(value = StartsWith.class, name = StartsWith.NAME),
    @JsonSubTypes.Type(value = EndsWith.class, name = EndsWith.NAME),
    @JsonSubTypes.Type(value = Contains.class, name = Contains.NAME),
    @JsonSubTypes.Type(value = Like.class, name = Like.NAME),
    @JsonSubTypes.Type(value = In.class, name = In.NAME),
    @JsonSubTypes.Type(value = NotIn.class, name = NotIn.NAME)
})
public abstract class LeafFunction implements Serializable {
    public static final String FIELD_TYPE = "type";

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
