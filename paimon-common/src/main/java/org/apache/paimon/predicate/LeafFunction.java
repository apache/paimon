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
        property = LeafFunction.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Equal.class, name = LeafFunction.Types.EQUAL),
    @JsonSubTypes.Type(value = NotEqual.class, name = LeafFunction.Types.NOT_EQUAL),
    @JsonSubTypes.Type(value = LessThan.class, name = LeafFunction.Types.LESS_THAN),
    @JsonSubTypes.Type(value = LessOrEqual.class, name = LeafFunction.Types.LESS_OR_EQUAL),
    @JsonSubTypes.Type(value = GreaterThan.class, name = LeafFunction.Types.GREATER_THAN),
    @JsonSubTypes.Type(value = GreaterOrEqual.class, name = LeafFunction.Types.GREATER_OR_EQUAL),
    @JsonSubTypes.Type(value = IsNull.class, name = LeafFunction.Types.IS_NULL),
    @JsonSubTypes.Type(value = IsNotNull.class, name = LeafFunction.Types.IS_NOT_NULL),
    @JsonSubTypes.Type(value = StartsWith.class, name = LeafFunction.Types.STARTS_WITH),
    @JsonSubTypes.Type(value = EndsWith.class, name = LeafFunction.Types.ENDS_WITH),
    @JsonSubTypes.Type(value = Contains.class, name = LeafFunction.Types.CONTAINS),
    @JsonSubTypes.Type(value = Like.class, name = LeafFunction.Types.LIKE),
    @JsonSubTypes.Type(value = In.class, name = LeafFunction.Types.IN),
    @JsonSubTypes.Type(value = NotIn.class, name = LeafFunction.Types.NOT_IN)
})
public abstract class LeafFunction implements Serializable {
    /** Types for function. */
    class Types {
        public static final String FIELD_TYPE = "type";

        public static final String EQUAL = "equal";
        public static final String NOT_EQUAL = "notEqual";
        public static final String LESS_THAN = "lessThan";
        public static final String LESS_OR_EQUAL = "lessOrEqual";
        public static final String GREATER_THAN = "greaterThan";
        public static final String GREATER_OR_EQUAL = "greaterOrEqual";
        public static final String IS_NULL = "isNull";
        public static final String IS_NOT_NULL = "isNotNull";
        public static final String STARTS_WITH = "startsWith";
        public static final String ENDS_WITH = "endsWith";
        public static final String CONTAINS = "contains";
        public static final String LIKE = "like";
        public static final String IN = "in";
        public static final String NOT_IN = "notIn";

        private Types() {}
    }

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
