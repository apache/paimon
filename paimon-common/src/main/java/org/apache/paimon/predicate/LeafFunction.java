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
import java.util.List;
import java.util.Optional;

/** Function to test a field with literals. */
public abstract class LeafFunction implements Serializable {

    @JsonCreator
    public static LeafFunction fromJson(String name) throws IOException {
        switch (name) {
            case Equal.NAME:
                return Equal.INSTANCE;
            case NotEqual.NAME:
                return NotEqual.INSTANCE;
            case LessThan.NAME:
                return LessThan.INSTANCE;
            case LessOrEqual.NAME:
                return LessOrEqual.INSTANCE;
            case GreaterThan.NAME:
                return GreaterThan.INSTANCE;
            case GreaterOrEqual.NAME:
                return GreaterOrEqual.INSTANCE;
            case IsNull.NAME:
                return IsNull.INSTANCE;
            case IsNotNull.NAME:
                return IsNotNull.INSTANCE;
            case StartsWith.NAME:
                return StartsWith.INSTANCE;
            case EndsWith.NAME:
                return EndsWith.INSTANCE;
            case Contains.NAME:
                return Contains.INSTANCE;
            case Like.NAME:
                return Like.INSTANCE;
            case In.NAME:
                return In.INSTANCE;
            case NotIn.NAME:
                return NotIn.INSTANCE;
            default:
                throw new IllegalArgumentException(
                        "Could not resolve leaf function '" + name + "'");
        }
    }

    @JsonValue
    public String toJson() {
        if (this instanceof Equal) {
            return Equal.NAME;
        } else if (this instanceof NotEqual) {
            return NotEqual.NAME;
        } else if (this instanceof LessThan) {
            return LessThan.NAME;
        } else if (this instanceof LessOrEqual) {
            return LessOrEqual.NAME;
        } else if (this instanceof GreaterThan) {
            return GreaterThan.NAME;
        } else if (this instanceof GreaterOrEqual) {
            return GreaterOrEqual.NAME;
        } else if (this instanceof IsNull) {
            return IsNull.NAME;
        } else if (this instanceof IsNotNull) {
            return IsNotNull.NAME;
        } else if (this instanceof StartsWith) {
            return StartsWith.NAME;
        } else if (this instanceof EndsWith) {
            return EndsWith.NAME;
        } else if (this instanceof Contains) {
            return Contains.NAME;
        } else if (this instanceof Like) {
            return Like.NAME;
        } else if (this instanceof In) {
            return In.NAME;
        } else if (this instanceof NotIn) {
            return NotIn.NAME;
        } else {
            throw new IllegalArgumentException(
                    "Unknown leaf function class for JSON serialization: " + getClass());
        }
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
