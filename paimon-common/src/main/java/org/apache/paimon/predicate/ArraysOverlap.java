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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

/** A {@link LeafNAryFunction} testing whether an array overlaps literal elements. */
public class ArraysOverlap extends LeafNAryFunction {

    public static final String NAME = "ARRAYS_OVERLAP";

    public static final ArraysOverlap INSTANCE = new ArraysOverlap();

    @JsonCreator
    private ArraysOverlap() {}

    @Override
    public DataType literalType(DataType fieldType) {
        return elementType(fieldType);
    }

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null && ArrayContains.INSTANCE.test(type, field, literal)) {
                return true;
            }
        }
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
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitArraysOverlap(fieldRef, literals);
    }

    @Override
    public String toJson() {
        return NAME;
    }

    static DataType elementType(DataType fieldType) {
        Preconditions.checkArgument(
                fieldType instanceof ArrayType,
                "ARRAYS_OVERLAP requires an ARRAY field, but field type is %s.",
                fieldType);
        return ((ArrayType) fieldType).getElementType();
    }
}
