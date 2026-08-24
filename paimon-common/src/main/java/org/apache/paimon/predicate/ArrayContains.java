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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;

/** A {@link LeafBinaryFunction} to test whether an array contains an element. */
public class ArrayContains extends LeafBinaryFunction {

    public static final String NAME = "ARRAY_CONTAINS";

    public static final ArrayContains INSTANCE = new ArrayContains();

    @JsonCreator
    private ArrayContains() {}

    @Override
    public DataType literalType(DataType fieldType) {
        return elementType(fieldType);
    }

    @Override
    public boolean test(DataType type, Object field, Object literal) {
        if (field == null || literal == null) {
            return false;
        }
        DataType elementType = elementType(type);
        InternalArray array = (InternalArray) field;
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
        for (int i = 0; i < array.size(); i++) {
            Object element = getter.getElementOrNull(array, i);
            if (element != null && compareLiteral(elementType, literal, element) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal) {
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitArrayContains(fieldRef, literals.get(0));
    }

    @Override
    public String toJson() {
        return NAME;
    }

    static DataType elementType(DataType fieldType) {
        Preconditions.checkArgument(
                fieldType instanceof ArrayType,
                "ARRAY_CONTAINS requires an ARRAY field, but field type is %s.",
                fieldType);
        return ((ArrayType) fieldType).getElementType();
    }
}
