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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Data type of fixed-size vector type. The elements are densely stored.
 *
 * @since 2.0.0
 */
@Public
public class VectorType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_LENGTH = 1;

    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    public static final String FORMAT = "VECTOR<%s, %d>";

    private final DataType elementType;

    private final int length;

    public VectorType(boolean isNullable, int length, DataType elementType) {
        super(isNullable, DataTypeRoot.VECTOR);
        this.elementType =
                Preconditions.checkNotNull(elementType, "Element type must not be null.");
        Preconditions.checkArgument(
                isValidElementType(elementType), "Invalid element type for vector: " + elementType);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    public VectorType(int length, DataType elementType) {
        this(true, length, elementType);
    }

    public int getLength() {
        return length;
    }

    public DataType getElementType() {
        return elementType;
    }

    public static boolean isValidElementType(DataType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int defaultSize() {
        return elementType.defaultSize() * length;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new VectorType(isNullable, length, elementType.copy());
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, elementType.asSQLString(), length);
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "VECTOR" : "VECTOR NOT NULL");
        generator.writeFieldName("element");
        elementType.serializeJson(generator);
        generator.writeFieldName("length");
        generator.writeNumber(length);
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VectorType vectorType = (VectorType) o;
        return elementType.equals(vectorType.elementType) && length == vectorType.length;
    }

    @Override
    public boolean equalsIgnoreFieldId(DataType o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VectorType vectorType = (VectorType) o;
        return elementType.equalsIgnoreFieldId(vectorType.elementType)
                && length == vectorType.length;
    }

    @Override
    public boolean isPrunedFrom(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VectorType vectorType = (VectorType) o;
        return elementType.isPrunedFrom(vectorType.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType, length);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        elementType.collectFieldIds(fieldIds);
    }
}
