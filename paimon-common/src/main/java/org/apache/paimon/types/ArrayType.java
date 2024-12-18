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
 * Data type of an array of elements with same subtype. Compared to the SQL standard, the maximum
 * cardinality of an array cannot be specified but is fixed at {@link Integer#MAX_VALUE}. Also, any
 * valid type is supported as a subtype.
 *
 * @since 0.4.0
 */
@Public
public final class ArrayType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String FORMAT = "ARRAY<%s>";

    private final DataType elementType;

    public ArrayType(boolean isNullable, DataType elementType) {
        super(isNullable, DataTypeRoot.ARRAY);
        this.elementType =
                Preconditions.checkNotNull(elementType, "Element type must not be null.");
    }

    public ArrayType(DataType elementType) {
        this(true, elementType);
    }

    public DataType getElementType() {
        return elementType;
    }

    public DataType newElementType(DataType newElementType) {
        return new ArrayType(isNullable(), newElementType);
    }

    @Override
    public int defaultSize() {
        return elementType.defaultSize();
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new ArrayType(isNullable, elementType.copy());
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, elementType.asSQLString());
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "ARRAY" : "ARRAY NOT NULL");
        generator.writeFieldName("element");
        elementType.serializeJson(generator);
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
        ArrayType arrayType = (ArrayType) o;
        return elementType.equals(arrayType.elementType);
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
        ArrayType arrayType = (ArrayType) o;
        return elementType.equalsIgnoreFieldId(arrayType.elementType);
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
        ArrayType arrayType = (ArrayType) o;
        return elementType.isPrunedFrom(arrayType.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
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
