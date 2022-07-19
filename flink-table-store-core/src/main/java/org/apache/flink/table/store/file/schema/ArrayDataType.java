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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.types.logical.ArrayType;

import java.util.Objects;

/** A data type for array. */
public class ArrayDataType extends DataType {

    private static final long serialVersionUID = 1L;

    private final DataType elementType;

    public ArrayDataType(boolean isNullable, DataType elementType) {
        super(new ArrayType(isNullable, elementType.logicalType));
        this.elementType = elementType;
    }

    public DataType elementType() {
        return elementType;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new ArrayDataType(isNullable, elementType);
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
        ArrayDataType that = (ArrayDataType) o;
        return Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
    }
}
