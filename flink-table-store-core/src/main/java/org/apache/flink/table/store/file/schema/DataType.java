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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** Describes the data type in the table store ecosystem. */
public abstract class DataType implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final LogicalType logicalType;

    DataType(LogicalType logicalType) {
        this.logicalType =
                Preconditions.checkNotNull(logicalType, "Logical type must not be null.");
    }

    /**
     * Returns the corresponding logical type.
     *
     * @return a parameterized instance of {@link LogicalType}
     */
    public LogicalType logicalType() {
        return logicalType;
    }

    public static DataType copy(DataType type, boolean isNullable) {
        if (type instanceof AtomicDataType) {
            return new AtomicDataType(type.logicalType.copy(isNullable));
        } else if (type instanceof ArrayDataType) {
            return new ArrayDataType(isNullable, ((ArrayDataType) type).elementType());
        } else if (type instanceof MultisetDataType) {
            return new MultisetDataType(isNullable, ((MultisetDataType) type).elementType());
        } else if (type instanceof MapDataType) {
            return new MapDataType(
                    isNullable, ((MapDataType) type).keyType(), ((MapDataType) type).valueType());
        } else if (type instanceof RowDataType) {
            return new RowDataType(isNullable, ((RowDataType) type).fields());
        }
        throw new UnsupportedOperationException("Unknown type: " + type.logicalType.getClass());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType dataType = (DataType) o;
        return Objects.equals(logicalType, dataType.logicalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalType);
    }
}
