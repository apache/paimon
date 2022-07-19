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

import org.apache.flink.table.types.logical.MapType;

import java.util.Objects;

/** A data type that contains a key and value data type. */
public class MapDataType extends DataType {

    private static final long serialVersionUID = 1L;

    private final DataType keyType;

    private final DataType valueType;

    public MapDataType(boolean isNullable, DataType keyType, DataType valueType) {
        super(new MapType(isNullable, keyType.logicalType, valueType.logicalType));
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public DataType keyType() {
        return keyType;
    }

    public DataType valueType() {
        return valueType;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new MapDataType(isNullable, keyType, valueType);
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
        MapDataType that = (MapDataType) o;
        return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
    }
}
