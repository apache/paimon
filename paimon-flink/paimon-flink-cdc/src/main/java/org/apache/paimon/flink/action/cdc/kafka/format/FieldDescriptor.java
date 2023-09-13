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

package org.apache.paimon.flink.action.cdc.kafka.format;

import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.Objects;

/** An abstract immutable representation of cdc record field. */
public abstract class FieldDescriptor<T> implements Serializable {

    protected final T schema;
    protected final String name;
    protected final boolean key;
    protected String columnName;
    protected String typeName;
    protected Integer length;
    protected Integer scale;
    protected DataType paimonType;

    public FieldDescriptor(T schema, String name, boolean key) {
        this.schema = schema;
        this.name = name;
        this.key = key;
    }

    public abstract DataType toPaimonDataType();

    public T getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isKey() {
        return key;
    }

    public DataType getPaimonType() {
        return paimonType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldDescriptor<?> other = (FieldDescriptor<?>) o;
        return Objects.equals(schema, other.schema)
                && Objects.equals(name, other.name)
                && Objects.equals(key, other.key)
                && Objects.equals(columnName, other.columnName)
                && Objects.equals(typeName, other.typeName)
                && Objects.equals(length, other.length)
                && Objects.equals(scale, other.scale)
                && Objects.equals(paimonType, other.paimonType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, key, columnName, typeName, length, scale, paimonType);
    }

    @Override
    public String toString() {
        return "FieldDescriptor{"
                + "schema="
                + schema
                + ", name='"
                + name
                + '\''
                + ", key="
                + key
                + ", columnName='"
                + columnName
                + '\''
                + ", typeName='"
                + typeName
                + '\''
                + ", length="
                + length
                + '\''
                + ", scale="
                + scale
                + '\''
                + ", paimonType="
                + paimonType
                + '}';
    }
}
