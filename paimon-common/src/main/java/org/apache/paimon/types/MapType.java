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
 * Data type of an associative array that maps keys (including {@code NULL}) to values (including
 * {@code NULL}). A map cannot contain duplicate keys; each key can map to at most one value. There
 * is no restriction of key types; it is the responsibility of the user to ensure uniqueness. The
 * map type is an extension to the SQL standard.
 *
 * @since 0.4.0
 */
@Public
public class MapType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String FORMAT = "MAP<%s, %s>";

    private final DataType keyType;

    private final DataType valueType;

    public MapType(boolean isNullable, DataType keyType, DataType valueType) {
        super(isNullable, DataTypeRoot.MAP);
        this.keyType = Preconditions.checkNotNull(keyType, "Key type must not be null.");
        this.valueType = Preconditions.checkNotNull(valueType, "Value type must not be null.");
    }

    public MapType(DataType keyType, DataType valueType) {
        this(true, keyType, valueType);
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    @Override
    public int defaultSize() {
        return keyType.defaultSize() + valueType.defaultSize();
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new MapType(isNullable, keyType.copy(), valueType.copy());
    }

    public DataType newKeyValueType(DataType newKeyType, DataType newValueType) {
        return new MapType(isNullable(), newKeyType, newValueType);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, keyType.asSQLString(), valueType.asSQLString());
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "MAP" : "MAP NOT NULL");
        generator.writeFieldName("key");
        keyType.serializeJson(generator);
        generator.writeFieldName("value");
        valueType.serializeJson(generator);
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
        MapType mapType = (MapType) o;
        return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
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
        MapType mapType = (MapType) o;
        return keyType.equalsIgnoreFieldId(mapType.keyType)
                && valueType.equalsIgnoreFieldId(mapType.valueType);
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
        MapType mapType = (MapType) o;
        return keyType.isPrunedFrom(mapType.keyType) && valueType.isPrunedFrom(mapType.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        keyType.collectFieldIds(fieldIds);
        valueType.collectFieldIds(fieldIds);
    }
}
