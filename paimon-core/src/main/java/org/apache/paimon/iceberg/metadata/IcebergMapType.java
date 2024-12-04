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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * {@link org.apache.paimon.types.MapType} in Iceberg.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#schemas">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergMapType {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_KEY_ID = "key-id";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE_ID = "value-id";
    private static final String FIELD_VALUE_REQUIRED = "value-required";
    private static final String FIELD_VALUE = "value";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_KEY_ID)
    private final int keyId;

    @JsonProperty(FIELD_KEY)
    private final Object key;

    @JsonProperty(FIELD_VALUE_ID)
    private final int valueId;

    @JsonProperty(FIELD_VALUE_REQUIRED)
    private final boolean valueRequired;

    @JsonProperty(FIELD_VALUE)
    private final Object value;

    public IcebergMapType(int keyId, Object key, int valueId, boolean valueRequired, Object value) {
        this("map", keyId, key, valueId, valueRequired, value);
    }

    @JsonCreator
    public IcebergMapType(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_KEY_ID) int keyId,
            @JsonProperty(FIELD_KEY) Object key,
            @JsonProperty(FIELD_VALUE_ID) int valueId,
            @JsonProperty(FIELD_VALUE_REQUIRED) boolean valueRequired,
            @JsonProperty(FIELD_VALUE) Object value) {
        this.type = type;
        this.keyId = keyId;
        this.key = key;
        this.valueId = valueId;
        this.valueRequired = valueRequired;
        this.value = value;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_KEY_ID)
    public int keyId() {
        return keyId;
    }

    @JsonGetter(FIELD_KEY)
    public Object key() {
        return key;
    }

    @JsonGetter(FIELD_VALUE_ID)
    public int valueId() {
        return valueId;
    }

    @JsonGetter(FIELD_VALUE_REQUIRED)
    public boolean valueRequired() {
        return valueRequired;
    }

    @JsonGetter(FIELD_VALUE)
    public Object value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keyId, key, valueId, valueRequired, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergMapType)) {
            return false;
        }
        IcebergMapType that = (IcebergMapType) o;
        return Objects.equals(type, that.type)
                && keyId == that.keyId
                && Objects.equals(key, that.key)
                && valueId == that.valueId
                && valueRequired == that.valueRequired
                && Objects.equals(value, that.value);
    }
}
