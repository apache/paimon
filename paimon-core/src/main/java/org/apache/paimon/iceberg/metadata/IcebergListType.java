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
 * {@link org.apache.paimon.types.ArrayType} in Iceberg.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#schemas">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergListType {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_ELEMENT_ID = "element-id";
    private static final String FIELD_ELEMENT_REQUIRED = "element-required";
    private static final String FIELD_ELEMENT = "element";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_ELEMENT_ID)
    private final int elementId;

    @JsonProperty(FIELD_ELEMENT_REQUIRED)
    private final boolean elementRequired;

    @JsonProperty(FIELD_ELEMENT)
    private final Object element;

    public IcebergListType(int elementId, boolean elementRequired, Object element) {
        this("list", elementId, elementRequired, element);
    }

    @JsonCreator
    public IcebergListType(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_ELEMENT_ID) int elementId,
            @JsonProperty(FIELD_ELEMENT_REQUIRED) boolean elementRequired,
            @JsonProperty(FIELD_ELEMENT) Object element) {
        this.type = type;
        this.elementId = elementId;
        this.elementRequired = elementRequired;
        this.element = element;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_ELEMENT_ID)
    public int elementId() {
        return elementId;
    }

    @JsonGetter(FIELD_ELEMENT_REQUIRED)
    public boolean elementRequired() {
        return elementRequired;
    }

    @JsonGetter(FIELD_ELEMENT)
    public Object element() {
        return element;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, elementId, elementRequired, element);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergListType)) {
            return false;
        }

        IcebergListType that = (IcebergListType) o;
        return Objects.equals(type, that.type)
                && elementId == that.elementId
                && elementRequired == that.elementRequired
                && Objects.equals(element, that.element);
    }
}
