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
 * Partition field in Iceberg's partition spec.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#partition-specs">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergPartitionField {

    // not sure why, but the sample in Iceberg spec is like this
    public static final int FIRST_FIELD_ID = 1000;

    private static final String FIELD_NAME = "name";
    private static final String FIELD_TRANSFORM = "transform";
    private static final String FIELD_SOURCE_ID = "source-id";
    private static final String FIELD_FIELD_ID = "field-id";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_TRANSFORM)
    private final String transform;

    @JsonProperty(FIELD_SOURCE_ID)
    private final int sourceId;

    @JsonProperty(FIELD_FIELD_ID)
    private final int fieldId;

    public IcebergPartitionField(IcebergDataField dataField, int fieldId) {
        this(
                dataField.name(),
                // currently Paimon's partition value does not have any transformation
                "identity",
                dataField.id(),
                fieldId);
    }

    @JsonCreator
    public IcebergPartitionField(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_TRANSFORM) String transform,
            @JsonProperty(FIELD_SOURCE_ID) int sourceId,
            @JsonProperty(FIELD_FIELD_ID) int fieldId) {
        this.name = name;
        this.transform = transform;
        this.sourceId = sourceId;
        this.fieldId = fieldId;
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_TRANSFORM)
    public String transform() {
        return transform;
    }

    @JsonGetter(FIELD_SOURCE_ID)
    public int sourceId() {
        return sourceId;
    }

    @JsonGetter(FIELD_FIELD_ID)
    public int fieldId() {
        return fieldId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, transform, sourceId, fieldId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergPartitionField)) {
            return false;
        }

        IcebergPartitionField that = (IcebergPartitionField) o;
        return Objects.equals(name, that.name)
                && Objects.equals(transform, that.transform)
                && sourceId == that.sourceId
                && fieldId == that.fieldId;
    }
}
