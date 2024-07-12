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

import java.util.List;
import java.util.Objects;

/**
 * Partition spec in Iceberg's metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#partition-specs">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergPartitionSpec {

    // always 0, Paimon does not support partition evolution
    public static final int SPEC_ID = 0;

    private static final String FIELD_SPEC_ID = "spec-id";
    private static final String FIELD_FIELDS = "fields";

    @JsonProperty(FIELD_SPEC_ID)
    private final int specId;

    @JsonProperty(FIELD_FIELDS)
    private final List<IcebergPartitionField> fields;

    public IcebergPartitionSpec(List<IcebergPartitionField> fields) {
        this(SPEC_ID, fields);
    }

    @JsonCreator
    public IcebergPartitionSpec(
            @JsonProperty(FIELD_SPEC_ID) int specId,
            @JsonProperty(FIELD_FIELDS) List<IcebergPartitionField> fields) {
        this.specId = specId;
        this.fields = fields;
    }

    @JsonGetter(FIELD_SPEC_ID)
    public int specId() {
        return specId;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<IcebergPartitionField> fields() {
        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(specId, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergPartitionSpec)) {
            return false;
        }

        IcebergPartitionSpec that = (IcebergPartitionSpec) o;
        return specId == that.specId && Objects.equals(fields, that.fields);
    }
}
