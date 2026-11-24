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

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Schema in Iceberg's metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#schemas">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSchema {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_SCHEMA_ID = "schema-id";
    private static final String FIELD_FIELDS = "fields";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final int schemaId;

    @JsonProperty(FIELD_FIELDS)
    private final List<IcebergDataField> fields;

    public static IcebergSchema create(TableSchema tableSchema) {
        return new IcebergSchema(
                (int) tableSchema.id(),
                tableSchema.fields().stream()
                        .map(IcebergDataField::new)
                        .collect(Collectors.toList()));
    }

    public IcebergSchema(int schemaId, List<IcebergDataField> fields) {
        this("struct", schemaId, fields);
    }

    @JsonCreator
    public IcebergSchema(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_SCHEMA_ID) int schemaId,
            @JsonProperty(FIELD_FIELDS) List<IcebergDataField> fields) {
        this.type = type;
        this.schemaId = schemaId;
        this.fields = fields;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public int schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<IcebergDataField> fields() {
        return fields;
    }

    @JsonIgnore
    public int highestFieldId() {
        return fields.stream().mapToInt(IcebergDataField::id).max().orElse(0);
    }

    /** Checks the IDs emitted for both named fields and synthetic collection fields. */
    public void validateFieldIds() {
        validateFieldIds(new IcebergStructType(fields), new HashSet<>());
    }

    private static void validateFieldIds(Object type, Set<Integer> ids) {
        if (type instanceof IcebergStructType) {
            for (IcebergDataField field : ((IcebergStructType) type).fields()) {
                addFieldId(field.id(), ids);
                validateFieldIds(field.type(), ids);
            }
        } else if (type instanceof IcebergListType) {
            IcebergListType list = (IcebergListType) type;
            addFieldId(list.elementId(), ids);
            validateFieldIds(list.element(), ids);
        } else if (type instanceof IcebergMapType) {
            IcebergMapType map = (IcebergMapType) type;
            addFieldId(map.keyId(), ids);
            validateFieldIds(map.key(), ids);
            addFieldId(map.valueId(), ids);
            validateFieldIds(map.value(), ids);
        }
    }

    private static void addFieldId(int id, Set<Integer> ids) {
        checkArgument(
                ids.add(id),
                "Cannot publish Iceberg metadata with duplicate field ID %s. "
                        + "Use a schema with unique Iceberg field IDs or disable metadata.iceberg.storage.",
                id);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, schemaId, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSchema)) {
            return false;
        }

        IcebergSchema that = (IcebergSchema) o;
        return Objects.equals(type, that.type)
                && schemaId == that.schemaId
                && Objects.equals(fields, that.fields);
    }
}
