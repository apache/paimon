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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    /** Paimon column IDs are 0-based; Iceberg IDs must be >= 1, so every ID is shifted by one. */
    private static final int ID_OFFSET = 1;

    /**
     * Builds the Iceberg schema for a Paimon table schema.
     *
     * <p>Iceberg field IDs must be positive, but Paimon assigns a single 0-based counter to both
     * top-level and nested fields (see {@code Schema.Builder#column}). Every Paimon ID - top-level
     * and nested ROW/ARRAY/MAP/MULTISET alike - is shifted by {@link #ID_OFFSET} so the whole
     * emitted Iceberg schema uses one consistent, positive ID space. The same mapping therefore
     * applies to the manifest header, the table metadata schemas, the partition source IDs and the
     * manifest metrics maps, which are all derived from this schema.
     */
    public static IcebergSchema create(TableSchema tableSchema) {
        return new IcebergSchema(
                (int) tableSchema.id(),
                tableSchema.fields().stream()
                        .map(IcebergSchema::positiveField)
                        .collect(Collectors.toList()));
    }

    private static IcebergDataField positiveField(DataField field) {
        return new IcebergDataField(shiftIds(field));
    }

    private static DataField shiftIds(DataField field) {
        return new DataField(
                field.id() + ID_OFFSET, field.name(), shiftIds(field.type()), field.description());
    }

    private static DataType shiftIds(DataType type) {
        switch (type.getTypeRoot()) {
            case ROW:
                RowType rowType = (RowType) type;
                return new RowType(
                        rowType.isNullable(),
                        rowType.getFields().stream()
                                .map(IcebergSchema::shiftIds)
                                .collect(Collectors.toList()));
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return new ArrayType(arrayType.isNullable(), shiftIds(arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) type;
                return new MapType(
                        mapType.isNullable(),
                        shiftIds(mapType.getKeyType()),
                        shiftIds(mapType.getValueType()));
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return new MultisetType(
                        multisetType.isNullable(), shiftIds(multisetType.getElementType()));
            default:
                return type;
        }
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
