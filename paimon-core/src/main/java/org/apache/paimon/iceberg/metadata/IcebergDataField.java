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

import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link DataField} in Iceberg.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#schemas">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergDataField {

    private static final String FIELD_ID = "id";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_REQUIRED = "required";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DOC = "doc";

    @JsonProperty(FIELD_ID)
    private final int id;

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_REQUIRED)
    private final boolean required;

    @JsonProperty(FIELD_TYPE)
    private final Object type;

    @JsonIgnore private final DataType dataType;

    @JsonProperty(FIELD_DOC)
    private final String doc;

    public IcebergDataField(DataField dataField) {
        this(
                dataField.id(),
                dataField.name(),
                !dataField.type().isNullable(),
                toTypeObject(dataField.type(), dataField.id(), 0),
                dataField.type(),
                dataField.description());
    }

    @JsonCreator
    public IcebergDataField(
            @JsonProperty(FIELD_ID) int id,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_REQUIRED) boolean required,
            @JsonProperty(FIELD_TYPE) Object type,
            @JsonProperty(FIELD_DOC) String doc) {
        this(id, name, required, type, null, doc);
    }

    public IcebergDataField(
            int id, String name, boolean required, Object type, DataType dataType, String doc) {
        this.id = id;
        this.name = name;
        this.required = required;
        this.type = type;
        this.dataType = dataType;
        this.doc = doc;
    }

    @JsonGetter(FIELD_ID)
    public int id() {
        return id;
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_REQUIRED)
    public boolean required() {
        return required;
    }

    @JsonGetter(FIELD_TYPE)
    public Object type() {
        return type;
    }

    @JsonGetter(FIELD_DOC)
    public String doc() {
        return doc;
    }

    @JsonIgnore
    public DataType dataType() {
        return Preconditions.checkNotNull(dataType);
    }

    private static Object toTypeObject(DataType dataType, int fieldId, int depth) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return "boolean";
            case INTEGER:
                return "int";
            case BIGINT:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case CHAR:
            case VARCHAR:
                return "string";
            case BINARY:
            case VARBINARY:
                return "binary";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "decimal(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int timestampPrecision = ((TimestampType) dataType).getPrecision();
                Preconditions.checkArgument(
                        timestampPrecision > 3 && timestampPrecision <= 6,
                        "Paimon Iceberg compatibility only support timestamp type with precision from 4 to 6.");
                return "timestamp";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int timestampLtzPrecision = ((LocalZonedTimestampType) dataType).getPrecision();
                Preconditions.checkArgument(
                        timestampLtzPrecision > 3 && timestampLtzPrecision <= 6,
                        "Paimon Iceberg compatibility only support timestamp type with precision from 4 to 6.");
                return "timestamptz";
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                return new IcebergListType(
                        SpecialFields.getArrayElementFieldId(fieldId, depth + 1),
                        !dataType.isNullable(),
                        toTypeObject(arrayType.getElementType(), fieldId, depth + 1));
            case MAP:
                MapType mapType = (MapType) dataType;
                return new IcebergMapType(
                        SpecialFields.getMapKeyFieldId(fieldId, depth + 1),
                        toTypeObject(mapType.getKeyType(), fieldId, depth + 1),
                        SpecialFields.getMapValueFieldId(fieldId, depth + 1),
                        !mapType.getValueType().isNullable(),
                        toTypeObject(mapType.getValueType(), fieldId, depth + 1));
            case ROW:
                RowType rowType = (RowType) dataType;
                return new IcebergStructType(
                        rowType.getFields().stream()
                                .map(IcebergDataField::new)
                                .collect(Collectors.toList()));
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, required, type, doc);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergDataField)) {
            return false;
        }

        IcebergDataField that = (IcebergDataField) o;
        return id == that.id
                && Objects.equals(name, that.name)
                && required == that.required
                && Objects.equals(type, that.type)
                && Objects.equals(doc, that.doc);
    }
}
