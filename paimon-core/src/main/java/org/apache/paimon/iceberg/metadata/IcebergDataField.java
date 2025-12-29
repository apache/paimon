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
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

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
    @JsonDeserialize(using = IcebergDataTypeDeserializer.class)
    private final Object type;

    @JsonIgnore private DataType dataType;

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
        if (dataType != null) {
            return dataType;
        }

        dataType = getDataTypeFromType(type, required);
        return Preconditions.checkNotNull(dataType);
    }

    private static Object toTypeObject(DataType dataType, int fieldId, int depth) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return "boolean";
            case INTEGER:
            case TINYINT:
            case SMALLINT:
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
                        timestampPrecision >= 3 && timestampPrecision <= 9,
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 9.");
                return timestampPrecision >= 7 ? "timestamp_ns" : "timestamp";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int timestampLtzPrecision = ((LocalZonedTimestampType) dataType).getPrecision();
                Preconditions.checkArgument(
                        timestampLtzPrecision >= 3 && timestampLtzPrecision <= 9,
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 9.");
                return timestampLtzPrecision >= 7 ? "timestamptz_ns" : "timestamptz";
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

    private DataType getDataTypeFromType(Object icebergType, boolean isRequired) {
        if (icebergType instanceof String) {
            // for primitive type
            String simpleType = icebergType.toString();
            String delimiter = "(";
            if (simpleType.contains("[")) {
                delimiter = "[";
            }
            String typePrefix =
                    !simpleType.contains(delimiter)
                            ? simpleType
                            : simpleType.substring(0, simpleType.indexOf(delimiter));
            switch (typePrefix) {
                case "boolean":
                    return new BooleanType(!isRequired);
                case "int":
                    return new IntType(!isRequired);
                case "long":
                    return new BigIntType(!isRequired);
                case "float":
                    return new FloatType(!isRequired);
                case "double":
                    return new DoubleType(!isRequired);
                case "date":
                    return new DateType(!isRequired);
                case "string":
                    return new VarCharType(!isRequired, VarCharType.MAX_LENGTH);
                case "binary":
                    return new VarBinaryType(!isRequired, VarBinaryType.MAX_LENGTH);
                case "fixed":
                    int fixedLength =
                            Integer.parseInt(
                                    simpleType.substring(
                                            simpleType.indexOf("[") + 1, simpleType.indexOf("]")));
                    return new BinaryType(!isRequired, fixedLength);
                case "uuid":
                    // https://iceberg.apache.org/spec/?h=vector#primitive-types
                    // uuid should use 16-byte fixed
                    return new BinaryType(!isRequired, 16);
                case "decimal":
                    int precision =
                            Integer.parseInt(
                                    simpleType.substring(
                                            simpleType.indexOf("(") + 1, simpleType.indexOf(",")));
                    int scale =
                            Integer.parseInt(
                                    simpleType.substring(
                                            simpleType.indexOf(",") + 2, simpleType.indexOf(")")));
                    return new DecimalType(!isRequired, precision, scale);
                case "timestamp":
                    return new TimestampType(!isRequired, 6);
                case "timestamptz":
                    return new LocalZonedTimestampType(!isRequired, 6);
                case "timestamp_ns": // iceberg v3 format
                    return new TimestampType(!isRequired, 9);
                case "timestamptz_ns": // iceberg v3 format
                    return new LocalZonedTimestampType(!isRequired, 9);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported primitive data type: " + icebergType);
            }
        } else {
            // for nested type
            if (icebergType instanceof IcebergListType) {
                IcebergListType listType = (IcebergListType) icebergType;
                return new ArrayType(
                        !isRequired,
                        getDataTypeFromType(listType.element(), listType.elementRequired()));
            } else if (icebergType instanceof IcebergMapType) {
                IcebergMapType mapType = (IcebergMapType) icebergType;
                return new MapType(
                        !isRequired,
                        getDataTypeFromType(mapType.key(), true),
                        getDataTypeFromType(mapType.value(), mapType.valueRequired()));
            } else if (icebergType instanceof IcebergStructType) {
                IcebergStructType structType = (IcebergStructType) icebergType;
                return new RowType(
                        !isRequired,
                        structType.fields().stream()
                                .map(IcebergDataField::toDatafield)
                                .collect(Collectors.toList()));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported nested data type: " + icebergType.getClass());
            }
        }
    }

    public DataField toDatafield() {
        return new DataField(id, name, dataType(), doc);
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
