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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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
    private final String type;

    @JsonProperty(FIELD_DOC)
    private final String doc;

    public IcebergDataField(DataField dataField, int bias) {
        this(
                dataField.id() + bias,
                dataField.name(),
                !dataField.type().isNullable(),
                toTypeString(dataField.type()),
                dataField.description());
    }

    @JsonCreator
    public IcebergDataField(
            @JsonProperty(FIELD_ID) int id,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_REQUIRED) boolean required,
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_DOC) String doc) {
        this.id = id;
        this.name = name;
        this.required = required;
        this.type = type;
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
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_DOC)
    public String doc() {
        return doc;
    }

    private static String toTypeString(DataType dataType) {
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
