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
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static org.apache.paimon.utils.EncodingUtils.escapeIdentifier;
import static org.apache.paimon.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Defines the field of a row type.
 *
 * @since 0.4.0
 */
@Public
public final class DataField implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int id;
    private final String name;
    private final DataType type;
    private final @Nullable String description;
    private final @Nullable String defaultValue;

    public DataField(int id, String name, DataType dataType) {
        this(id, name, dataType, null, null);
    }

    public DataField(int id, String name, DataType dataType, @Nullable String description) {
        this(id, name, dataType, description, null);
    }

    public DataField(
            int id,
            String name,
            DataType type,
            @Nullable String description,
            @Nullable String defaultValue) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public DataType type() {
        return type;
    }

    public DataField newId(int newId) {
        return new DataField(newId, name, type, description, defaultValue);
    }

    public DataField newName(String newName) {
        return new DataField(id, newName, type, description, defaultValue);
    }

    public DataField newType(DataType newType) {
        return new DataField(id, name, newType, description, defaultValue);
    }

    public DataField newDescription(String newDescription) {
        return new DataField(id, name, type, defaultValue, newDescription);
    }

    public DataField newDefaultValue(String newDefaultValue) {
        return new DataField(id, name, type, newDefaultValue, description);
    }

    @Nullable
    public String description() {
        return description;
    }

    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    public DataField copy() {
        return new DataField(id, name, type.copy(), description, defaultValue);
    }

    public DataField copy(boolean isNullable) {
        return new DataField(id, name, type.copy(isNullable), description, defaultValue);
    }

    public String asSQLString() {
        StringBuilder sb = new StringBuilder();
        sb.append(escapeIdentifier(name)).append(" ").append(type.asSQLString());
        if (StringUtils.isNotEmpty(description)) {
            sb.append(" COMMENT '").append(escapeSingleQuotes(description)).append("'");
        }
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(defaultValue);
        }
        return sb.toString();
    }

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("id", id());
        generator.writeStringField("name", name());
        generator.writeFieldName("type");
        type.serializeJson(generator);
        if (description() != null) {
            generator.writeStringField("description", description());
        }
        if (defaultValue() != null) {
            generator.writeStringField("defaultValue", defaultValue());
        }
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
        DataField field = (DataField) o;
        return Objects.equals(id, field.id)
                && Objects.equals(name, field.name)
                && Objects.equals(type, field.type)
                && Objects.equals(description, field.description)
                && Objects.equals(defaultValue, field.defaultValue);
    }

    public boolean equalsIgnoreFieldId(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(name, other.name)
                && type.equalsIgnoreFieldId(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    public boolean isPrunedFrom(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(id, other.id)
                && Objects.equals(name, other.name)
                && type.isPrunedFrom(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, description, defaultValue);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    /**
     * When the order of the same field is different, its ID may also be different, so the
     * comparison should not include the ID.
     */
    public static boolean dataFieldEqualsIgnoreId(DataField dataField1, DataField dataField2) {
        if (dataField1 == dataField2) {
            return true;
        } else if (dataField1 != null && dataField2 != null) {
            return Objects.equals(dataField1.name(), dataField2.name())
                    && Objects.equals(dataField1.type(), dataField2.type())
                    && Objects.equals(dataField1.description(), dataField2.description())
                    && Objects.equals(dataField1.defaultValue(), dataField2.defaultValue());
        } else {
            return false;
        }
    }
}
