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

    public static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

    public static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

    private final int id;

    private final String name;

    private final DataType type;

    private final @Nullable String description;

    public DataField(int id, String name, DataType dataType) {
        this(id, name, dataType, null);
    }

    public DataField(int id, String name, DataType type, @Nullable String description) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
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

    public DataField newId(int newid) {
        return new DataField(newid, name, type, description);
    }

    public DataField newName(String newName) {
        return new DataField(id, newName, type, description);
    }

    public DataField newType(DataType newType) {
        return new DataField(id, name, newType, description);
    }

    public DataField newDescription(String newDescription) {
        return new DataField(id, name, type, newDescription);
    }

    @Nullable
    public String description() {
        return description;
    }

    public DataField copy() {
        return new DataField(id, name, type.copy(), description);
    }

    public DataField copy(boolean isNullable) {
        return new DataField(id, name, type.copy(isNullable), description);
    }

    public String asSQLString() {
        return formatString(type.asSQLString());
    }

    private String formatString(String typeString) {
        if (description == null) {
            return String.format(FIELD_FORMAT_NO_DESCRIPTION, escapeIdentifier(name), typeString);
        } else {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION,
                    escapeIdentifier(name),
                    typeString,
                    escapeSingleQuotes(description));
        }
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
                && Objects.equals(description, field.description);
    }

    public boolean isPrunedFrom(DataField field) {
        if (this == field) {
            return true;
        }
        if (field == null) {
            return false;
        }
        return Objects.equals(id, field.id)
                && Objects.equals(name, field.name)
                && type.isPrunedFrom(field.type)
                && Objects.equals(description, field.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, description);
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
                    && Objects.equals(dataField1.description(), dataField2.description());
        } else {
            return false;
        }
    }
}
