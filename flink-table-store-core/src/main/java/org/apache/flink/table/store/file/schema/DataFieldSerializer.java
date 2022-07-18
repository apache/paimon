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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.store.file.utils.JsonSerializer;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** A {@link JsonSerializer} for {@link DataField}. */
public class DataFieldSerializer implements JsonSerializer<DataField> {

    public static final DataFieldSerializer INSTANCE = new DataFieldSerializer();

    @Override
    public void serialize(DataField dataField, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("id", dataField.id());
        generator.writeStringField("name", dataField.name());
        generator.writeFieldName("type");
        typeToJson(dataField.type(), generator);
        if (dataField.description() != null) {
            generator.writeStringField("description", dataField.description());
        }
        generator.writeEndObject();
    }

    @Override
    public DataField deserialize(JsonNode node) {
        int id = node.get("id").asInt();
        String name = node.get("name").asText();
        DataType type = typeFromJson(node.get("type"));
        JsonNode descriptionNode = node.get("description");
        String description = null;
        if (descriptionNode != null) {
            description = descriptionNode.asText();
        }
        return new DataField(id, name, type, description);
    }

    private static void typeToJson(DataType type, JsonGenerator generator) throws IOException {
        if (type instanceof AtomicDataType) {
            generator.writeString(type.logicalType.asSerializableString());
        } else if (type instanceof ArrayDataType) {
            generator.writeStartObject();
            generator.writeStringField(
                    "type", type.logicalType.isNullable() ? "ARRAY" : "ARRAY NOT NULL");
            generator.writeFieldName("element");
            typeToJson(((ArrayDataType) type).elementType(), generator);
            generator.writeEndObject();
        } else if (type instanceof MultisetDataType) {
            generator.writeStartObject();
            generator.writeStringField(
                    "type", type.logicalType.isNullable() ? "MULTISET" : "MULTISET NOT NULL");
            generator.writeFieldName("element");
            typeToJson(((MultisetDataType) type).elementType(), generator);
            generator.writeEndObject();
        } else if (type instanceof MapDataType) {
            generator.writeStartObject();
            generator.writeStringField(
                    "type", type.logicalType.isNullable() ? "MAP" : "MAP NOT NULL");
            generator.writeFieldName("key");
            typeToJson(((MapDataType) type).keyType(), generator);
            generator.writeFieldName("value");
            typeToJson(((MapDataType) type).valueType(), generator);
            generator.writeEndObject();
        } else if (type instanceof RowDataType) {
            generator.writeStartObject();
            generator.writeStringField(
                    "type", type.logicalType.isNullable() ? "ROW" : "ROW NOT NULL");
            generator.writeArrayFieldStart("fields");
            for (DataField field : ((RowDataType) type).fields()) {
                INSTANCE.serialize(field, generator);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DataType typeFromJson(JsonNode json) {
        if (json.isTextual()) {
            return new AtomicDataType(LogicalTypeParser.parse(json.asText()));
        } else if (json.isObject()) {
            String typeString = json.get("type").asText();
            if (typeString.startsWith("ARRAY")) {
                DataType element = typeFromJson(json.get("element"));
                return new ArrayDataType(!typeString.contains("NOT NULL"), element);
            } else if (typeString.startsWith("MULTISET")) {
                DataType element = typeFromJson(json.get("element"));
                return new MultisetDataType(!typeString.contains("NOT NULL"), element);
            } else if (typeString.startsWith("MAP")) {
                DataType key = typeFromJson(json.get("key"));
                DataType value = typeFromJson(json.get("value"));
                return new MapDataType(!typeString.contains("NOT NULL"), key, value);
            } else if (typeString.startsWith("ROW")) {
                JsonNode fieldArray = json.get("fields");
                Iterator<JsonNode> iterator = fieldArray.elements();
                List<DataField> fields = new ArrayList<>(fieldArray.size());
                while (iterator.hasNext()) {
                    fields.add(INSTANCE.deserialize(iterator.next()));
                }
                return new RowDataType(!typeString.contains("NOT NULL"), fields);
            }
        }

        throw new IllegalArgumentException("Can not parse: " + json);
    }
}
