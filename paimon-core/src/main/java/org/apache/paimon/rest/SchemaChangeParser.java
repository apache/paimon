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

package org.apache.paimon.rest;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Parser for schema change. */
public class SchemaChangeParser {
    private static final String ACTION = "action";

    static final String SET_OPTION = "set-option";
    static final String REMOVE_OPTION = "remove-option";
    static final String UPDATE_COMMENT = "update-comment";
    static final String ADD_COLUMN = "add-column";
    static final String RENAME_COLUMN = "rename-column";
    static final String DROP_COLUMN = "drop-column";
    static final String UPDATE_COLUMN_TYPE = "update-column-type";
    static final String UPDATE_COLUMN_NULLABILITY = "update-column-nullability";
    static final String UPDATE_COLUMN_COMMENT = "update-column-comment";
    static final String UPDATE_COLUMN_POSITION = "update-column-position";

    private static final String FIELD_FILED_NAMES = "fieldNames";
    private static final String FIELD_DATA_TYPE = "dataType";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_MOVE = "move";
    // move
    private static final String FIELD_FILED_NAME = "fieldName";
    private static final String FIELD_REFERENCE_FIELD_NAME = "referenceFieldName";
    private static final String FIELD_TYPE = "type";

    private static final Map<Class<? extends SchemaChange>, String> ACTIONS =
            ImmutableMap.<Class<? extends SchemaChange>, String>builder()
                    .put(SchemaChange.AddColumn.class, ADD_COLUMN)
                    .build();

    public static void toJson(SchemaChange schemaChange, JsonGenerator generator)
            throws IOException {
        String updateAction = ACTIONS.get(schemaChange.getClass());

        // Provide better exception message than the NPE thrown by writing null for the change
        // action,
        // which is required
        checkArgument(
                updateAction != null,
                "Cannot convert schema change to json. Unrecognized schema change type: %s",
                schemaChange.getClass().getName());

        generator.writeStartObject();
        generator.writeStringField(ACTION, updateAction);

        switch (updateAction) {
            case ADD_COLUMN:
                writeAddColumn((SchemaChange.AddColumn) schemaChange, generator);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot convert metadata update to json. Unrecognized action: %s",
                                updateAction));
        }

        generator.writeEndObject();
    }

    public static SchemaChange fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, SchemaChangeParser::fromJson);
    }

    public static SchemaChange fromJson(JsonNode jsonNode) {
        checkArgument(
                jsonNode != null && jsonNode.isObject(),
                "Cannot parse schema change from non-object value: %s",
                jsonNode);
        checkArgument(
                jsonNode.hasNonNull(ACTION), "Cannot parse schema change. Missing field: action");
        String action = jsonNode.get(ACTION).asText().toLowerCase(Locale.ROOT);

        switch (action) {
            case ADD_COLUMN:
                return readAddColumn(jsonNode);
            default:
                throw new UnsupportedOperationException(
                        String.format("Cannot convert schema change action to json: %s", action));
        }
    }

    private static SchemaChange readAddColumn(JsonNode node) {
        Iterator<JsonNode> fieldNamesJson = node.get(FIELD_FILED_NAMES).elements();
        List<String> filedNames = new ArrayList<>();
        while (fieldNamesJson.hasNext()) {
            filedNames.add(fieldNamesJson.next().asText());
        }
        JsonNode dataTypeJson = node.get(FIELD_DATA_TYPE);
        DataType dataType = DataTypeJsonParser.parseDataType(dataTypeJson);
        String comment = node.has(FIELD_COMMENT) ? node.get(FIELD_COMMENT).asText() : null;
        SchemaChange.Move move = node.has(FIELD_MOVE) ? readMove(node.get(FIELD_MOVE)) : null;
        return SchemaChange.addColumn(filedNames.toArray(new String[0]), dataType, comment, move);
    }

    private static SchemaChange.Move readMove(JsonNode node) {
        String filedName = node.get(FIELD_FILED_NAME).asText();
        String referenceFieldName = node.get(FIELD_REFERENCE_FIELD_NAME).asText();
        SchemaChange.Move.MoveType type =
                SchemaChange.Move.MoveType.valueOf(node.get(FIELD_TYPE).asText());
        return new SchemaChange.Move(filedName, referenceFieldName, type);
    }

    private static void writeAddColumn(SchemaChange.AddColumn schemaChange, JsonGenerator generator)
            throws IOException {
        generator.writeArrayFieldStart(FIELD_FILED_NAMES);
        for (String fieldName : schemaChange.fieldNames()) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
        generator.writeFieldName(FIELD_DATA_TYPE);
        schemaChange.dataType().serializeJson(generator);
        if (schemaChange.description() != null) {
            generator.writeStringField(FIELD_COMMENT, schemaChange.description());
        }
        if (schemaChange.move() != null) {
            generator.writeFieldName(FIELD_MOVE);
            writeMove(schemaChange.move(), generator);
        }
    }

    private static void writeMove(SchemaChange.Move move, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeStringField(FIELD_FILED_NAME, move.fieldName());
        generator.writeStringField(FIELD_REFERENCE_FIELD_NAME, move.referenceFieldName());
        generator.writeStringField(FIELD_TYPE, move.type().name());
        generator.writeEndObject();
    }
}
