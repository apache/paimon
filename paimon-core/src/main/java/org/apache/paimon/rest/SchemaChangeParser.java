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

import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.ADD_COLUMN;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.DROP_COLUMN;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.REMOVE_OPTION;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.RENAME_COLUMN;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.SET_OPTION;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.UPDATE_COLUMN_COMMENT;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.UPDATE_COLUMN_NULLABILITY;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.UPDATE_COLUMN_TYPE;
import static org.apache.paimon.rest.SchemaChangeParser.SchemaChangeActionEnum.UPDATE_COMMENT;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Parser for schema change. */
public class SchemaChangeParser {
    private static final String ACTION = "action";

    private static final String FIELD_REMOVE_OPTION = "removeOptionKey";
    private static final String FIELD_FILED_NAMES = "fieldNames";
    private static final String FIELD_DATA_TYPE = "dataType";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_MOVE = "move";
    private static final String FIELD_NEW_NAME = "newName";
    private static final String FIELD_NEW_DATA_TYPE = "newDataType";
    private static final String FIELD_KEEP_NULLABILITY = "keepNullability";
    private static final String FIELD_NEW_NULLABILITY = "newNullability";
    // move
    private static final String FIELD_FILED_NAME = "fieldName";
    private static final String FIELD_REFERENCE_FIELD_NAME = "referenceFieldName";
    private static final String FIELD_TYPE = "type";

    private static final Map<Class<? extends SchemaChange>, SchemaChangeActionEnum> ACTIONS =
            ImmutableMap.<Class<? extends SchemaChange>, SchemaChangeActionEnum>builder()
                    .put(SchemaChange.SetOption.class, SET_OPTION)
                    .put(SchemaChange.RemoveOption.class, REMOVE_OPTION)
                    .put(SchemaChange.UpdateComment.class, UPDATE_COMMENT)
                    .put(SchemaChange.AddColumn.class, ADD_COLUMN)
                    .put(SchemaChange.RenameColumn.class, RENAME_COLUMN)
                    .put(SchemaChange.DropColumn.class, DROP_COLUMN)
                    .put(SchemaChange.UpdateColumnType.class, UPDATE_COLUMN_TYPE)
                    .put(SchemaChange.UpdateColumnNullability.class, UPDATE_COLUMN_NULLABILITY)
                    .put(SchemaChange.UpdateColumnComment.class, UPDATE_COLUMN_COMMENT)
                    .put(
                            SchemaChange.UpdateColumnPosition.class,
                            SchemaChangeActionEnum.UPDATE_COLUMN_POSITION)
                    .build();

    public static void toJson(SchemaChange schemaChange, JsonGenerator generator)
            throws IOException {
        SchemaChangeActionEnum updateAction = ACTIONS.get(schemaChange.getClass());

        // Provide better exception message than the NPE thrown by writing null for the change
        // action,
        // which is required
        checkArgument(
                updateAction != null,
                "Cannot convert schema change to json. Unrecognized schema change type: %s",
                schemaChange.getClass().getName());

        generator.writeStartObject();
        generator.writeStringField(ACTION, updateAction.name().toLowerCase(Locale.ROOT));

        switch (updateAction) {
            case SET_OPTION:
                writeSetOption((SchemaChange.SetOption) schemaChange, generator);
                break;
            case REMOVE_OPTION:
                writeRemoveOption((SchemaChange.RemoveOption) schemaChange, generator);
                break;
            case UPDATE_COMMENT:
                writeUpdateComment((SchemaChange.UpdateComment) schemaChange, generator);
                break;
            case ADD_COLUMN:
                writeAddColumn((SchemaChange.AddColumn) schemaChange, generator);
                break;
            case RENAME_COLUMN:
                writeRenameColumn((SchemaChange.RenameColumn) schemaChange, generator);
                break;
            case DROP_COLUMN:
                writeDropColumn((SchemaChange.DropColumn) schemaChange, generator);
                break;
            case UPDATE_COLUMN_TYPE:
                writeUpdateColumnType((SchemaChange.UpdateColumnType) schemaChange, generator);
                break;
            case UPDATE_COLUMN_NULLABILITY:
                writeUpdateColumnNullability(
                        (SchemaChange.UpdateColumnNullability) schemaChange, generator);
                break;
            case UPDATE_COLUMN_COMMENT:
                writeUpdateColumnComment(
                        (SchemaChange.UpdateColumnComment) schemaChange, generator);
                break;
            case UPDATE_COLUMN_POSITION:
                writeUpdateColumnPosition(
                        (SchemaChange.UpdateColumnPosition) schemaChange, generator);
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
        SchemaChangeActionEnum action =
                SchemaChangeActionEnum.fromString(jsonNode.get(ACTION).asText());

        switch (action) {
            case SET_OPTION:
                return readSetOption(jsonNode);
            case REMOVE_OPTION:
                return readRemoveOption(jsonNode);
            case UPDATE_COMMENT:
                return readUpdateComment(jsonNode);
            case ADD_COLUMN:
                return readAddColumn(jsonNode);
            case RENAME_COLUMN:
                return readRenameColumn(jsonNode);
            case DROP_COLUMN:
                return readDropColumn(jsonNode);
            case UPDATE_COLUMN_TYPE:
                return readUpdateColumnType(jsonNode);
            case UPDATE_COLUMN_NULLABILITY:
                return readUpdateColumnNullability(jsonNode);
            case UPDATE_COLUMN_COMMENT:
                return readUpdateColumnComment(jsonNode);
            case UPDATE_COLUMN_POSITION:
                return readUpdateColumnPosition(jsonNode);
            default:
                throw new UnsupportedOperationException(
                        String.format("Cannot convert schema change action to json: %s", action));
        }
    }

    /** Schema change action type. */
    public enum SchemaChangeActionEnum {
        SET_OPTION,
        REMOVE_OPTION,
        UPDATE_COMMENT,
        ADD_COLUMN,
        RENAME_COLUMN,
        DROP_COLUMN,
        UPDATE_COLUMN_TYPE,
        UPDATE_COLUMN_NULLABILITY,
        UPDATE_COLUMN_COMMENT,
        UPDATE_COLUMN_POSITION;

        public static SchemaChangeActionEnum fromString(String action) {
            return SchemaChangeActionEnum.valueOf(action.toUpperCase());
        }
    }

    private static void writeSetOption(SchemaChange.SetOption schemaChange, JsonGenerator generator)
            throws IOException {
        generator.writeStringField(schemaChange.key(), schemaChange.value());
    }

    private static void writeRemoveOption(
            SchemaChange.RemoveOption schemaChange, JsonGenerator generator) throws IOException {
        generator.writeStringField(FIELD_REMOVE_OPTION, schemaChange.key());
    }

    private static void writeUpdateComment(
            SchemaChange.UpdateComment schemaChange, JsonGenerator generator) throws IOException {
        generator.writeStringField(FIELD_COMMENT, schemaChange.comment());
    }

    private static void writeAddColumn(SchemaChange.AddColumn schemaChange, JsonGenerator generator)
            throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
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

    private static void writeFiledNames(String[] fieldNames, JsonGenerator generator)
            throws IOException {
        generator.writeArrayFieldStart(FIELD_FILED_NAMES);
        for (String fieldName : fieldNames) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
    }

    private static void writeMove(SchemaChange.Move move, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeStringField(FIELD_FILED_NAME, move.fieldName());
        generator.writeStringField(FIELD_REFERENCE_FIELD_NAME, move.referenceFieldName());
        generator.writeStringField(FIELD_TYPE, move.type().name());
        generator.writeEndObject();
    }

    private static void writeRenameColumn(
            SchemaChange.RenameColumn schemaChange, JsonGenerator generator) throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
        generator.writeStringField(FIELD_NEW_NAME, schemaChange.newName());
    }

    private static void writeDropColumn(
            SchemaChange.DropColumn schemaChange, JsonGenerator generator) throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
    }

    private static void writeUpdateColumnType(
            SchemaChange.UpdateColumnType schemaChange, JsonGenerator generator)
            throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
        generator.writeFieldName(FIELD_NEW_DATA_TYPE);
        schemaChange.newDataType().serializeJson(generator);
        generator.writeBooleanField(FIELD_KEEP_NULLABILITY, schemaChange.keepNullability());
    }

    private static void writeUpdateColumnNullability(
            SchemaChange.UpdateColumnNullability schemaChange, JsonGenerator generator)
            throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
        generator.writeBooleanField(FIELD_NEW_NULLABILITY, schemaChange.newNullability());
    }

    private static void writeUpdateColumnComment(
            SchemaChange.UpdateColumnComment schemaChange, JsonGenerator generator)
            throws IOException {
        writeFiledNames(schemaChange.fieldNames(), generator);
        generator.writeStringField(FIELD_COMMENT, schemaChange.newDescription());
    }

    private static void writeUpdateColumnPosition(
            SchemaChange.UpdateColumnPosition schemaChange, JsonGenerator generator)
            throws IOException {
        if (schemaChange.move() != null) {
            generator.writeFieldName(FIELD_MOVE);
            writeMove(schemaChange.move(), generator);
        }
    }

    private static SchemaChange readSetOption(JsonNode node) {
        Iterator<String> fieldNames = node.fieldNames();
        String key = null;
        String value = null;
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            if (ACTION.equals(fieldName)) {
                continue;
            }
            key = fieldName;
            value = node.get(fieldName).asText();
        }
        return SchemaChange.setOption(key, value);
    }

    private static SchemaChange readRemoveOption(JsonNode node) {
        return SchemaChange.removeOption(node.get(FIELD_REMOVE_OPTION).asText());
    }

    private static SchemaChange readUpdateComment(JsonNode node) {
        String comment = node.has(FIELD_COMMENT) ? node.get(FIELD_COMMENT).asText() : null;
        return SchemaChange.updateComment(comment);
    }

    private static SchemaChange readAddColumn(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        JsonNode dataTypeJson = node.get(FIELD_DATA_TYPE);
        DataType dataType = DataTypeJsonParser.parseDataType(dataTypeJson);
        String comment = node.has(FIELD_COMMENT) ? node.get(FIELD_COMMENT).asText() : null;
        SchemaChange.Move move = node.has(FIELD_MOVE) ? readMove(node.get(FIELD_MOVE)) : null;
        return SchemaChange.addColumn(fieldNames, dataType, comment, move);
    }

    private static SchemaChange.Move readMove(JsonNode node) {
        String filedName = node.get(FIELD_FILED_NAME).asText();
        String referenceFieldName = node.get(FIELD_REFERENCE_FIELD_NAME).asText();
        SchemaChange.Move.MoveType type =
                SchemaChange.Move.MoveType.valueOf(node.get(FIELD_TYPE).asText());
        return new SchemaChange.Move(filedName, referenceFieldName, type);
    }

    private static SchemaChange readRenameColumn(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        String newName = node.get(FIELD_NEW_NAME).asText();
        return SchemaChange.renameColumn(fieldNames, newName);
    }

    private static SchemaChange readDropColumn(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        return SchemaChange.dropColumn(fieldNames);
    }

    private static SchemaChange readUpdateColumnType(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        JsonNode dataTypeJson = node.get(FIELD_NEW_DATA_TYPE);
        DataType newDataType = DataTypeJsonParser.parseDataType(dataTypeJson);
        boolean keepNullability =
                node.has(FIELD_KEEP_NULLABILITY)
                        ? node.get(FIELD_KEEP_NULLABILITY).asBoolean()
                        : false;
        return SchemaChange.updateColumnType(fieldNames, newDataType, keepNullability);
    }

    private static SchemaChange readUpdateColumnNullability(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        boolean newNullability = node.get(FIELD_NEW_NULLABILITY).asBoolean();
        return SchemaChange.updateColumnNullability(fieldNames, newNullability);
    }

    private static SchemaChange readUpdateColumnComment(JsonNode node) {
        String[] fieldNames = getFieldNames(node);
        String comment = node.get(FIELD_COMMENT).asText();
        return SchemaChange.updateColumnComment(fieldNames, comment);
    }

    private static SchemaChange readUpdateColumnPosition(JsonNode node) {
        SchemaChange.Move move = node.has(FIELD_MOVE) ? readMove(node.get(FIELD_MOVE)) : null;
        return SchemaChange.updateColumnPosition(move);
    }

    private static String[] getFieldNames(JsonNode jsonNode) {
        Iterator<JsonNode> fieldNamesJson = jsonNode.get(FIELD_FILED_NAMES).elements();
        List<String> filedNames = new ArrayList<>();
        while (fieldNamesJson.hasNext()) {
            filedNames.add(fieldNamesJson.next().asText());
        }
        return filedNames.toArray(new String[0]);
    }
}
