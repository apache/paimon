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

package org.apache.paimon.format.json;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.text.AbstractTextFileReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON file reader. */
public class JsonFileReader extends AbstractTextFileReader {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    private final JsonOptions options;

    public JsonFileReader(
            FileIO fileIO,
            Path filePath,
            RowType rowType,
            JsonOptions options,
            long offset,
            @Nullable Long length)
            throws IOException {
        super(fileIO, filePath, rowType, options.getLineDelimiter(), offset, length);
        this.options = options;
    }

    @Override
    protected InternalRow parseLine(String line) throws IOException {
        try {
            JsonNode jsonNode = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(line);
            return (InternalRow) convertJsonValue(jsonNode, rowType, options);
        } catch (JsonProcessingException e) {
            if (options.ignoreParseErrors()) {
                return null;
            } else {
                throw new IOException("Failed to parse JSON line: " + line, e);
            }
        } catch (RuntimeException e) {
            if (options.ignoreParseErrors()) {
                return null;
            } else {
                throw new IOException("Failed to convert JSON line: " + line, e);
            }
        }
    }

    private Object convertJsonValue(JsonNode node, DataType dataType, JsonOptions options) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                try {
                    return BASE64_DECODER.decode(node.asText());
                } catch (Exception e) {
                    return handleParseError(e);
                }
            case ARRAY:
                return convertJsonArray(node, (ArrayType) dataType, options);
            case MAP:
                return convertJsonMap(node, (MapType) dataType, options);
            case ROW:
                return convertJsonRow(node, (RowType) dataType, options);
            default:
                return convertPrimitiveStringToType(node.asText(), dataType, options);
        }
    }

    private GenericArray convertJsonArray(
            JsonNode arrayNode, ArrayType arrayType, JsonOptions options) {
        if (!arrayNode.isArray()) {
            return handleParseError(
                    new RuntimeException(
                            "Expected array node but got: " + arrayNode.getNodeType()));
        }

        int size = arrayNode.size();
        List<Object> elements = new ArrayList<>(size); // Pre-allocate capacity
        DataType elementType = arrayType.getElementType();

        for (int i = 0; i < size; i++) {
            Object element;
            try {
                element = convertJsonValue(arrayNode.get(i), elementType, options);
                elements.add(element);
            } catch (Exception e) {
                Object elementValue = handleParseError(e);
                elements.add(elementValue);
            }
        }
        return new GenericArray(elements.toArray());
    }

    private GenericMap convertJsonMap(JsonNode objectNode, MapType mapType, JsonOptions options) {
        if (!objectNode.isObject()) {
            return handleParseError(
                    new IllegalArgumentException(
                            "Expected object node but got: " + objectNode.getNodeType()));
        }

        Map<Object, Object> map = new LinkedHashMap<>(objectNode.size());
        JsonOptions.MapNullKeyMode mapNullKeyMode = options.getMapNullKeyMode();
        String mapNullKeyLiteral = options.getMapNullKeyLiteral();
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        objectNode
                .fields()
                .forEachRemaining(
                        field -> {
                            try {
                                String keyStr = field.getKey();

                                // Handle null keys based on the configured mode
                                if (keyStr == null) {
                                    switch (mapNullKeyMode) {
                                        case DROP:
                                            return; // Skip this entry
                                        case FAIL:
                                            if (options.ignoreParseErrors()) {
                                                return; // Skip null keys when ignoring errors
                                            }
                                            throw new RuntimeException(
                                                    "Null map key encountered and map-null-key-mode is set to FAIL.");
                                        case LITERAL:
                                            // Will be handled below in key conversion
                                            break;
                                        default:
                                            throw new IllegalStateException(
                                                    "Unknown MapNullKeyMode: " + mapNullKeyMode);
                                    }
                                }

                                // Convert the key
                                Object key;
                                if (keyStr == null) {
                                    // Only LITERAL mode reaches here for null keys
                                    key =
                                            convertPrimitiveStringToType(
                                                    mapNullKeyLiteral, keyType, options);
                                } else {
                                    key = convertPrimitiveStringToType(keyStr, keyType, options);
                                }

                                // Add the entry to the map if key is not null
                                if (key != null) {
                                    Object value =
                                            convertJsonValue(field.getValue(), valueType, options);
                                    map.put(key, value);
                                }
                            } catch (Exception e) {
                                // Use the error handling method for consistency
                                handleParseError(e);
                            }
                        });
        return new GenericMap(map);
    }

    private Object convertPrimitiveStringToType(
            String str, DataType dataType, JsonOptions options) {
        try {
            switch (dataType.getTypeRoot()) {
                case TINYINT:
                    return Byte.parseByte(str);
                case SMALLINT:
                    return Short.parseShort(str);
                case INTEGER:
                    return Integer.parseInt(str);
                case BIGINT:
                    return Long.parseLong(str);
                case FLOAT:
                    return Float.parseFloat(str);
                case DOUBLE:
                    return Double.parseDouble(str);
                case BOOLEAN:
                    return Boolean.parseBoolean(str);
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(str);
                default:
                    BinaryString binaryString = BinaryString.fromString(str);
                    CastExecutor cast = CastExecutors.resolve(DataTypes.STRING(), dataType);
                    return cast.cast(binaryString);
            }
        } catch (Exception e) {
            return handleParseError(e);
        }
    }

    private GenericRow convertJsonRow(JsonNode objectNode, RowType rowType, JsonOptions options) {
        if (!objectNode.isObject()) {
            return handleParseError(
                    new IllegalArgumentException(
                            "Expected object node but got: " + objectNode.getNodeType()));
        }

        List<DataField> fields = rowType.getFields();
        int fieldCount = fields.size();
        Object[] values = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataField field = fields.get(i);
            try {
                values[i] = convertJsonValue(objectNode.get(field.name()), field.type(), options);
            } catch (Exception e) {
                values[i] = handleParseError(e);
            }
        }
        return GenericRow.of(values);
    }

    /**
     * Handles parse errors based on the ignoreParseErrors option.
     *
     * @param exception The exception that occurred
     * @return null if ignoring errors, otherwise re-throws the exception
     * @throws RuntimeException if not ignoring errors
     */
    private <T> T handleParseError(Exception exception) {
        if (options.ignoreParseErrors()) {
            return null;
        } else {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }
}
