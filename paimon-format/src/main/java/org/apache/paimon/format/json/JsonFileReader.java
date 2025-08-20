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
import org.apache.paimon.format.BaseTextFileReader;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** High-performance JSON file reader implementation with optimized buffering. */
public class JsonFileReader extends BaseTextFileReader {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    // Map null key mode constants
    private static final String MAP_NULL_KEY_MODE_DROP = "DROP";
    private static final String MAP_NULL_KEY_MODE_LITERAL = "LITERAL";
    private static final String MAP_NULL_KEY_MODE_FAIL = "FAIL";

    private final JsonOptions options;

    public JsonFileReader(FileIO fileIO, Path filePath, RowType rowType, JsonOptions options)
            throws IOException {
        super(fileIO, filePath, rowType);
        this.options = options;
    }

    @Override
    protected BaseTextRecordIterator createRecordIterator() {
        return new JsonRecordIterator();
    }

    @Override
    protected InternalRow parseLine(String line) throws IOException {
        try {
            return convertJsonStringToRow(line, rowType, options);
        } catch (JsonProcessingException e) {
            if (options.ignoreParseErrors()) {
                return null;
            } else {
                throw new IOException("Failed to parse JSON line: " + line, e);
            }
        } catch (RuntimeException e) {
            if (options.ignoreParseErrors()) {
                // Follow Spark behavior: ignore runtime errors during JSON conversion and return
                // null
                return null;
            } else {
                throw new IOException("Failed to convert JSON line: " + line, e);
            }
        }
    }

    private InternalRow convertJsonStringToRow(String line, RowType rowType, JsonOptions options)
            throws JsonProcessingException {
        JsonNode jsonNode = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(line);
        return (InternalRow) convertJsonValue(jsonNode, rowType, options);
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
                } catch (IllegalArgumentException e) {
                    if (options.ignoreParseErrors()) {
                        // Follow Spark behavior: return null for invalid base64 data when ignoring
                        // errors
                        return null;
                    }
                    throw new RuntimeException(
                            "Failed to decode base64 binary data: " + node.asText(), e);
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
            if (options.ignoreParseErrors()) {
                // Follow Spark behavior: return null for non-array nodes when ignoring errors
                return null;
            }
            throw new RuntimeException("Expected array node but got: " + arrayNode.getNodeType());
        }

        int size = arrayNode.size();
        List<Object> elements = new ArrayList<>(size); // Pre-allocate capacity
        DataType elementType = arrayType.getElementType();

        for (int i = 0; i < size; i++) {
            try {
                elements.add(convertJsonValue(arrayNode.get(i), elementType, options));
            } catch (RuntimeException e) {
                if (options.ignoreParseErrors()) {
                    // Follow Spark behavior: add null for elements that fail to convert
                    elements.add(null);
                } else {
                    throw e;
                }
            }
        }
        return new GenericArray(elements.toArray());
    }

    private GenericMap convertJsonMap(JsonNode objectNode, MapType mapType, JsonOptions options) {
        if (!objectNode.isObject()) {
            if (options.ignoreParseErrors()) {
                // Follow Spark behavior: return null for non-object nodes when ignoring errors
                return null;
            }
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        int estimatedSize = Math.max(16, objectNode.size()); // Pre-allocate with estimated size
        Map<Object, Object> map = new HashMap<>(estimatedSize);
        String mapNullKeyMode = options.getMapNullKeyMode();
        String mapNullKeyLiteral = options.getMapNullKeyLiteral();
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        objectNode
                .fields()
                .forEachRemaining(
                        field -> {
                            try {
                                String keyStr = field.getKey();
                                if (keyStr == null
                                        && MAP_NULL_KEY_MODE_DROP.equals(mapNullKeyMode)) {
                                    return;
                                }

                                Object key =
                                        keyStr == null
                                                ? (MAP_NULL_KEY_MODE_LITERAL.equals(mapNullKeyMode)
                                                        ? convertPrimitiveStringToType(
                                                                mapNullKeyLiteral, keyType, options)
                                                        : null)
                                                : convertPrimitiveStringToType(
                                                        keyStr, keyType, options);

                                if (key == null && MAP_NULL_KEY_MODE_FAIL.equals(mapNullKeyMode)) {
                                    if (options.ignoreParseErrors()) {
                                        // Follow Spark behavior: skip null keys when ignoring
                                        // errors
                                        return;
                                    }
                                    throw new RuntimeException(
                                            "Null map key encountered and map-null-key-mode is set to FAIL.");
                                }

                                if (key != null) {
                                    Object value =
                                            convertJsonValue(field.getValue(), valueType, options);
                                    map.put(key, value);
                                }
                            } catch (RuntimeException e) {
                                if (!options.ignoreParseErrors()) {
                                    throw e;
                                }
                                // Follow Spark behavior: skip entries that fail to convert when
                                // ignoring errors
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
            if (options.ignoreParseErrors()) {
                // Follow Spark behavior: return null for conversion errors when ignoring errors
                return null;
            }
            throw new RuntimeException("Failed to convert string '" + str + "' to " + dataType, e);
        }
    }

    // Overloaded method for backward compatibility
    private Object convertPrimitiveStringToType(String str, DataType dataType) {
        return convertPrimitiveStringToType(str, dataType, options);
    }

    private GenericRow convertJsonRow(JsonNode objectNode, RowType rowType, JsonOptions options) {
        if (!objectNode.isObject()) {
            if (options.ignoreParseErrors()) {
                // Follow Spark behavior: return null for non-object nodes when ignoring errors
                return null;
            }
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        List<DataField> fields = rowType.getFields();
        int fieldCount = fields.size();
        Object[] values = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataField field = fields.get(i);
            try {
                values[i] = convertJsonValue(objectNode.get(field.name()), field.type(), options);
            } catch (RuntimeException e) {
                if (options.ignoreParseErrors()) {
                    // Follow Spark behavior: set null for fields that fail to convert
                    values[i] = null;
                } else {
                    throw e;
                }
            }
        }
        return GenericRow.of(values);
    }

    private class JsonRecordIterator extends BaseTextRecordIterator {
        // Inherits all functionality from BaseTextRecordIterator
        // No additional JSON-specific iterator logic needed
    }
}
