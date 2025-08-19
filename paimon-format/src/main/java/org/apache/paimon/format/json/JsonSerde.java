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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON Serde for Paimon Json format. */
public class JsonSerde {

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    // JSON to Row conversion
    public static InternalRow convertJsonStringToRow(String line, RowType rowType, Options options)
            throws JsonProcessingException {
        JsonNode jsonNode = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(line);
        return (InternalRow) convertJsonValue(jsonNode, rowType, options);
    }

    private static Object convertJsonValue(JsonNode node, DataType dataType, Options options) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                try {
                    return BASE64_DECODER.decode(node.asText());
                } catch (IllegalArgumentException e) {
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
                return convertPrimitiveStringToType(node.asText(), dataType);
        }
    }

    private static GenericArray convertJsonArray(
            JsonNode arrayNode, ArrayType arrayType, Options options) {
        if (!arrayNode.isArray()) {
            throw new RuntimeException("Expected array node but got: " + arrayNode.getNodeType());
        }

        int size = arrayNode.size();
        List<Object> elements = new ArrayList<>(size); // Pre-allocate capacity
        DataType elementType = arrayType.getElementType();

        for (int i = 0; i < size; i++) {
            elements.add(convertJsonValue(arrayNode.get(i), elementType, options));
        }
        return new GenericArray(elements.toArray());
    }

    private static GenericMap convertJsonMap(
            JsonNode objectNode, MapType mapType, Options options) {
        if (!objectNode.isObject()) {
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        int estimatedSize = Math.max(16, objectNode.size()); // Pre-allocate with estimated size
        Map<Object, Object> map = new HashMap<>(estimatedSize);
        String mapNullKeyMode = options.get(JsonFileFormat.JSON_MAP_NULL_KEY_MODE);
        String mapNullKeyLiteral = options.get(JsonFileFormat.JSON_MAP_NULL_KEY_LITERAL);
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        objectNode
                .fields()
                .forEachRemaining(
                        field -> {
                            String keyStr = field.getKey();
                            if (keyStr == null && "DROP".equals(mapNullKeyMode)) {
                                return;
                            }

                            Object key =
                                    keyStr == null
                                            ? ("LITERAL".equals(mapNullKeyMode)
                                                    ? convertPrimitiveStringToType(
                                                            mapNullKeyLiteral, keyType)
                                                    : null)
                                            : convertPrimitiveStringToType(keyStr, keyType);

                            if (key == null && "FAIL".equals(mapNullKeyMode)) {
                                throw new RuntimeException(
                                        "Null map key encountered and map-null-key-mode is set to FAIL.");
                            }

                            if (key != null) {
                                map.put(
                                        key,
                                        convertJsonValue(field.getValue(), valueType, options));
                            }
                        });
        return new GenericMap(map);
    }

    private static Object convertPrimitiveStringToType(String str, DataType dataType) {
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
    }

    private static GenericRow convertJsonRow(
            JsonNode objectNode, RowType rowType, Options options) {
        if (!objectNode.isObject()) {
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        List<DataField> fields = rowType.getFields();
        int fieldCount = fields.size();
        Object[] values = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataField field = fields.get(i);
            values[i] = convertJsonValue(objectNode.get(field.name()), field.type(), options);
        }
        return GenericRow.of(values);
    }

    // Row to JSON conversion
    public static String convertRowToJsonString(InternalRow row, RowType rowType)
            throws JsonProcessingException {
        Map<String, Object> result = convertRowToMap(row, rowType);
        return JsonSerdeUtil.writeValueAsString(result);
    }

    private static Map<String, Object> convertRowToMap(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        int fieldCount = fields.size();
        Map<String, Object> result = new LinkedHashMap<>(fieldCount); // Pre-allocate capacity

        for (int i = 0; i < fieldCount; i++) {
            DataField field = fields.get(i);
            Object value = InternalRowUtils.get(row, i, field.type());
            result.put(field.name(), convertRowValue(value, field.type()));
        }
        return result;
    }

    private static Object convertRowValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case CHAR:
            case VARCHAR:
                return value.toString();
            case BINARY:
            case VARBINARY:
                return BASE64_ENCODER.encodeToString((byte[]) value);
            case ARRAY:
                return convertRowArray((InternalArray) value, (ArrayType) dataType);
            case MAP:
                return convertRowMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRowToMap((InternalRow) value, (RowType) dataType);
            default:
                CastExecutor cast = CastExecutors.resolveToString(dataType);
                return cast.cast(value).toString();
        }
    }

    private static List<Object> convertRowArray(InternalArray array, ArrayType arrayType) {
        int size = array.size();
        List<Object> result = new ArrayList<>(size); // Pre-allocate capacity
        DataType elementType = arrayType.getElementType();

        for (int i = 0; i < size; i++) {
            result.add(convertRowValue(InternalRowUtils.get(array, i, elementType), elementType));
        }
        return result;
    }

    private static Map<String, Object> convertRowMap(InternalMap map, MapType mapType) {
        int size = map.size();
        Map<String, Object> result = new LinkedHashMap<>(size); // Pre-allocate capacity
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        for (int i = 0; i < size; i++) {
            Object key = InternalRowUtils.get(keyArray, i, keyType);
            Object value = InternalRowUtils.get(valueArray, i, valueType);
            result.put(convertToString(key, keyType), convertRowValue(value, valueType));
        }
        return result;
    }

    private static String convertToString(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        if (typeRoot == DataTypeRoot.CHAR || typeRoot == DataTypeRoot.VARCHAR) {
            return ((BinaryString) value).toString();
        }
        return value.toString();
    }
}
