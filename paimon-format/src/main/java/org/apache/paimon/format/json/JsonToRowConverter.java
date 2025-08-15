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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Converter to convert JSON nodes to Paimon internal rows. */
public class JsonToRowConverter {

    private final RowType rowType;
    private final String mapNullKeyMode;
    private final String mapNullKeyLiteral;

    public JsonToRowConverter(RowType rowType, Options options) {
        this.rowType = rowType;
        this.mapNullKeyMode = options.get(JsonFileFormat.JSON_MAP_NULL_KEY_MODE);
        this.mapNullKeyLiteral = options.get(JsonFileFormat.JSON_MAP_NULL_KEY_LITERAL);
    }

    public InternalRow convert(JsonNode jsonNode) {
        return (InternalRow) convertValue(jsonNode, rowType);
    }

    private Object convertValue(JsonNode node, DataType dataType) {
        if (node == null || node.isNull()) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
                return node.asBoolean();
            case TINYINT:
                return (byte) node.asInt();
            case SMALLINT:
                return (short) node.asInt();
            case INTEGER:
                return node.asInt();
            case BIGINT:
                return node.asLong();
            case FLOAT:
                return (float) node.asDouble();
            case DOUBLE:
                return node.asDouble();
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(node.asText());
            case BINARY:
            case VARBINARY:
                // Assume base64 encoded bytes
                try {
                    return java.util.Base64.getDecoder().decode(node.asText());
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(
                            "Failed to decode base64 binary data: " + node.asText(), e);
                }
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                BigDecimal bigDecimal = new BigDecimal(node.asText());
                return Decimal.fromBigDecimal(
                        bigDecimal, decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return (int) LocalDate.parse(node.asText()).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (LocalTime.parse(node.asText()).toNanoOfDay() / 1_000_000);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TypeUtils.castFromString(node.asText(), dataType);
            case ARRAY:
                return convertArray(node, (ArrayType) dataType);
            case MAP:
                return convertMap(node, (MapType) dataType);
            case ROW:
                return convertRow(node, (RowType) dataType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private GenericArray convertArray(JsonNode arrayNode, ArrayType arrayType) {
        if (!arrayNode.isArray()) {
            throw new RuntimeException("Expected array node but got: " + arrayNode.getNodeType());
        }

        DataType elementType = arrayType.getElementType();
        List<Object> elements = new ArrayList<>();

        for (JsonNode elementNode : arrayNode) {
            elements.add(convertValue(elementNode, elementType));
        }

        return new GenericArray(elements.toArray());
    }

    private GenericMap convertMap(JsonNode objectNode, MapType mapType) {
        if (!objectNode.isObject()) {
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        Map<Object, Object> map = new HashMap<>();

        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String keyStr = field.getKey();
            JsonNode valueNode = field.getValue();

            Object key;
            if (keyStr == null) {
                switch (mapNullKeyMode) {
                    case "FAIL":
                        throw new RuntimeException(
                                "Null map key encountered and map-null-key-mode is set to FAIL. "
                                        + "To resolve this, set map-null-key-mode to 'DROP' to skip null keys, "
                                        + "or 'LITERAL' and provide a replacement value using map-null-key-literal.");
                    case "DROP":
                        continue; // Skip this entry
                    case "LITERAL":
                        key = convertStringToType(mapNullKeyLiteral, keyType);
                        break;
                    default:
                        throw new RuntimeException("Unknown map-null-key-mode: " + mapNullKeyMode);
                }
            } else {
                key = convertStringToType(keyStr, keyType);
            }

            Object value = convertValue(valueNode, valueType);
            map.put(key, value);
        }

        return new GenericMap(map);
    }

    private Object convertStringToType(String str, DataType dataType) {
        // Simple string to type conversion for map keys
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(str);
            case INTEGER:
                return Integer.parseInt(str);
            case BIGINT:
                return Long.parseLong(str);
            case FLOAT:
                return Float.parseFloat(str);
            case DOUBLE:
                return Double.parseDouble(str);
            default:
                return BinaryString.fromString(str);
        }
    }

    private GenericRow convertRow(JsonNode objectNode, RowType rowType) {
        if (!objectNode.isObject()) {
            throw new RuntimeException("Expected object node but got: " + objectNode.getNodeType());
        }

        List<DataField> fields = rowType.getFields();
        Object[] values = new Object[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            JsonNode fieldNode = objectNode.get(field.name());
            values[i] = convertValue(fieldNode, field.type());
        }

        return GenericRow.of(values);
    }
}
