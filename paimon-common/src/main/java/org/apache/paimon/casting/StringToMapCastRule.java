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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/** {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeRoot#MAP} cast rule. */
public class StringToMapCastRule extends AbstractCastRule<BinaryString, InternalMap> {

    static final StringToMapCastRule INSTANCE = new StringToMapCastRule();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private StringToMapCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.MAP)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalMap> create(DataType inputType, DataType targetType) {
        MapType mapType = (MapType) targetType;
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        return str -> {
            if (str == null) {
                return null;
            }

            String s = str.toString();
            if (s == null || s.trim().isEmpty()) {
                return new GenericMap(new HashMap<>());
            }

            Map<Object, Object> resultMap = new HashMap<>();

            // Try JSON parsing first
            try {
                JsonNode mapNode = OBJECT_MAPPER.readTree(s);
                if (mapNode.isObject()) {
                    mapNode.fields()
                            .forEachRemaining(
                                    entry -> {
                                        String keyStr = entry.getKey();
                                        JsonNode valueNode = entry.getValue();

                                        // Cast key
                                        CastExecutor<?, ?> keyCastExecutor =
                                                CastExecutors.resolve(
                                                        VarCharType.STRING_TYPE, keyType);
                                        Object key = null;
                                        if (keyCastExecutor != null) {
                                            @SuppressWarnings("unchecked")
                                            CastExecutor<BinaryString, Object> castExecutor =
                                                    (CastExecutor<BinaryString, Object>)
                                                            keyCastExecutor;
                                            key =
                                                    castExecutor.cast(
                                                            BinaryString.fromString(keyStr));
                                        }

                                        // Cast value
                                        Object value = null;
                                        if (!valueNode.isNull()) {
                                            String valueStr;
                                            if (valueNode.isTextual()) {
                                                valueStr = valueNode.asText();
                                            } else {
                                                valueStr = valueNode.toString();
                                            }

                                            CastExecutor<?, ?> valueCastExecutor =
                                                    CastExecutors.resolve(
                                                            VarCharType.STRING_TYPE, valueType);
                                            if (valueCastExecutor != null) {
                                                @SuppressWarnings("unchecked")
                                                CastExecutor<BinaryString, Object> castExecutor =
                                                        (CastExecutor<BinaryString, Object>)
                                                                valueCastExecutor;
                                                value =
                                                        castExecutor.cast(
                                                                BinaryString.fromString(valueStr));
                                            }
                                        }

                                        resultMap.put(key, value);
                                    });
                    return new GenericMap(resultMap);
                }
            } catch (JsonProcessingException e) {
                // Fall through to manual parsing
            }

            // Manual parsing for Paimon's map string format: {key1 -> value1, key2 -> value2}
            String trimmed = s.trim();
            if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
                throw new IllegalArgumentException("Map must start with '{' and end with '}'");
            }

            String content = trimmed.substring(1, trimmed.length() - 1).trim();
            if (content.isEmpty()) {
                return new GenericMap(new HashMap<>());
            }

            // Handle Paimon's map format: key1 -> value1, key2 -> value2
            if (content.contains(" -> ")) {
                String[] pairs = content.split(",");
                for (String pair : pairs) {
                    String[] keyValue = pair.trim().split(" -> ", 2);
                    if (keyValue.length == 2) {
                        String keyStr = keyValue[0].trim();
                        String valueStr = keyValue[1].trim();

                        // Handle null values
                        Object key = "null".equals(keyStr) ? null : parseValue(keyStr, keyType);
                        Object value =
                                "null".equals(valueStr) ? null : parseValue(valueStr, valueType);

                        resultMap.put(key, value);
                    }
                }
            }
            // Handle CSV-style format: {key1=value1, key2=value2}
            else if (content.contains("=")) {
                String[] pairs = content.split(",");
                for (String pair : pairs) {
                    String[] keyValue = pair.trim().split("=", 2);
                    if (keyValue.length == 2) {
                        String keyStr = keyValue[0].trim();
                        String valueStr = keyValue[1].trim();

                        Object key = parseValue(keyStr, keyType);
                        Object value = parseValue(valueStr, valueType);

                        resultMap.put(key, value);
                    }
                }
            }
            // Handle colon-separated format: key1:value1,key2:value2
            else if (content.contains(":")) {
                String[] pairs = content.split(",");
                for (String pair : pairs) {
                    String[] keyValue = pair.trim().split(":", 2);
                    if (keyValue.length == 2) {
                        String keyStr = keyValue[0].trim();
                        String valueStr = keyValue[1].trim();

                        Object key = parseValue(keyStr, keyType);
                        Object value = parseValue(valueStr, valueType);

                        resultMap.put(key, value);
                    }
                }
            }

            return new GenericMap(resultMap);
        };
    }

    private Object parseValue(String valueStr, DataType targetType) {
        CastExecutor<?, ?> castExecutor =
                CastExecutors.resolve(VarCharType.STRING_TYPE, targetType);
        if (castExecutor != null) {
            @SuppressWarnings("unchecked")
            CastExecutor<BinaryString, Object> typedExecutor =
                    (CastExecutor<BinaryString, Object>) castExecutor;
            return typedExecutor.cast(BinaryString.fromString(valueStr));
        } else {
            // Fallback for unsupported types
            return BinaryString.fromString(valueStr);
        }
    }
}
