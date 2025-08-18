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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/** {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeRoot#ARRAY} cast rule. */
public class StringToArrayCastRule extends AbstractCastRule<BinaryString, InternalArray> {

    static final StringToArrayCastRule INSTANCE = new StringToArrayCastRule();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private StringToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.ARRAY)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalArray> create(
            DataType inputType, DataType targetType) {
        ArrayType arrayType = (ArrayType) targetType;
        DataType elementType = arrayType.getElementType();

        return str -> {
            if (str == null) {
                return null;
            }

            String s = str.toString();
            if (s == null || s.trim().isEmpty()) {
                return new GenericArray(new Object[0]);
            }

            // Try JSON parsing first
            try {
                JsonNode arrayNode = OBJECT_MAPPER.readTree(s);
                if (arrayNode.isArray()) {
                    List<Object> resultList = new ArrayList<>();
                    for (JsonNode elementNode : arrayNode) {
                        if (elementNode != null && !elementNode.isNull()) {
                            String elementJson;
                            if (elementNode.isTextual()) {
                                elementJson = elementNode.asText();
                            } else {
                                elementJson = elementNode.toString();
                            }
                            CastExecutor<?, ?> elementCastExecutor =
                                    CastExecutors.resolve(VarCharType.STRING_TYPE, elementType);
                            if (elementCastExecutor != null) {
                                BinaryString elementStr = BinaryString.fromString(elementJson);
                                @SuppressWarnings("unchecked")
                                CastExecutor<BinaryString, Object> castExecutor =
                                        (CastExecutor<BinaryString, Object>) elementCastExecutor;
                                Object elementObject = castExecutor.cast(elementStr);
                                resultList.add(elementObject);
                            } else {
                                // Fallback to direct value conversion for complex types
                                if (elementNode.isTextual()) {
                                    resultList.add(BinaryString.fromString(elementNode.asText()));
                                } else {
                                    resultList.add(elementNode.toString());
                                }
                            }
                        } else {
                            resultList.add(null);
                        }
                    }
                    return new GenericArray(resultList.toArray());
                }
            } catch (JsonProcessingException e) {
                // Fall through to manual parsing
            }

            // Manual parsing for non-JSON formats
            String trimmed = s.trim();
            if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
                throw new IllegalArgumentException("Array must start with '[' and end with ']'");
            }

            String content = trimmed.substring(1, trimmed.length() - 1).trim();
            if (content.isEmpty()) {
                return new GenericArray(new Object[0]);
            }

            // Handle arrays of rows with CSV format: [{1, 0.1}, {2, 0.2}]
            if (elementType instanceof RowType) {
                return parseCsvArrayOfRows(content, (RowType) elementType);
            }

            // Handle simple array elements (comma-separated)
            if (elementType instanceof VarCharType) {
                String[] elements = content.split(",");
                BinaryString[] binaryStrings = new BinaryString[elements.length];
                for (int i = 0; i < elements.length; i++) {
                    binaryStrings[i] = BinaryString.fromString(elements[i].trim());
                }
                return new GenericArray(binaryStrings);
            }

            // Handle other element types
            String[] elements = content.split(",");
            List<Object> resultList = new ArrayList<>();
            for (String element : elements) {
                element = element.trim();
                if ("null".equals(element)) {
                    resultList.add(null);
                } else {
                    CastExecutor<?, ?> elementCastExecutor =
                            CastExecutors.resolve(VarCharType.STRING_TYPE, elementType);
                    if (elementCastExecutor != null) {
                        BinaryString elementStr = BinaryString.fromString(element);
                        @SuppressWarnings("unchecked")
                        CastExecutor<BinaryString, Object> castExecutor =
                                (CastExecutor<BinaryString, Object>) elementCastExecutor;
                        Object elementObject = castExecutor.cast(elementStr);
                        resultList.add(elementObject);
                    } else {
                        throw new UnsupportedOperationException(
                                "Cannot cast string to element type: " + elementType);
                    }
                }
            }
            return new GenericArray(resultList.toArray());
        };
    }

    private static GenericArray parseCsvArrayOfRows(String content, RowType rowType) {
        List<Object> rows = new ArrayList<>();
        List<DataField> fields = rowType.getFields();

        // Parse each row: {1, 0.1}, {2, 0.2}
        int braceCount = 0;
        int start = 0;
        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if (ch == '{') {
                braceCount++;
            } else if (ch == '}') {
                braceCount--;
                if (braceCount == 0) {
                    // Found complete row
                    String rowStr = content.substring(start, i + 1).trim();
                    if (rowStr.startsWith("{") && rowStr.endsWith("}")) {
                        // Remove braces and parse values
                        String valuesStr = rowStr.substring(1, rowStr.length() - 1).trim();
                        String[] values = valuesStr.split(",");

                        Object[] rowData = new Object[fields.size()];
                        for (int j = 0; j < Math.min(values.length, fields.size()); j++) {
                            String value = values[j].trim();
                            DataField field = fields.get(j);

                            CastExecutor<?, ?> fieldCastExecutor =
                                    CastExecutors.resolve(VarCharType.STRING_TYPE, field.type());
                            if (fieldCastExecutor != null) {
                                BinaryString valueStr = BinaryString.fromString(value);
                                @SuppressWarnings("unchecked")
                                CastExecutor<BinaryString, Object> castExecutor =
                                        (CastExecutor<BinaryString, Object>) fieldCastExecutor;
                                rowData[j] = castExecutor.cast(valueStr);
                            } else {
                                throw new UnsupportedOperationException(
                                        "Cannot cast string to field type: " + field.type());
                            }
                        }
                        rows.add(GenericRow.of(rowData));
                    }

                    // Move to next row (skip comma and whitespace)
                    start = i + 1;
                    while (start < content.length()
                            && (content.charAt(start) == ','
                                    || Character.isWhitespace(content.charAt(start)))) {
                        start++;
                    }
                    i = start - 1; // Will be incremented by loop
                }
            }
        }
        return new GenericArray(rows.toArray());
    }
}
