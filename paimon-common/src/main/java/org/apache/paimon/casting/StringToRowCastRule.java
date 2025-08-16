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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeRoot#ROW} cast rule. */
class StringToRowCastRule extends AbstractCastRule<BinaryString, InternalRow> {

    static final StringToRowCastRule INSTANCE = new StringToRowCastRule();

    // Pattern for bracket format: {field1, field2, field3}
    private static final Pattern BRACKET_ROW_PATTERN = Pattern.compile("^\\s*\\{(.*)\\}\\s*$");

    // Pattern for SQL function format: STRUCT(field1, field2, field3)
    private static final Pattern FUNCTION_ROW_PATTERN =
            Pattern.compile("^\\s*STRUCT\\s*\\((.*)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private StringToRowCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.ROW)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalRow> create(DataType inputType, DataType targetType) {
        RowType rowType = (RowType) targetType;
        CastExecutor<BinaryString, Object>[] fieldCastExecutors = createFieldCastExecutors(rowType);
        return value -> parseRow(value, fieldCastExecutors, rowType.getFieldCount());
    }

    private CastExecutor<BinaryString, Object>[] createFieldCastExecutors(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object>[] fieldCastExecutors = new CastExecutor[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataType fieldType = rowType.getTypeAt(i);
            @SuppressWarnings("unchecked")
            CastExecutor<BinaryString, Object> executor =
                    (CastExecutor<BinaryString, Object>)
                            CastExecutors.resolve(VarCharType.STRING_TYPE, fieldType);
            if (executor == null) {
                throw new RuntimeException("Cannot cast string to row field type: " + fieldType);
            }
            fieldCastExecutors[i] = executor;
        }
        return fieldCastExecutors;
    }

    private InternalRow parseRow(
            BinaryString value,
            CastExecutor<BinaryString, Object>[] fieldCastExecutors,
            int fieldCount) {
        try {
            String str = value.toString().trim();
            if ("{}".equals(str) || "STRUCT()".equalsIgnoreCase(str)) {
                return createNullRow(fieldCount);
            }
            String content = extractRowContent(str);
            if (content.isEmpty()) {
                return createNullRow(fieldCount);
            }
            List<String> fieldValues = splitRowFields(content);
            if (fieldValues.size() != fieldCount) {
                throw new RuntimeException(
                        "Row field count mismatch. Expected: "
                                + fieldCount
                                + ", Actual: "
                                + fieldValues.size());
            }

            return createRowFromFields(fieldValues, fieldCastExecutors, fieldCount);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse '" + value + "' as ROW: " + e.getMessage(), e);
        }
    }

    /**
     * Extract content from row string, supporting both bracket format {f1, f2} and function format
     * STRUCT(f1, f2).
     */
    private String extractRowContent(String str) {
        // Try bracket format first: {field1, field2, field3}
        Matcher bracketMatcher = BRACKET_ROW_PATTERN.matcher(str);
        if (bracketMatcher.matches()) {
            return bracketMatcher.group(1).trim();
        }
        // Try SQL function format: STRUCT(field1, field2, field3)
        Matcher functionMatcher = FUNCTION_ROW_PATTERN.matcher(str);
        if (functionMatcher.matches()) {
            return functionMatcher.group(1).trim();
        }

        throw new RuntimeException(
                "Invalid row format: " + str + ". Expected format: {f1, f2} or STRUCT(f1, f2)");
    }

    private GenericRow createNullRow(int fieldCount) {
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            row.setField(i, null);
        }
        return row;
    }

    private GenericRow createRowFromFields(
            List<String> fieldValues,
            CastExecutor<BinaryString, Object>[] fieldCastExecutors,
            int fieldCount) {
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            String fieldValue = fieldValues.get(i).trim();
            Object value = parseFieldValue(fieldValue, fieldCastExecutors[i]);
            row.setField(i, value);
        }
        return row;
    }

    private Object parseFieldValue(
            String fieldValue, CastExecutor<BinaryString, Object> castExecutor) {
        return "null".equals(fieldValue)
                ? null
                : castExecutor.cast(BinaryString.fromString(fieldValue));
    }

    private List<String> splitRowFields(String content) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Stack<Character> bracketStack = new Stack<>();
        boolean inQuotes = false;
        boolean escaped = false;

        for (char c : content.toCharArray()) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (StringUtils.isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (StringUtils.isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addCurrentField(fields, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentField(fields, current);
        return fields;
    }

    private void addCurrentField(List<String> fields, StringBuilder current) {
        if (current.length() > 0) {
            fields.add(current.toString());
            current.setLength(0);
        }
    }
}
