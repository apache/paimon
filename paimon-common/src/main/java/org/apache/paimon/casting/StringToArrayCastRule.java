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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeRoot#ARRAY} cast rule. */
class StringToArrayCastRule extends AbstractCastRule<BinaryString, InternalArray> {

    static final StringToArrayCastRule INSTANCE = new StringToArrayCastRule();

    private static final Pattern ARRAY_PATTERN = Pattern.compile("^\\s*\\[(.*)\\]\\s*$");

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
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object> elementCastExecutor =
                (CastExecutor<BinaryString, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, arrayType.getElementType());
        if (elementCastExecutor == null) {
            throw new RuntimeException(
                    "Cannot cast string to array element type: " + arrayType.getElementType());
        }
        return value -> parseArray(value, elementCastExecutor);
    }

    private InternalArray parseArray(
            BinaryString value, CastExecutor<BinaryString, Object> elementCastExecutor) {
        try {
            String str = value.toString().trim();
            if ("[]".equals(str)) {
                return new GenericArray(new Object[0]);
            }

            Matcher matcher = ARRAY_PATTERN.matcher(str);
            if (!matcher.matches()) {
                throw new RuntimeException("Invalid array format: " + str);
            }

            String content = matcher.group(1).trim();
            if (content.isEmpty()) {
                return new GenericArray(new Object[0]);
            }

            List<Object> elements = parseArrayElements(content, elementCastExecutor);
            return new GenericArray(elements.toArray());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot parse '" + value + "' as ARRAY: " + e.getMessage(), e);
        }
    }

    private List<Object> parseArrayElements(
            String content, CastExecutor<BinaryString, Object> elementCastExecutor) {
        List<Object> elements = new ArrayList<>();
        for (String token : splitArrayElements(content)) {
            String trimmedToken = token.trim();
            Object element =
                    "null".equals(trimmedToken)
                            ? null
                            : elementCastExecutor.cast(BinaryString.fromString(trimmedToken));
            elements.add(element);
        }
        return elements;
    }

    private List<String> splitArrayElements(String content) {
        List<String> elements = new ArrayList<>();
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
                if (isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addCurrentElement(elements, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentElement(elements, current);
        return elements;
    }

    private boolean isOpenBracket(char c) {
        return c == '[' || c == '{' || c == '(';
    }

    private boolean isCloseBracket(char c) {
        return c == ']' || c == '}' || c == ')';
    }

    private void addCurrentElement(List<String> elements, StringBuilder current) {
        if (current.length() > 0) {
            elements.add(current.toString());
            current.setLength(0);
        }
    }
}
