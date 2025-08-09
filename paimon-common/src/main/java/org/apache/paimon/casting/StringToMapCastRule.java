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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeRoot#MAP} cast rule. */
class StringToMapCastRule extends AbstractCastRule<BinaryString, InternalMap> {

    static final StringToMapCastRule INSTANCE = new StringToMapCastRule();

    private static final Pattern MAP_PATTERN = Pattern.compile("^\\s*\\{(.*)\\}\\s*$");
    private static final Pattern ENTRY_PATTERN = Pattern.compile("(.+?)\\s*->\\s*(.+)");

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
        CastExecutor<BinaryString, Object> keyCastExecutor =
                createCastExecutor(mapType.getKeyType());
        CastExecutor<BinaryString, Object> valueCastExecutor =
                createCastExecutor(mapType.getValueType());

        return value -> parseMap(value, keyCastExecutor, valueCastExecutor);
    }

    private CastExecutor<BinaryString, Object> createCastExecutor(DataType targetType) {
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object> executor =
                (CastExecutor<BinaryString, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, targetType);
        if (executor == null) {
            throw new RuntimeException("Cannot cast string to type: " + targetType);
        }
        return executor;
    }

    private InternalMap parseMap(
            BinaryString value,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {
        try {
            String str = value.toString().trim();
            if ("{}".equals(str)) {
                return new GenericMap(new HashMap<>());
            }

            Matcher matcher = MAP_PATTERN.matcher(str);
            if (!matcher.matches()) {
                throw new RuntimeException("Invalid map format: " + str);
            }

            String content = matcher.group(1).trim();
            if (content.isEmpty()) {
                return new GenericMap(new HashMap<>());
            }

            Map<Object, Object> javaMap = new HashMap<>();
            for (String entry : splitMapEntries(content)) {
                parseMapEntry(entry.trim(), javaMap, keyCastExecutor, valueCastExecutor);
            }

            return new GenericMap(javaMap);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse '" + value + "' as MAP: " + e.getMessage(), e);
        }
    }

    private void parseMapEntry(
            String entry,
            Map<Object, Object> javaMap,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {
        Matcher entryMatcher = ENTRY_PATTERN.matcher(entry);
        if (!entryMatcher.matches()) {
            throw new RuntimeException("Invalid map entry format: " + entry);
        }
        javaMap.put(
                parseValue(entryMatcher.group(1).trim(), keyCastExecutor),
                parseValue(entryMatcher.group(2).trim(), valueCastExecutor));
    }

    private Object parseValue(String valueStr, CastExecutor<BinaryString, Object> castExecutor) {
        return "null".equals(valueStr)
                ? null
                : castExecutor.cast(BinaryString.fromString(valueStr));
    }

    private List<String> splitMapEntries(String content) {
        List<String> entries = new ArrayList<>();
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
                    addCurrentEntry(entries, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentEntry(entries, current);
        return entries;
    }

    private boolean isOpenBracket(char c) {
        return c == '[' || c == '{' || c == '(';
    }

    private boolean isCloseBracket(char c) {
        return c == ']' || c == '}' || c == ')';
    }

    private void addCurrentEntry(List<String> entries, StringBuilder current) {
        if (current.length() > 0) {
            entries.add(current.toString());
            current.setLength(0);
        }
    }
}
