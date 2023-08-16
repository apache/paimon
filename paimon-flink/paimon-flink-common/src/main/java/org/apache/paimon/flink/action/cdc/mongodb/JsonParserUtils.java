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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.type.MapType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * specified, and return json string of the extracted json object. It will return null if the input
 * json string is invalid. A limited version of JSONPath supported: $ : Root object . : Child
 * operator [] : Subscript operator for array * : Wildcard for [] Syntax not supported that's worth
 * noticing: '' : Zero length string as key .. : Recursive descent &amp;#064; : Current
 * object/element () : Script expression ?() : Filter (script) expression. [,] : Union operator
 * [start:end:step] : array slice operator.
 */
@SuppressWarnings("unchecked")
public class JsonParserUtils implements Serializable {

    private static final Pattern patternKey = Pattern.compile("^([a-zA-Z0-9_\\-:\\s]+).*");
    private static final Pattern patternIndex = Pattern.compile("\\[([0-9]+|\\*)]");
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    static {
        // Allows for unescaped ASCII control characters in JSON values
        JSON_FACTORY.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper(JSON_FACTORY);
    private static final MapType MAP_TYPE =
            MAPPER.getTypeFactory().constructMapType(Map.class, String.class, Object.class);

    // An LRU cache using a linked hash map
    static class HashCache<K, V> extends LinkedHashMap<K, V> {

        private static final int CACHE_SIZE = 16;
        private static final int INIT_SIZE = 32;
        private static final float LOAD_FACTOR = 0.6f;

        HashCache() {
            super(INIT_SIZE, LOAD_FACTOR);
        }

        private static final long serialVersionUID = 1;

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > CACHE_SIZE;
        }
    }

    static Map<String, Object> extractObjectCache = new HashCache<>();
    static Map<String, String[]> pathExprCache = new HashCache<>();
    static Map<String, ArrayList<String>> indexListCache = new HashCache<>();
    static Map<String, String> mKeyGroupCache = new HashCache<>();
    static Map<String, Boolean> mKeyMatchesCache = new HashCache<>();

    public static String evaluate(String jsonString, String pathString) {

        if (jsonString == null
                || jsonString.equals("")
                || pathString == null
                || pathString.equals("")) {
            return null;
        }

        // Cache pathExpr
        String[] pathExpr = pathExprCache.computeIfAbsent(pathString, s -> s.split("\\.", -1));

        if (!pathExpr[0].equalsIgnoreCase("$")) {
            return null;
        }
        // Cache extractObject
        Object extractObject = extractObjectCache.get(jsonString);
        if (extractObject == null) {
            try {
                extractObject = MAPPER.readValue(jsonString, MAP_TYPE);
            } catch (Exception e) {
                return null;
            }
            extractObjectCache.put(jsonString, extractObject);
        }
        for (int i = 1; i < pathExpr.length; i++) {
            if (extractObject == null) {
                return null;
            }
            extractObject = extract(extractObject, pathExpr[i]);
        }
        if (extractObject instanceof Map || extractObject instanceof List) {
            try {
                return MAPPER.writeValueAsString(extractObject);
            } catch (Exception e) {
                return null;
            }
        } else if (extractObject != null) {
            return extractObject.toString();
        } else {
            return null;
        }
    }

    private static Object extract(Object json, String path) {

        // Cache patternkey.matcher(path).matches()
        Matcher mKey = null;
        Boolean mKeyMatches = mKeyMatchesCache.get(path);
        if (mKeyMatches == null) {
            mKey = patternKey.matcher(path);
            mKeyMatches = mKey.matches() ? Boolean.TRUE : Boolean.FALSE;
            mKeyMatchesCache.put(path, mKeyMatches);
        }
        if (!mKeyMatches) {
            return null;
        }

        // Cache mkey.group(1)
        String mKeyGroup1 = mKeyGroupCache.get(path);
        if (mKeyGroup1 == null) {
            if (mKey == null) {
                mKey = patternKey.matcher(path);
            }
            mKeyGroup1 = mKey.group(1);
            mKeyGroupCache.put(path, mKeyGroup1);
        }
        json = extract_json_key(json, mKeyGroup1);

        // Cache indexList
        ArrayList<String> indexList = indexListCache.get(path);
        if (indexList == null) {
            Matcher mIndex = patternIndex.matcher(path);
            indexList = new ArrayList<>();
            while (mIndex.find()) {
                indexList.add(mIndex.group(1));
            }
            indexListCache.put(path, indexList);
        }

        if (indexList.size() > 0) {
            json = extract_json_withIndex(json, indexList);
        }

        return json;
    }

    private static Object extract_json_withIndex(Object json, ArrayList<String> indexList) {
        List<Object> jsonList = new ArrayList<>();
        jsonList.add(json);
        for (String index : indexList) {
            List<Object> tmpJsonList = new ArrayList<>();
            if (index.equalsIgnoreCase("*")) {
                for (Object array : jsonList) {
                    if (array instanceof List) {
                        tmpJsonList.addAll((List<Object>) array);
                    }
                }
                jsonList = tmpJsonList;
            } else {
                for (int i = 0; i < (jsonList).size(); i++) {
                    Object array = jsonList.get(i);
                    int indexValue = Integer.parseInt(index);
                    if (!(array instanceof List)) {
                        continue;
                    }
                    if (indexValue >= ((List<Object>) array).size()) {
                        return null;
                    }
                    tmpJsonList.add(((List<Object>) array).get(indexValue));
                    jsonList = tmpJsonList;
                }
            }
        }
        if (jsonList.isEmpty()) {
            return null;
        }
        return (jsonList.size() > 1) ? new ArrayList<>(jsonList) : jsonList.get(0);
    }

    private static Object extract_json_key(Object json, String path) {
        if (json instanceof List) {
            List<Object> jsonArray = new ArrayList<>();
            for (int i = 0; i < ((List<Object>) json).size(); i++) {
                Object jsonElem = ((List<Object>) json).get(i);
                Object jsonObj;
                if (jsonElem instanceof Map) {
                    jsonObj = ((Map<String, Object>) jsonElem).get(path);
                } else {
                    continue;
                }
                if (jsonObj instanceof List) {
                    jsonArray.addAll((List<Object>) jsonObj);
                } else if (jsonObj != null) {
                    jsonArray.add(jsonObj);
                }
            }
            return (jsonArray.size() == 0) ? null : jsonArray;
        } else if (json instanceof Map) {
            return ((Map<String, Object>) json).get(path);
        } else {
            return null;
        }
    }

    public static LinkedHashMap<String, String> extractMap(String jsonString) {
        try {
            LinkedHashMap<String, ?> originalMap = MAPPER.readValue(jsonString, MAP_TYPE);
            LinkedHashMap<String, String> stringMap = new LinkedHashMap<>();

            originalMap.forEach(
                    (key, value) -> {
                        String stringValue = Objects.toString(value, null);
                        stringMap.put(key.toLowerCase(), stringValue);
                    });

            return stringMap;

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
