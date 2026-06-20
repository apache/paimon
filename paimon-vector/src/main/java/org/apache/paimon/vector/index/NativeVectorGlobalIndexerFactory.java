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

package org.apache.paimon.vector.index;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

import java.util.LinkedHashMap;
import java.util.Map;

/** Factory for creating vector indexes backed by paimon-vector-index-java. */
public abstract class NativeVectorGlobalIndexerFactory implements GlobalIndexerFactory {

    private static final int DEFAULT_DIMENSION = 128;

    @Override
    public GlobalIndexer create(DataField field, Options options) {
        String identifier = identifier();
        return new NativeVectorGlobalIndexer(
                field.type(),
                nativeOptions(field.type(), options, identifier, field.name()),
                identifier);
    }

    static Map<String, String> nativeOptions(
            DataType fieldType, Options tableOptions, String identifier, String fieldName) {
        Map<String, String> nativeOptions = new LinkedHashMap<>();
        String optionPrefix = identifier + ".";
        String fieldPrefix = "fields." + fieldName + ".";
        Map<String, String> tableOptionsMap = tableOptions.toMap();

        // First collect index-type level options, e.g. <index-type>.xxx.
        for (Map.Entry<String, String> entry : tableOptionsMap.entrySet()) {
            String optionKey = entry.getKey();
            if (optionKey.startsWith(optionPrefix)) {
                String nativeKey = nativeOptionKey(optionKey.substring(optionPrefix.length()));
                if (nativeKey != null) {
                    nativeOptions.put(nativeKey, entry.getValue());
                }
            }
        }

        // Then collect field level options, e.g. fields.<field-name>.xxx, which take precedence
        // over the index-type level options for this field.
        for (Map.Entry<String, String> entry : tableOptionsMap.entrySet()) {
            String optionKey = entry.getKey();
            if (optionKey.startsWith(fieldPrefix)) {
                String nativeKey = nativeOptionKey(optionKey.substring(fieldPrefix.length()));
                if (nativeKey != null) {
                    nativeOptions.put(nativeKey, entry.getValue());
                }
            }
        }

        nativeOptions.put("index.type", identifier.replace('-', '_'));
        nativeOptions.put(
                "dimension", String.valueOf(dimension(fieldType, nativeOptions, identifier)));
        return nativeOptions;
    }

    private static String nativeOptionKey(String optionKey) {
        switch (optionKey) {
            case "index.dimension":
            case "dimension":
                return "dimension";
            case "distance.metric":
            case "metric":
                return "metric";
            case "nlist":
            case "pq.m":
            case "hnsw.m":
            case "hnsw.ef-construction":
            case "hnsw.max-level":
                return optionKey;
            case "pq.use-opq":
            case "use-opq":
                return "use-opq";
            default:
                return null;
        }
    }

    private static int dimension(
            DataType fieldType, Map<String, String> nativeOptions, String identifier) {
        if (fieldType instanceof VectorType) {
            return ((VectorType) fieldType).getLength();
        }
        String dimension = nativeOptions.get("dimension");
        int value = dimension == null ? DEFAULT_DIMENSION : Integer.parseInt(dimension);
        if (value <= 0) {
            throw new IllegalArgumentException(
                    "Invalid value for '"
                            + identifier
                            + ".dimension': "
                            + value
                            + ". Must be a positive integer.");
        }
        return value;
    }
}
