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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for a vector index file.
 *
 * <p>Serialized as a flat JSON {@code Map<String, String>} storing Paimon search parameters that
 * are not part of the native vector index file metadata.
 */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String KEY_NPROBE = "nprobe";
    static final String KEY_EF_SEARCH = "ef_search";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private final Map<String, String> params;

    VectorIndexMeta(Map<String, String> params) {
        this.params = new LinkedHashMap<>(params);
    }

    public int nprobe() {
        return intValue(KEY_NPROBE, 16);
    }

    public int efSearch() {
        return intValue(KEY_EF_SEARCH, 0);
    }

    public byte[] serialize() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(params);
    }

    public static VectorIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> map = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        return new VectorIndexMeta(map);
    }

    private int intValue(String key, int defaultValue) {
        String val = params.get(key);
        return val == null ? defaultValue : Integer.parseInt(val);
    }
}
