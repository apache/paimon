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

package org.apache.paimon.lumina.index;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for a Lumina vector index file.
 *
 * <p>Serialized as a flat JSON {@code Map<String, String>} whose keys are lumina native option keys
 * (with the {@code lumina.} prefix stripped). This matches paimon-cpp's metadata format exactly, so
 * that indexes built by either paimon-cpp or paimon-lumina can be read by both implementations.
 *
 * <p>Standard keys include:
 *
 * <ul>
 *   <li>{@code index.dimension} &ndash; vector dimension
 *   <li>{@code index.type} &ndash; index algorithm (e.g. "diskann")
 *   <li>{@code distance.metric} &ndash; distance metric (e.g. "l2", "cosine", "inner_product")
 *   <li>{@code encoding.type} &ndash; vector encoding (e.g. "rawf32", "pq", "sq8")
 * </ul>
 */
public class LuminaIndexMeta implements Serializable {

    private static final long serialVersionUID = 3L;

    private static final String KEY_DIMENSION =
            LuminaVectorIndexOptions.toLuminaKey(LuminaVectorIndexOptions.DIMENSION);
    private static final String KEY_DISTANCE_METRIC =
            LuminaVectorIndexOptions.toLuminaKey(LuminaVectorIndexOptions.DISTANCE_METRIC);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private final Map<String, String> options;

    public LuminaIndexMeta(Map<String, String> options) {
        this.options = new LinkedHashMap<>(options);
    }

    /** Returns the full options map. */
    public Map<String, String> options() {
        return options;
    }

    public int dim() {
        return Integer.parseInt(options.get(KEY_DIMENSION));
    }

    public String distanceMetric() {
        return options.get(KEY_DISTANCE_METRIC);
    }

    public LuminaVectorMetric metric() {
        return LuminaVectorMetric.fromLuminaName(distanceMetric());
    }

    /** Serializes this metadata as a UTF-8 encoded JSON string (flat key-value map). */
    public byte[] serialize() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(options);
    }

    /** Deserializes metadata from a UTF-8 encoded JSON byte array. */
    public static LuminaIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> map = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        if (!map.containsKey(KEY_DIMENSION)) {
            throw new IOException(
                    "Missing required key in Lumina index metadata: " + KEY_DIMENSION);
        }
        if (!map.containsKey(KEY_DISTANCE_METRIC)) {
            throw new IOException(
                    "Missing required key in Lumina index metadata: " + KEY_DISTANCE_METRIC);
        }
        return new LuminaIndexMeta(map);
    }
}
