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

package org.apache.paimon.ivfpq.index;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for an IVF-PQ vector index file.
 *
 * <p>Serialized as a flat JSON {@code Map<String, String>} storing the index build parameters
 * required for correct search-time behavior.
 */
public class IvfpqIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String KEY_DIMENSION = "dimension";
    private static final String KEY_METRIC = "metric";
    private static final String KEY_NLIST = "nlist";
    private static final String KEY_M = "m";
    private static final String KEY_USE_OPQ = "use_opq";
    private static final String KEY_NPROBE = "nprobe";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private final Map<String, String> params;

    public IvfpqIndexMeta(IvfpqVectorIndexOptions options) {
        this.params = new LinkedHashMap<>();
        params.put(KEY_DIMENSION, String.valueOf(options.dimension()));
        params.put(KEY_METRIC, options.metric().getConfigName());
        params.put(KEY_NLIST, String.valueOf(options.nlist()));
        params.put(KEY_M, String.valueOf(options.m()));
        params.put(KEY_USE_OPQ, String.valueOf(options.useOpq()));
        params.put(KEY_NPROBE, String.valueOf(options.nprobe()));
    }

    private IvfpqIndexMeta(Map<String, String> params) {
        this.params = new LinkedHashMap<>(params);
    }

    public int dimension() {
        return Integer.parseInt(params.get(KEY_DIMENSION));
    }

    public IvfpqVectorMetric metric() {
        return IvfpqVectorMetric.fromConfigName(params.get(KEY_METRIC));
    }

    public int nlist() {
        return Integer.parseInt(params.get(KEY_NLIST));
    }

    public int m() {
        return Integer.parseInt(params.get(KEY_M));
    }

    public boolean useOpq() {
        return Boolean.parseBoolean(params.get(KEY_USE_OPQ));
    }

    public int nprobe() {
        String val = params.get(KEY_NPROBE);
        return val != null ? Integer.parseInt(val) : 16;
    }

    public byte[] serialize() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(params);
    }

    public static IvfpqIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> map = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        if (!map.containsKey(KEY_DIMENSION)) {
            throw new IOException(
                    "Missing required key in IVF-PQ index metadata: " + KEY_DIMENSION);
        }
        if (!map.containsKey(KEY_METRIC)) {
            throw new IOException("Missing required key in IVF-PQ index metadata: " + KEY_METRIC);
        }
        return new IvfpqIndexMeta(map);
    }
}
