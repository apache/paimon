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

import org.apache.paimon.index.ivfpq.IndexType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for a vector index file.
 *
 * <p>Serialized as a flat JSON {@code Map<String, String>} storing the index build parameters
 * required for correct search-time behavior.
 */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String KEY_INDEX_TYPE = "index_type";
    static final String KEY_DIMENSION = "dimension";
    static final String KEY_METRIC = "metric";
    static final String KEY_NLIST = "nlist";
    static final String KEY_M = "m";
    static final String KEY_USE_OPQ = "use_opq";
    static final String KEY_HNSW_M = "hnsw_m";
    static final String KEY_HNSW_EF_CONSTRUCTION = "hnsw_ef_construction";
    static final String KEY_HNSW_MAX_LEVEL = "hnsw_max_level";
    static final String KEY_NPROBE = "nprobe";
    static final String KEY_EF_SEARCH = "ef_search";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private final Map<String, String> params;

    VectorIndexMeta(Map<String, String> params) {
        this.params = new LinkedHashMap<>(params);
    }

    public IndexType indexType() {
        String value = params.get(KEY_INDEX_TYPE);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required key in vector index metadata: " + KEY_INDEX_TYPE);
        }
        return parseIndexType(value);
    }

    public int dimension() {
        return Integer.parseInt(params.get(KEY_DIMENSION));
    }

    public String metric() {
        return params.get(KEY_METRIC);
    }

    public int nlist() {
        return Integer.parseInt(params.get(KEY_NLIST));
    }

    public int m() {
        return intValue(KEY_M, 0);
    }

    public boolean useOpq() {
        return Boolean.parseBoolean(params.get(KEY_USE_OPQ));
    }

    public int hnswM() {
        return intValue(KEY_HNSW_M, 20);
    }

    public int hnswEfConstruction() {
        return intValue(KEY_HNSW_EF_CONSTRUCTION, 150);
    }

    public int hnswMaxLevel() {
        return intValue(KEY_HNSW_MAX_LEVEL, 7);
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
        if (!map.containsKey(KEY_DIMENSION)) {
            throw new IOException(
                    "Missing required key in vector index metadata: " + KEY_DIMENSION);
        }
        if (!map.containsKey(KEY_INDEX_TYPE)) {
            throw new IOException(
                    "Missing required key in vector index metadata: " + KEY_INDEX_TYPE);
        }
        if (!map.containsKey(KEY_METRIC)) {
            throw new IOException("Missing required key in vector index metadata: " + KEY_METRIC);
        }
        return new VectorIndexMeta(map);
    }

    private int intValue(String key, int defaultValue) {
        String val = params.get(key);
        return val == null ? defaultValue : Integer.parseInt(val);
    }

    private static IndexType parseIndexType(String value) {
        if (IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER.equals(value)) {
            return IndexType.IVF_PQ;
        } else if (IvfFlatVectorGlobalIndexerFactory.IDENTIFIER.equals(value)) {
            return IndexType.IVF_FLAT;
        } else if (IvfHnswFlatVectorGlobalIndexerFactory.IDENTIFIER.equals(value)) {
            return IndexType.IVF_HNSW_FLAT;
        } else if (IvfHnswSqVectorGlobalIndexerFactory.IDENTIFIER.equals(value)) {
            return IndexType.IVF_HNSW_SQ;
        }
        throw new IllegalArgumentException("Unknown vector index type: " + value);
    }
}
