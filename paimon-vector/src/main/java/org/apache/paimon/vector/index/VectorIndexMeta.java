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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for a vector index file.
 *
 * <p>Serialized as a JSON {@code Map<String, String>}; it records the metric the index was built
 * with so searches can reject segments built under a different metric. Legacy segments carry an
 * empty map. Search-time parameters are passed through {@link
 * org.apache.paimon.predicate.VectorSearch#options()}.
 */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private static final String METRIC_KEY = "metric";

    @Nullable private final String metric;

    VectorIndexMeta(@Nullable String metric) {
        this.metric = metric;
    }

    public byte[] serialize() throws IOException {
        Map<String, String> data = new LinkedHashMap<>();
        if (metric != null) {
            data.put(METRIC_KEY, metric);
        }
        return OBJECT_MAPPER.writeValueAsBytes(data);
    }

    public static VectorIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> map = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        return new VectorIndexMeta(map.get(METRIC_KEY));
    }

    /** The metric this index was built with, or null for legacy segments that record none. */
    @Nullable
    public String metric() {
        return metric;
    }
}
