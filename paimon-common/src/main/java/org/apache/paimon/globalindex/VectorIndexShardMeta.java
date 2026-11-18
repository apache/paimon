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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.Range;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manifest metadata shared by vector implementations and engine-independent planning. */
public final class VectorIndexShardMeta {
    public static final String CENTROID_BASED = "centroid-based";
    public static final String ABSOLUTE_ROW_ID = "ABSOLUTE_ROW_ID";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, String>> TYPE =
            new TypeReference<LinkedHashMap<String, String>>() {};
    private final Map<String, String> values;

    private VectorIndexShardMeta(Map<String, String> values) {
        this.values = values;
    }

    public static VectorIndexShardMeta deserialize(@Nullable byte[] bytes) {
        try {
            return new VectorIndexShardMeta(
                    bytes == null ? Collections.emptyMap() : MAPPER.readValue(bytes, TYPE));
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid vector shard metadata", e);
        }
    }

    public static VectorIndexShardMeta routing(String modelId, int nlist, List<Range> coverage) {
        checkArgument(
                modelId != null && !modelId.isEmpty() && nlist > 0 && !coverage.isEmpty(),
                "Invalid routing model metadata.");
        Map<String, String> values = base(modelId);
        values.put("nlist", Integer.toString(nlist));
        values.put(
                "sourceRowRanges",
                Range.sortAndMergeOverlap(coverage, true).stream()
                        .map(r -> r.from + ":" + r.to)
                        .collect(Collectors.joining(",")));
        return new VectorIndexShardMeta(values);
    }

    public static VectorIndexShardMeta centroid(String modelId, int centroid) {
        checkArgument(centroid >= 0, "Centroid must be nonnegative.");
        Map<String, String> values = base(modelId);
        values.put("centroid", Integer.toString(centroid));
        values.put("rowIdEncoding", ABSOLUTE_ROW_ID);
        return new VectorIndexShardMeta(values);
    }

    private static Map<String, String> base(String modelId) {
        checkArgument(modelId != null && !modelId.isEmpty(), "Model id is required.");
        Map<String, String> values = new LinkedHashMap<>();
        values.put("shardMode", CENTROID_BASED);
        values.put("modelId", modelId);
        return values;
    }

    public boolean centroidBased() {
        return CENTROID_BASED.equals(values.get("shardMode"));
    }

    public boolean routingModel() {
        return centroidBased() && values.containsKey("nlist");
    }

    public String modelId() {
        String id = values.get("modelId");
        checkArgument(id != null && !id.isEmpty(), "Missing centroid model id.");
        return id;
    }

    public int centroid() {
        checkArgument(
                ABSOLUTE_ROW_ID.equals(values.get("rowIdEncoding")),
                "Unsupported centroid row ID encoding.");
        int centroid = Integer.parseInt(values.get("centroid"));
        checkArgument(centroid >= 0, "Invalid centroid id.");
        return centroid;
    }

    public int nlist() {
        int nlist = Integer.parseInt(values.get("nlist"));
        checkArgument(nlist > 0, "Invalid nlist.");
        return nlist;
    }

    public List<Range> sourceRowRanges() {
        List<Range> ranges = new ArrayList<>();
        String encoded = values.get("sourceRowRanges");
        checkArgument(encoded != null && !encoded.isEmpty(), "Missing source row coverage.");
        for (String range : encoded.split(",")) {
            String[] endpoints = range.split(":");
            checkArgument(endpoints.length == 2, "Invalid source row range.");
            long from = Long.parseLong(endpoints[0]);
            long to = Long.parseLong(endpoints[1]);
            checkArgument(from >= 0 && to >= from, "Invalid source row range.");
            ranges.add(new Range(from, to));
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(values);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize vector shard metadata", e);
        }
    }
}
