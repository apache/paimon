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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manifest metadata shared by vector implementations and engine-independent planning. */
public final class VectorIndexShardMeta {
    private static final byte[] MAGIC = {'P', 'C', 'S', 'M'};
    private static final byte VERSION = 1;
    // The preview writer emitted this key first in a LinkedHashMap.
    private static final byte[] PREVIEW_PREFIX =
            "{\"shardMode\":\"centroid-based\",".getBytes(StandardCharsets.UTF_8);
    private static final String ABSOLUTE_ROW_ID = "ABSOLUTE_ROW_ID";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, String>> TYPE =
            new TypeReference<LinkedHashMap<String, String>>() {};
    private final Map<String, String> values;
    @Nullable private final byte[] payloadMeta;

    private VectorIndexShardMeta(Map<String, String> values, @Nullable byte[] payloadMeta) {
        this.values = values;
        this.payloadMeta = payloadMeta;
    }

    /** Ordinary index metadata is opaque and can use any encoding. */
    public static boolean isWrapped(@Nullable byte[] bytes) {
        return startsWith(bytes, MAGIC) || startsWith(bytes, PREVIEW_PREFIX);
    }

    private static boolean startsWith(@Nullable byte[] bytes, byte[] prefix) {
        if (bytes == null || bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public static VectorIndexShardMeta deserialize(byte[] bytes) {
        try {
            Map<String, String> descriptor;
            byte[] payload = null;
            if (startsWith(bytes, PREVIEW_PREFIX)) {
                descriptor = MAPPER.readValue(bytes, TYPE);
            } else {
                checkArgument(
                        startsWith(bytes, MAGIC)
                                && bytes.length >= MAGIC.length + 1 + 2 * Integer.BYTES,
                        "Invalid centroid metadata header.");
                ByteBuffer input = ByteBuffer.wrap(bytes);
                input.position(MAGIC.length);
                checkArgument(input.get() == VERSION, "Unsupported centroid metadata version.");
                int descriptorSize = input.getInt();
                int payloadSize = input.getInt();
                checkArgument(
                        descriptorSize > 0
                                && descriptorSize <= input.remaining()
                                && payloadSize >= -1
                                && input.remaining() - descriptorSize == Math.max(0, payloadSize),
                        "Invalid centroid metadata lengths.");
                descriptor = MAPPER.readValue(bytes, input.position(), descriptorSize, TYPE);
                if (payloadSize >= 0) {
                    input.position(input.position() + descriptorSize);
                    payload = new byte[payloadSize];
                    input.get(payload);
                }
            }
            checkArgument(
                    descriptor != null
                            && descriptor.containsKey("nlist")
                                    != descriptor.containsKey("centroid"),
                    "Expected a routing model or a centroid shard.");
            return new VectorIndexShardMeta(descriptor, payload);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid centroid metadata", e);
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
        return new VectorIndexShardMeta(values, null);
    }

    public static VectorIndexShardMeta centroid(String modelId, int centroid) {
        checkArgument(centroid >= 0, "Centroid must be nonnegative.");
        Map<String, String> values = base(modelId);
        values.put("centroid", Integer.toString(centroid));
        values.put("rowIdEncoding", ABSOLUTE_ROW_ID);
        return new VectorIndexShardMeta(values, null);
    }

    private static Map<String, String> base(String modelId) {
        checkArgument(modelId != null && !modelId.isEmpty(), "Model id is required.");
        Map<String, String> values = new LinkedHashMap<>();
        values.put("modelId", modelId);
        return values;
    }

    public boolean routingModel() {
        return values.containsKey("nlist");
    }

    @Nullable
    public byte[] payloadMeta() {
        return payloadMeta;
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
        return wrap(payloadMeta);
    }

    /** Store routing information and the underlying writer's opaque metadata in indexMeta. */
    public byte[] wrap(@Nullable byte[] payload) {
        try {
            byte[] descriptor = MAPPER.writeValueAsBytes(values);
            int size = Math.addExact(MAGIC.length + 1 + 2 * Integer.BYTES, descriptor.length);
            ByteBuffer output =
                    ByteBuffer.allocate(Math.addExact(size, payload == null ? 0 : payload.length));
            output.put(MAGIC)
                    .put(VERSION)
                    .putInt(descriptor.length)
                    .putInt(payload == null ? -1 : payload.length)
                    .put(descriptor);
            if (payload != null) {
                output.put(payload);
            }
            return output.array();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize centroid metadata", e);
        }
    }
}
