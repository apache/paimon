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
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Private wire format for the indexMeta of native centroid models and shards. */
public final class VectorIndexShardMeta {
    private static final byte[] MAGIC = {'P', 'C', 'S', 'M'};
    private static final byte VERSION = 3;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, String>> TYPE =
            new TypeReference<LinkedHashMap<String, String>>() {};
    private final Map<String, String> values;
    @Nullable private final byte[] payloadMeta;

    private VectorIndexShardMeta(Map<String, String> values, @Nullable byte[] payloadMeta) {
        this.values = values;
        this.payloadMeta = payloadMeta;
    }

    public static VectorIndexShardMeta deserialize(byte[] bytes) {
        try {
            Map<String, String> descriptor;
            byte[] payload = null;
            checkArgument(
                    bytes != null && bytes.length >= MAGIC.length + 1 + 2 * Integer.BYTES,
                    "Invalid centroid metadata header.");
            ByteBuffer input = ByteBuffer.wrap(bytes);
            for (byte expected : MAGIC) {
                checkArgument(input.get() == expected, "Invalid centroid metadata header.");
            }
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

    public static VectorIndexShardMeta routing(String modelId, int nlist, @Nullable String metric) {
        checkArgument(
                modelId != null && !modelId.isEmpty() && nlist >= 0,
                "Invalid routing model metadata.");
        Map<String, String> values = base(modelId);
        values.put("nlist", Integer.toString(nlist));
        if (metric != null) {
            values.put("metric", metric);
        }
        return new VectorIndexShardMeta(values, null);
    }

    public static VectorIndexShardMeta centroid(String modelId, int centroid) {
        checkArgument(centroid >= 0, "Centroid must be nonnegative.");
        Map<String, String> values = base(modelId);
        values.put("centroid", Integer.toString(centroid));
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
        int centroid = Integer.parseInt(values.get("centroid"));
        checkArgument(centroid >= 0, "Invalid centroid id.");
        return centroid;
    }

    @Nullable
    String metric() {
        return values.get("metric");
    }

    public int nlist() {
        int nlist = Integer.parseInt(values.get("nlist"));
        checkArgument(nlist >= 0, "Invalid nlist.");
        return nlist;
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
