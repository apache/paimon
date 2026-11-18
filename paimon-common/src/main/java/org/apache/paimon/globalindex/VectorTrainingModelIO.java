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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned Paimon envelope around the independently versioned native training payload. */
public final class VectorTrainingModelIO {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SHARD_MODE = "centroid-based";

    private VectorTrainingModelIO() {}

    public static void write(OutputStream out, byte[] payload, String modelId) throws IOException {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("version", 1);
        meta.put("shardMode", SHARD_MODE);
        meta.put("modelId", modelId);
        byte[] header = MAPPER.writeValueAsBytes(meta);
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(header.length);
        data.write(header);
        data.write(payload);
    }

    public static byte[] read(InputStream input, long fileSize, String modelId) throws IOException {
        return read(input, fileSize, modelId, Integer.MAX_VALUE - 8);
    }

    public static byte[] read(InputStream input, long fileSize, String modelId, int maxPayloadBytes)
            throws IOException {
        DataInputStream data = new DataInputStream(input);
        int size = data.readInt();
        if (size <= 0 || size > 65536 || size > fileSize - 4) {
            throw new IOException("Invalid routing model metadata length.");
        }
        long payloadSize = fileSize - 4 - size;
        if (payloadSize <= 0 || payloadSize > maxPayloadBytes) {
            throw new IOException("Invalid routing model payload length.");
        }
        byte[] header = new byte[size];
        data.readFully(header);
        Map<String, Object> metadata =
                MAPPER.readValue(header, new TypeReference<Map<String, Object>>() {});
        if (!Integer.valueOf(1).equals(metadata.get("version"))
                || !SHARD_MODE.equals(metadata.get("shardMode"))
                || !modelId.equals(metadata.get("modelId"))) {
            throw new IOException("Unsupported or mismatched routing model envelope.");
        }
        byte[] payload = new byte[(int) payloadSize];
        data.readFully(payload);
        return payload;
    }
}
