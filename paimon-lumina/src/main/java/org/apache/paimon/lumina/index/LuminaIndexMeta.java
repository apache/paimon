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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Metadata for a Lumina vector index file. */
public class LuminaIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private final int dim;
    private final int metricValue;
    private final String indexTypeName;
    private final long numVectors;
    private final long minId;
    private final long maxId;

    public LuminaIndexMeta(
            int dim,
            int metricValue,
            String indexTypeName,
            long numVectors,
            long minId,
            long maxId) {
        this.dim = dim;
        this.metricValue = metricValue;
        this.indexTypeName = indexTypeName;
        this.numVectors = numVectors;
        this.minId = minId;
        this.maxId = maxId;
    }

    public int dim() {
        return dim;
    }

    public int metricValue() {
        return metricValue;
    }

    public LuminaIndexType indexType() {
        try {
            return LuminaIndexType.fromString(indexTypeName);
        } catch (IllegalArgumentException e) {
            return LuminaIndexType.UNKNOWN;
        }
    }

    public long minId() {
        return minId;
    }

    public long maxId() {
        return maxId;
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(VERSION);
        out.writeInt(dim);
        out.writeInt(metricValue);
        out.writeUTF(indexTypeName);
        out.writeLong(numVectors);
        out.writeLong(minId);
        out.writeLong(maxId);
        out.flush();
        return baos.toByteArray();
    }

    public static LuminaIndexMeta deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported Lumina index meta version: " + version);
        }
        int dim = in.readInt();
        int metricValue = in.readInt();
        String indexTypeName = in.readUTF();
        long numVectors = in.readLong();
        long minId = in.readLong();
        long maxId = in.readLong();
        return new LuminaIndexMeta(dim, metricValue, indexTypeName, numVectors, minId, maxId);
    }
}
