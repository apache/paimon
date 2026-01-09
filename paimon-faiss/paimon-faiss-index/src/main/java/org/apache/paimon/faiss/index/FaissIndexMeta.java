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

package org.apache.paimon.faiss.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Metadata for FAISS vector index. */
public class FaissIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private final int dim;
    private final int metricValue;
    private final int indexTypeOrdinal;
    private final long numVectors;
    private final long minId;
    private final long maxId;

    public FaissIndexMeta(
            int dim,
            int metricValue,
            int indexTypeOrdinal,
            long numVectors,
            long minId,
            long maxId) {
        this.dim = dim;
        this.metricValue = metricValue;
        this.indexTypeOrdinal = indexTypeOrdinal;
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

    public int indexTypeOrdinal() {
        return indexTypeOrdinal;
    }

    public long numVectors() {
        return numVectors;
    }

    public long minId() {
        return minId;
    }

    public long maxId() {
        return maxId;
    }

    /** Serialize metadata to byte array. */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(VERSION);
        out.writeInt(dim);
        out.writeInt(metricValue);
        out.writeInt(indexTypeOrdinal);
        out.writeLong(numVectors);
        out.writeLong(minId);
        out.writeLong(maxId);
        out.flush();
        return baos.toByteArray();
    }

    /** Deserialize metadata from byte array. */
    public static FaissIndexMeta deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported FAISS index meta version: " + version);
        }
        int dim = in.readInt();
        int metricValue = in.readInt();
        int indexTypeOrdinal = in.readInt();
        long numVectors = in.readLong();
        long minId = in.readLong();
        long maxId = in.readLong();
        return new FaissIndexMeta(dim, metricValue, indexTypeOrdinal, numVectors, minId, maxId);
    }
}
