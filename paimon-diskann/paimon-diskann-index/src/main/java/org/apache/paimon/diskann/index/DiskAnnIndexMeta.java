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

package org.apache.paimon.diskann.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Metadata for DiskANN vector index.
 *
 * <p>Stores the file names of the companion files that live alongside the index file:
 *
 * <ul>
 *   <li>{@link #dataFileName()} — raw vector data file
 *   <li>{@link #pqPivotsFileName()} — PQ codebook (pivots)
 *   <li>{@link #pqCompressedFileName()} — PQ compressed codes
 * </ul>
 */
public class DiskAnnIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private final int dim;
    private final int metricValue;
    private final int indexTypeValue;
    private final long numVectors;
    private final long minId;
    private final long maxId;
    private final String dataFileName;
    private final String pqPivotsFileName;
    private final String pqCompressedFileName;

    public DiskAnnIndexMeta(
            int dim,
            int metricValue,
            int indexTypeValue,
            long numVectors,
            long minId,
            long maxId,
            String dataFileName,
            String pqPivotsFileName,
            String pqCompressedFileName) {
        this.dim = dim;
        this.metricValue = metricValue;
        this.indexTypeValue = indexTypeValue;
        this.numVectors = numVectors;
        this.minId = minId;
        this.maxId = maxId;
        this.dataFileName = dataFileName;
        this.pqPivotsFileName = pqPivotsFileName;
        this.pqCompressedFileName = pqCompressedFileName;
    }

    public long minId() {
        return minId;
    }

    public long maxId() {
        return maxId;
    }

    /** The file name of the separate vector data file. */
    public String dataFileName() {
        return dataFileName;
    }

    /** Serialize metadata to byte array. */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(VERSION);
        out.writeInt(dim);
        out.writeInt(metricValue);
        out.writeInt(indexTypeValue);
        out.writeLong(numVectors);
        out.writeLong(minId);
        out.writeLong(maxId);
        out.writeUTF(dataFileName);
        out.writeUTF(pqPivotsFileName);
        out.writeUTF(pqCompressedFileName);
        out.flush();
        return baos.toByteArray();
    }

    /** Deserialize metadata from byte array. */
    public static DiskAnnIndexMeta deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported DiskANN index meta version: " + version);
        }
        int dim = in.readInt();
        int metricValue = in.readInt();
        int indexTypeValue = in.readInt();
        long numVectors = in.readLong();
        long minId = in.readLong();
        long maxId = in.readLong();
        String dataFileName = in.readUTF();
        String pqPivotsFileName = in.readUTF();
        String pqCompressedFileName = in.readUTF();
        return new DiskAnnIndexMeta(
                dim,
                metricValue,
                indexTypeValue,
                numVectors,
                minId,
                maxId,
                dataFileName,
                pqPivotsFileName,
                pqCompressedFileName);
    }
}
