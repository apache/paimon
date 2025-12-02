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

package org.apache.paimon.vector;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Metadata for the vector index, including dimension, similarity function, and HNSW parameters. */
public class VectorIndexMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int dimension;
    private final String similarityFunction;
    private final int m;
    private final int efConstruction;

    public VectorIndexMetadata(
            int dimension, String similarityFunction, int m, int efConstruction) {
        this.dimension = dimension;
        this.similarityFunction = similarityFunction;
        this.m = m;
        this.efConstruction = efConstruction;
    }

    public static byte[] serializeMetadata(VectorIndexMetadata vectorIndexMetadata)
            throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
            dataOut.writeInt(vectorIndexMetadata.dimension());
            dataOut.writeUTF(vectorIndexMetadata.similarityFunction());
            dataOut.writeInt(vectorIndexMetadata.m());
            dataOut.writeInt(vectorIndexMetadata.efConstruction());
        }
        return byteOut.toByteArray();
    }

    public int dimension() {
        return dimension;
    }

    public String similarityFunction() {
        return similarityFunction;
    }

    public int m() {
        return m;
    }

    public int efConstruction() {
        return efConstruction;
    }
}
