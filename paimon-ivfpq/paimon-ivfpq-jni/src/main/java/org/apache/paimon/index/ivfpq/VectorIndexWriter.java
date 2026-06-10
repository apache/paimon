// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.paimon.index.ivfpq;

public final class VectorIndexWriter implements AutoCloseable {

    private final VectorIndexConfig config;
    private long nativePtr;

    public VectorIndexWriter(VectorIndexConfig config) {
        if (config == null) {
            throw new NullPointerException("config");
        }
        this.config = config;
        HnswConfig hnsw = config.hnsw();
        this.nativePtr =
                VectorIndexNative.createWriter(
                        config.indexType().code(),
                        config.dimension(),
                        config.nlist(),
                        config.pqM(),
                        config.metric().code(),
                        config.useOpq(),
                        hnsw.m(),
                        hnsw.efConstruction(),
                        hnsw.maxLevel());
    }

    private VectorIndexWriter(long nativePtr, VectorIndexConfig config) {
        this.nativePtr = nativePtr;
        this.config = config;
    }

    static VectorIndexWriter fromNativePointerForTesting(long nativePtr, VectorIndexConfig config) {
        return new VectorIndexWriter(nativePtr, config);
    }

    public VectorIndexConfig config() {
        return config;
    }

    public int dimension() {
        return config.dimension();
    }

    public void train(float[] data, int vectorCount) {
        validateVectors(data, vectorCount);
        VectorIndexNative.train(requireOpen(), data, vectorCount);
    }

    public void addVectors(long[] ids, float[] data, int vectorCount) {
        if (ids == null) {
            throw new NullPointerException("ids");
        }
        validateVectors(data, vectorCount);
        if (ids.length < vectorCount) {
            throw new IllegalArgumentException(
                    "ids length " + ids.length + " < vectorCount " + vectorCount);
        }
        VectorIndexNative.addVectors(requireOpen(), ids, data, vectorCount);
    }

    public void writeIndex(Object output) {
        if (output == null) {
            throw new NullPointerException("output");
        }
        VectorIndexNative.writeIndex(requireOpen(), output);
    }

    @Override
    public void close() {
        long ptr = nativePtr;
        nativePtr = 0L;
        if (ptr != 0L) {
            VectorIndexNative.freeWriter(ptr);
        }
    }

    private void validateVectors(float[] data, int vectorCount) {
        if (data == null) {
            throw new NullPointerException("data");
        }
        VectorIndexConfig.validatePositive(vectorCount, "vectorCount");
        long expected = (long) vectorCount * (long) config.dimension();
        if (expected > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("vectorCount * dimension overflows int");
        }
        if (data.length < expected) {
            throw new IllegalArgumentException(
                    "data length " + data.length + " < vectorCount * dimension " + expected);
        }
    }

    private long requireOpen() {
        if (nativePtr == 0L) {
            throw new IllegalStateException("VectorIndexWriter is closed");
        }
        return nativePtr;
    }
}
