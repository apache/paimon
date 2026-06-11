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

package org.apache.paimon.index.vector;

final class VectorIndexNative {

    static {
        NativeLoader.loadJni();
    }

    private VectorIndexNative() {}

    static native long createWriter(String[] optionKeys, String[] optionValues);

    static native int writerDimension(long ptr);

    static native void train(long ptr, float[] data, int n);

    static native void addVectors(long ptr, long[] ids, float[] data, int n);

    static native void writeIndex(long ptr, Object streamOutput);

    static native void freeWriter(long ptr);

    static native long openReader(Object streamInput);

    static native VectorIndexMetadata metadata(long ptr);

    static native VectorSearchResult search(long ptr, float[] query, int k, int nprobe, int efSearch);

    static native VectorSearchResult searchWithRoaringFilter(
            long ptr, float[] query, int k, int nprobe, int efSearch, byte[] roaringFilter);

    static native VectorSearchBatchResult searchBatch(
            long ptr, float[] queries, int queryCount, int k, int nprobe, int efSearch);

    static native VectorSearchBatchResult searchBatchWithRoaringFilter(
            long ptr,
            float[] queries,
            int queryCount,
            int k,
            int nprobe,
            int efSearch,
            byte[] roaringFilter);

    static native void freeReader(long ptr);
}
