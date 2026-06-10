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

public final class VectorIndexReader implements AutoCloseable {

    private long nativePtr;
    private VectorIndexMetadata metadata;

    public VectorIndexReader(VectorIndexInput input) {
        if (input == null) {
            throw new NullPointerException("input");
        }
        this.nativePtr = VectorIndexNative.openReader(input);
    }

    private VectorIndexReader(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    static VectorIndexReader fromNativePointerForTesting(long nativePtr) {
        return new VectorIndexReader(nativePtr);
    }

    public VectorIndexMetadata metadata() {
        if (metadata == null) {
            metadata = VectorIndexNative.metadata(requireOpen());
        }
        return metadata;
    }

    public IndexType indexType() {
        return metadata().indexType();
    }

    public int dimension() {
        return metadata().dimension();
    }

    public long totalVectors() {
        return metadata().totalVectors();
    }

    public VectorSearchResult search(float[] query, int topK, int nprobe) {
        return search(query, topK, nprobe, 0);
    }

    public VectorSearchResult search(float[] query, int topK, int nprobe, int efSearch) {
        validateQuery(query);
        validateSearchParams(topK, nprobe, efSearch);
        return VectorIndexNative.search(requireOpen(), query, topK, nprobe, efSearch);
    }

    public VectorSearchResult search(float[] query, int topK, int nprobe, byte[] roaringFilter) {
        return search(query, topK, nprobe, 0, roaringFilter);
    }

    public VectorSearchResult search(
            float[] query, int topK, int nprobe, int efSearch, byte[] roaringFilter) {
        validateQuery(query);
        if (roaringFilter == null) {
            throw new NullPointerException("roaringFilter");
        }
        validateSearchParams(topK, nprobe, efSearch);
        return VectorIndexNative.searchWithRoaringFilter(
                requireOpen(), query, topK, nprobe, efSearch, roaringFilter);
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries, int queryCount, int topK, int nprobe) {
        return searchBatch(queries, queryCount, topK, nprobe, 0);
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries, int queryCount, int topK, int nprobe, int efSearch) {
        validateQueries(queries, queryCount);
        validateSearchParams(topK, nprobe, efSearch);
        return VectorIndexNative.searchBatch(requireOpen(), queries, queryCount, topK, nprobe, efSearch);
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries, int queryCount, int topK, int nprobe, byte[] roaringFilter) {
        return searchBatch(queries, queryCount, topK, nprobe, 0, roaringFilter);
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries,
            int queryCount,
            int topK,
            int nprobe,
            int efSearch,
            byte[] roaringFilter) {
        validateQueries(queries, queryCount);
        if (roaringFilter == null) {
            throw new NullPointerException("roaringFilter");
        }
        validateSearchParams(topK, nprobe, efSearch);
        return VectorIndexNative.searchBatchWithRoaringFilter(
                requireOpen(), queries, queryCount, topK, nprobe, efSearch, roaringFilter);
    }

    @Override
    public void close() {
        long ptr = nativePtr;
        nativePtr = 0L;
        if (ptr != 0L) {
            VectorIndexNative.freeReader(ptr);
        }
    }

    private void validateQuery(float[] query) {
        if (query == null) {
            throw new NullPointerException("query");
        }
        if (query.length != dimension()) {
            throw new IllegalArgumentException(
                    "query length " + query.length + " != index dimension " + dimension());
        }
    }

    private void validateQueries(float[] queries, int queryCount) {
        if (queries == null) {
            throw new NullPointerException("queries");
        }
        VectorIndexConfig.validatePositive(queryCount, "queryCount");
        long expected = (long) queryCount * (long) dimension();
        if (expected > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("queryCount * dimension overflows int");
        }
        if (queries.length != expected) {
            throw new IllegalArgumentException(
                    "queries length " + queries.length + " != queryCount * dimension " + expected);
        }
    }

    private static void validateSearchParams(int topK, int nprobe, int efSearch) {
        VectorIndexConfig.validatePositive(topK, "topK");
        VectorIndexConfig.validatePositive(nprobe, "nprobe");
        if (efSearch < 0) {
            throw new IllegalArgumentException("efSearch must be >= 0");
        }
    }

    private long requireOpen() {
        if (nativePtr == 0L) {
            throw new IllegalStateException("VectorIndexReader is closed");
        }
        return nativePtr;
    }
}
