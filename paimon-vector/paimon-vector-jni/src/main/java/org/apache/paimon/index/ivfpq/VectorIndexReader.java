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

    private final Object nativeHandleLock = new Object();
    private long nativePtr;
    private Thread nativeHandleOwner;
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
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                requireOpen();
                if (metadata == null) {
                    metadata = VectorIndexNative.metadata(nativePtr);
                }
                return metadata;
            } finally {
                exitNativeHandle();
            }
        }
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
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                return VectorIndexNative.search(requireOpen(), query, topK, nprobe, efSearch);
            } finally {
                exitNativeHandle();
            }
        }
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
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                return VectorIndexNative.searchWithRoaringFilter(
                        requireOpen(), query, topK, nprobe, efSearch, roaringFilter);
            } finally {
                exitNativeHandle();
            }
        }
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries, int queryCount, int topK, int nprobe) {
        return searchBatch(queries, queryCount, topK, nprobe, 0);
    }

    public VectorSearchBatchResult searchBatch(
            float[] queries, int queryCount, int topK, int nprobe, int efSearch) {
        if (queries == null) {
            throw new NullPointerException("queries");
        }
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                return VectorIndexNative.searchBatch(
                        requireOpen(), queries, queryCount, topK, nprobe, efSearch);
            } finally {
                exitNativeHandle();
            }
        }
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
        if (queries == null) {
            throw new NullPointerException("queries");
        }
        if (roaringFilter == null) {
            throw new NullPointerException("roaringFilter");
        }
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                return VectorIndexNative.searchBatchWithRoaringFilter(
                        requireOpen(), queries, queryCount, topK, nprobe, efSearch, roaringFilter);
            } finally {
                exitNativeHandle();
            }
        }
    }

    @Override
    public void close() {
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                long ptr = nativePtr;
                nativePtr = 0L;
                if (ptr != 0L) {
                    VectorIndexNative.freeReader(ptr);
                }
            } finally {
                exitNativeHandle();
            }
        }
    }

    private void validateQuery(float[] query) {
        if (query == null) {
            throw new NullPointerException("query");
        }
    }

    private long requireOpen() {
        if (nativePtr == 0L) {
            throw new IllegalStateException("VectorIndexReader is closed");
        }
        return nativePtr;
    }

    private void enterNativeHandle() {
        Thread current = Thread.currentThread();
        if (nativeHandleOwner == current) {
            throw new IllegalStateException("VectorIndexReader native handle is already in use");
        }
        nativeHandleOwner = current;
    }

    private void exitNativeHandle() {
        nativeHandleOwner = null;
    }
}
