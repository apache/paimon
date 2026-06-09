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

package org.apache.paimon.index.ivfpq;

final class IVFPQNative {

    static {
        NativeLoader.loadJni();
    }

    private IVFPQNative() {}

    static native long createWriter(int d, int nlist, int m, int metric, boolean useOpq);

    static native void train(long ptr, float[] data, int n);

    static native void addVectors(long ptr, long[] ids, float[] data, int n);

    static native void writeIndex(long ptr, Object streamOutput);

    static native void freeWriter(long ptr);

    static native long openReader(Object streamInput);

    static native IVFPQResult search(long ptr, float[] query, int k, int nprobe);

    static native IVFPQResult searchWithRoaringFilter(
            long ptr, float[] query, int k, int nprobe, byte[] roaringFilter);

    static native int getDimension(long ptr);

    static native long getTotalVectors(long ptr);

    static native IVFPQBatchResult searchBatch(
            long ptr, float[] queries, int queryCount, int k, int nprobe);

    static native IVFPQBatchResult searchBatchWithRoaringFilter(
            long ptr, float[] queries, int queryCount, int k, int nprobe, byte[] roaringFilter);

    static native void freeReader(long ptr);
}
