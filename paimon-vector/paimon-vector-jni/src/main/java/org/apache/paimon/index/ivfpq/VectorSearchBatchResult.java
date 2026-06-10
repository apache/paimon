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

import java.util.Arrays;

public final class VectorSearchBatchResult {

    private final long[] ids;
    private final float[] distances;
    private final int queryCount;
    private final int topK;

    public VectorSearchBatchResult(long[] ids, float[] distances, int queryCount, int topK) {
        if (ids == null) {
            throw new NullPointerException("ids");
        }
        if (distances == null) {
            throw new NullPointerException("distances");
        }
        if (queryCount < 0) {
            throw new IllegalArgumentException("queryCount must be >= 0");
        }
        if (topK < 0) {
            throw new IllegalArgumentException("topK must be >= 0");
        }
        int expectedLength = checkedResultLength(queryCount, topK);
        if (ids.length != expectedLength) {
            throw new IllegalArgumentException(
                    "ids length " + ids.length + " != queryCount * topK " + expectedLength);
        }
        if (distances.length != expectedLength) {
            throw new IllegalArgumentException(
                    "distances length "
                            + distances.length
                            + " != queryCount * topK "
                            + expectedLength);
        }
        this.ids = ids.clone();
        this.distances = distances.clone();
        this.queryCount = queryCount;
        this.topK = topK;
    }

    public int queryCount() {
        return queryCount;
    }

    public int topK() {
        return topK;
    }

    public long[] ids() {
        return ids.clone();
    }

    public float[] distances() {
        return distances.clone();
    }

    public long[] idsForQuery(int queryIndex) {
        checkQueryIndex(queryIndex);
        return Arrays.copyOfRange(ids, queryIndex * topK, (queryIndex + 1) * topK);
    }

    public float[] distancesForQuery(int queryIndex) {
        checkQueryIndex(queryIndex);
        return Arrays.copyOfRange(distances, queryIndex * topK, (queryIndex + 1) * topK);
    }

    private void checkQueryIndex(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= queryCount) {
            throw new IndexOutOfBoundsException(
                    "queryIndex " + queryIndex + " out of range [0, " + queryCount + ')');
        }
    }

    private static int checkedResultLength(int queryCount, int topK) {
        long length = (long) queryCount * (long) topK;
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("queryCount * topK overflows int");
        }
        return (int) length;
    }
}
