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

public abstract class VectorIndexConfig {

    private final IndexType indexType;
    private final int dimension;
    private final int nlist;
    private final Metric metric;

    VectorIndexConfig(IndexType indexType, int dimension, int nlist, Metric metric) {
        if (indexType == null) {
            throw new NullPointerException("indexType");
        }
        if (metric == null) {
            throw new NullPointerException("metric");
        }
        validatePositive(dimension, "dimension");
        validatePositive(nlist, "nlist");
        this.indexType = indexType;
        this.dimension = dimension;
        this.nlist = nlist;
        this.metric = metric;
    }

    public static VectorIndexConfig ivfFlat(int dimension, int nlist, Metric metric) {
        return new IvfFlatConfig(dimension, nlist, metric);
    }

    public static VectorIndexConfig ivfPq(
            int dimension, int nlist, int m, Metric metric, boolean useOpq) {
        return new IvfPqConfig(dimension, nlist, m, metric, useOpq);
    }

    public static VectorIndexConfig ivfHnswFlat(
            int dimension, int nlist, Metric metric, HnswConfig hnsw) {
        return new IvfHnswFlatConfig(dimension, nlist, metric, hnsw);
    }

    public static VectorIndexConfig ivfHnswSq(
            int dimension, int nlist, Metric metric, HnswConfig hnsw) {
        return new IvfHnswSqConfig(dimension, nlist, metric, hnsw);
    }

    public IndexType indexType() {
        return indexType;
    }

    public int dimension() {
        return dimension;
    }

    public int nlist() {
        return nlist;
    }

    public Metric metric() {
        return metric;
    }

    int pqM() {
        return 0;
    }

    boolean useOpq() {
        return false;
    }

    HnswConfig hnsw() {
        return HnswConfig.DEFAULT;
    }

    static void validatePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }
}
