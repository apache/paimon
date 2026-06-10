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

public final class VectorIndexMetadata {

    private final IndexType indexType;
    private final int dimension;
    private final int nlist;
    private final Metric metric;
    private final long totalVectors;
    private final int pqM;
    private final int hnswM;
    private final int hnswEfConstruction;
    private final int hnswMaxLevel;

    public VectorIndexMetadata(
            int indexType,
            int dimension,
            int nlist,
            int metric,
            long totalVectors,
            int pqM,
            int hnswM,
            int efConstruction,
            int maxLevel) {
        this.indexType = IndexType.fromCode(indexType);
        this.dimension = dimension;
        this.nlist = nlist;
        this.metric = metricFromCode(metric);
        this.totalVectors = totalVectors;
        this.pqM = pqM;
        this.hnswM = hnswM;
        this.hnswEfConstruction = efConstruction;
        this.hnswMaxLevel = maxLevel;
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

    public long totalVectors() {
        return totalVectors;
    }

    public int pqM() {
        return pqM;
    }

    public int hnswM() {
        return hnswM;
    }

    public int hnswEfConstruction() {
        return hnswEfConstruction;
    }

    public int hnswMaxLevel() {
        return hnswMaxLevel;
    }

    private static Metric metricFromCode(int code) {
        for (Metric metric : Metric.values()) {
            if (metric.code() == code) {
                return metric;
            }
        }
        throw new IllegalArgumentException("unknown metric code: " + code);
    }
}
