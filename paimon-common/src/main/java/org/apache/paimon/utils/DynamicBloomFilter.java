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

package org.apache.paimon.utils;

/* This file is based on source code from the hudi Project (http://hudi.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Dynamic Bloom Filter. This is largely based of
 * org.apache.hudi.common.bloom.InternalDynamicBloomFilter.
 */
public class DynamicBloomFilter {

    /** Threshold for the maximum number of key to record in a dynamic Bloom filter row. */
    private int items;

    /** The number of keys recorded in the current standard active Bloom filter. */
    private int currentNbRecord;

    private int maxItems;
    private boolean reachedMax = false;
    private int curMatrixIndex = 0;
    private double fpp;

    /** The matrix of Bloom filter. */
    private BloomFilter64[] matrix;

    public DynamicBloomFilter(BloomFilter64[] bloomFilter64s) {
        this.matrix = bloomFilter64s;
    }

    public DynamicBloomFilter(int items, double fpp, int maxItems) {
        this.items = items;
        this.currentNbRecord = 0;
        this.maxItems = maxItems;
        this.fpp = fpp;
        matrix = new BloomFilter64[1];
        matrix[0] = new BloomFilter64(items, fpp);
    }

    public void addHash(long hash64) {
        BloomFilter64 bf = getActiveStandardBF();
        if (bf == null) {
            addRow();
            bf = matrix[matrix.length - 1];
            currentNbRecord = 0;
        }
        bf.addHash(hash64);
        currentNbRecord++;
    }

    /** Adds a new row to <i>this</i> dynamic Bloom filter. */
    private void addRow() {
        BloomFilter64[] tmp = new BloomFilter64[matrix.length + 1];
        System.arraycopy(matrix, 0, tmp, 0, matrix.length);
        tmp[tmp.length - 1] = new BloomFilter64(items, fpp);
        matrix = tmp;
    }

    public boolean testHash(long key) {
        for (BloomFilter64 bloomFilter : matrix) {
            if (bloomFilter.testHash(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
     *
     * @return BloomFilter64 The active standard Bloom filter. <code>Null</code> otherwise.
     */
    private BloomFilter64 getActiveStandardBF() {
        if (reachedMax) {
            return matrix[curMatrixIndex++ % matrix.length];
        }

        if (currentNbRecord >= items && (matrix.length * items) < maxItems) {
            return null;
        } else if (currentNbRecord >= items && (matrix.length * items) >= maxItems) {
            reachedMax = true;
            return matrix[0];
        }
        return matrix[matrix.length - 1];
    }

    public BloomFilter64[] matrix() {
        return matrix;
    }
}
