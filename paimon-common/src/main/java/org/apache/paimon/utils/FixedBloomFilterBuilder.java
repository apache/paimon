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

import org.apache.paimon.memory.MemorySegment;

/** Bloom filter builder sized from an expected number of entries. */
final class FixedBloomFilterBuilder implements BloomFilter.Builder {

    private final BloomFilter filter;

    FixedBloomFilterBuilder(long expectedEntries, double falsePositiveProbability) {
        Preconditions.checkArgument(expectedEntries > 0, "expectedEntries must be positive.");
        Preconditions.checkArgument(
                falsePositiveProbability > 0 && falsePositiveProbability < 1,
                "Bloom filter false positive probability must be between 0 and 1.");
        int numBytes =
                (int)
                        Math.ceil(
                                BloomFilter.optimalNumOfBits(
                                                expectedEntries, falsePositiveProbability)
                                        / 8D);
        Preconditions.checkArgument(
                numBytes > 0,
                "The optimal bits should > 0. expectedEntries: %s, fpp: %s",
                expectedEntries,
                falsePositiveProbability);
        MemorySegment buffer = MemorySegment.wrap(new byte[numBytes]);
        this.filter = new BloomFilter(expectedEntries, numBytes);
        this.filter.setMemorySegment(buffer, 0);
    }

    @Override
    public void addHash(int hash) {
        filter.addHash(hash);
    }

    @Override
    public BloomFilter build() {
        return filter;
    }
}
