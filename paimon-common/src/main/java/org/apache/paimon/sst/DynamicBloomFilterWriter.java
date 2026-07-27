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

package org.apache.paimon.sst;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Bloom filter writer which sizes the filter from the actual number of entries. */
final class DynamicBloomFilterWriter implements BloomFilterWriter {

    private static final int INITIAL_HASH_CAPACITY = 1024;

    private final double falsePositiveProbability;

    @Nullable private int[] hashes;
    private int hashCount;

    DynamicBloomFilterWriter(double falsePositiveProbability) {
        checkArgument(
                falsePositiveProbability > 0 && falsePositiveProbability < 1,
                "Bloom filter false positive probability must be between 0 and 1.");
        this.falsePositiveProbability = falsePositiveProbability;
        this.hashes = new int[INITIAL_HASH_CAPACITY];
    }

    @Override
    public void addHash(int hash) {
        if (hashes == null) {
            return;
        }
        if (hashCount == hashes.length) {
            hashes = Arrays.copyOf(hashes, Math.multiplyExact(hashes.length, 2));
        }
        hashes[hashCount++] = hash;
    }

    @Override
    public BloomFilterHandle write(PositionOutputStream out) throws IOException {
        if (hashes == null || hashCount == 0) {
            return null;
        }

        BloomFilter.Builder bloomFilter = BloomFilter.builder(hashCount, falsePositiveProbability);
        for (int i = 0; i < hashCount; i++) {
            bloomFilter.addHash(hashes[i]);
        }
        hashes = null;
        return BloomFilterWriterUtils.write(out, bloomFilter);
    }
}
