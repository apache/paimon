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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Bloom filter based on one memory segment. */
public class BloomFilter {

    private final BitSet bitSet;
    private final int numHashFunctions;

    public BloomFilter(long expectedEntries, int byteSize) {
        checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, (long) byteSize << 3);
        this.bitSet = new BitSet(byteSize);
    }

    @VisibleForTesting
    int numHashFunctions() {
        return numHashFunctions;
    }

    public void setMemorySegment(MemorySegment memorySegment, int offset) {
        this.bitSet.setMemorySegment(memorySegment, offset);
    }

    public void unsetMemorySegment() {
        this.bitSet.unsetMemorySegment();
    }

    public MemorySegment getMemorySegment() {
        return this.bitSet.getMemorySegment();
    }

    /**
     * Compute optimal bits number with given input entries and expected false positive probability.
     *
     * @return optimal bits number
     */
    public static int optimalNumOfBits(long inputEntries, double fpp) {
        return (int) (-inputEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    /**
     * compute the optimal hash function number with given input entries and bits size, which would
     * make the false positive probability lowest.
     *
     * @return hash function number
     */
    static int optimalNumOfHashFunctions(long expectEntries, long bitSize) {
        return Math.max(1, (int) Math.round((double) bitSize / expectEntries * Math.log(2)));
    }

    public void addHash(int hash1) {
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            bitSet.set(pos);
        }
    }

    public boolean testHash(int hash1) {
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public void reset() {
        this.bitSet.clear();
    }

    @Override
    public String toString() {
        return "BloomFilter:\n" + "\thash function number:" + numHashFunctions + "\n" + bitSet;
    }

    public static Builder builder(long expectedRow, double fpp) {
        int numBytes = (int) Math.ceil(BloomFilter.optimalNumOfBits(expectedRow, fpp) / 8D);
        Preconditions.checkArgument(
                numBytes > 0,
                "The optimal bits should > 0. expectedRow: %s, fpp: %s",
                expectedRow,
                fpp);
        return new Builder(MemorySegment.wrap(new byte[numBytes]), expectedRow);
    }

    /** Bloom filter based on one memory segment. */
    public static class Builder {

        private final MemorySegment buffer;
        private final BloomFilter filter;
        private final long expectedEntries;

        Builder(MemorySegment buffer, long expectedEntries) {
            this.buffer = buffer;
            this.filter = new BloomFilter(expectedEntries, buffer.size());
            filter.setMemorySegment(buffer, 0);
            this.expectedEntries = expectedEntries;
        }

        public boolean testHash(int hash) {
            return filter.testHash(hash);
        }

        public void addHash(int hash) {
            filter.addHash(hash);
        }

        public MemorySegment getBuffer() {
            return buffer;
        }

        public long expectedEntries() {
            return expectedEntries;
        }

        @VisibleForTesting
        public BloomFilter getFilter() {
            return filter;
        }
    }
}
