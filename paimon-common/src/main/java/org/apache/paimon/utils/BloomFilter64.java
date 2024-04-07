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

import java.util.BitSet;

/** Bloom filter 64 handle 64 bits hash. */
public class BloomFilter64 {

    private final BitSet bitSet;
    private final int numBits;
    private final int numHashFunctions;

    public BloomFilter64(long items, double fpp) {
        int nb = (int) (-items * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        this.numHashFunctions =
                Math.max(1, (int) Math.round((double) numBits / items * Math.log(2)));
        this.bitSet = new BitSet(numBits);
    }

    public BloomFilter64(int numHashFunctions, BitSet bitSet) {
        this.numHashFunctions = numHashFunctions;
        this.numBits = bitSet.size();
        this.bitSet = bitSet;
    }

    public void addHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    public boolean testHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public BitSet getBitSet() {
        return bitSet;
    }
}
