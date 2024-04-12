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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Bloom filter 64 handle 64 bits hash. */
public final class BloomFilter64 {

    private final BitSet bitSet;
    private final int numBits;
    private final int numHashFunctions;

    public BloomFilter64(long items, double fpp) {
        int nb = (int) (-items * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        this.numHashFunctions =
                Math.max(1, (int) Math.round((double) numBits / items * Math.log(2)));
        this.bitSet = new BitSet(new long[numBits / Long.SIZE]);
    }

    public BloomFilter64(int numHashFunctions, BitSet bitSet) {
        this.numHashFunctions = numHashFunctions;
        this.numBits = bitSet.bitSize();
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

    /** Bit set used for bloom filter 64. */
    public static class BitSet {

        public final long[] data;

        public BitSet(byte[] bytes, int offset, int length) {
            assert length % 8 == 0 : "data length should be divisible by 8";
            int longSize = length / 8;
            ByteBuffer bb = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
            this.data = new long[longSize];
            for (int i = 0; i < longSize; i++) {
                this.data[i] = bb.getLong();
            }
        }

        public BitSet(long[] data) {
            assert data.length > 0 : "data length is zero!";
            this.data = data;
        }

        public void set(int index) {
            data[index >>> 6] |= (1L << index);
        }

        public boolean get(int index) {
            return (data[index >>> 6] & (1L << index)) != 0;
        }

        public int bitSize() {
            return data.length * Long.SIZE;
        }

        public void toByteArray(byte[] bytes, int offset, int length) {
            assert length >= data.length * Long.BYTES;
            ByteBuffer bb = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < data.length; i++) {
                bb.putLong(data[i]);
            }
        }
    }
}
