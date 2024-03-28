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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

/* This file is based on source code from the Hadoop Project (https://hadoop.apache.org//), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Mostly copied from hadoop BloomFilter. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HadoopBloomFilter extends HadoopFilter {
    private static final byte[] bitvalues =
            new byte[] {
                (byte) 0x01,
                (byte) 0x02,
                (byte) 0x04,
                (byte) 0x08,
                (byte) 0x10,
                (byte) 0x20,
                (byte) 0x40,
                (byte) 0x80
            };

    /** The bit vector. */
    BitSet bits;

    /** Default constructor - use with readFields. */
    public HadoopBloomFilter() {
        super();
    }

    /**
     * Constructor.
     *
     * @param vectorSize The vector size of <i>this</i> filter.
     * @param nbHash The number of hash function to consider.
     * @param hashType type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
     */
    public HadoopBloomFilter(int vectorSize, int nbHash, int hashType) {
        super(vectorSize, nbHash, hashType);

        bits = new BitSet(this.vectorSize);
    }

    @Override
    public boolean add(Key key) {
        if (key == null) {
            throw new NullPointerException("key cannot be null");
        }

        int[] h = hash.hash(key);
        hash.clear();

        boolean newRecord = false;
        for (int i = 0; i < nbHash; i++) {
            if (!bits.get(h[i])) {
                newRecord = true;
                bits.set(h[i]);
            }
        }
        return newRecord;
    }

    @Override
    public boolean addHash(long hash64) {
        boolean newRecord = false;
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 0; i < nbHash; i++) {
            int hash = hash1 + (i * hash2);
            if (!bits.get(hash)) {
                newRecord = true;
                bits.set(hash);
            }
        }
        return newRecord;
    }

    @Override
    public void and(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be and-ed");
        }

        this.bits.and(((HadoopBloomFilter) filter).bits);
    }

    @Override
    public boolean membershipTest(Key key) {
        if (key == null) {
            throw new NullPointerException("key cannot be null");
        }

        int[] h = hash.hash(key);
        hash.clear();
        for (int i = 0; i < nbHash; i++) {
            if (!bits.get(h[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean membershipTest(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 0; i < nbHash; i++) {
            int hash = hash1 + (i * hash2);
            if (!bits.get(hash)) {
                return false;
            }
        }

        return false;
    }

    @Override
    public void not() {
        bits.flip(0, vectorSize);
    }

    @Override
    public void or(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be or-ed");
        }
        bits.or(((HadoopBloomFilter) filter).bits);
    }

    @Override
    public void xor(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be xor-ed");
        }
        bits.xor(((HadoopBloomFilter) filter).bits);
    }

    @Override
    public String toString() {
        return bits.toString();
    }

    /** @return size of the the bloomfilter */
    public int getVectorSize() {
        return this.vectorSize;
    }

    // Writable

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        byte[] bytes = new byte[getNBytes()];
        for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if (bitIndex == 0) {
                bytes[byteIndex] = 0;
            }
            if (bits.get(i)) {
                bytes[byteIndex] |= bitvalues[bitIndex];
            }
        }
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        bits = new BitSet(this.vectorSize);
        byte[] bytes = new byte[getNBytes()];
        in.readFully(bytes);
        for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
                bits.set(i);
            }
        }
    }

    /* @return number of bytes needed to hold bit vector */
    private int getNBytes() {
        return (int) (((long) vectorSize + 7) / 8);
    }
} // end class
