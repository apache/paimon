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

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

/** Bloom filter for file index. */
public class BloomFilter implements FileIndexer {

    public static final String BLOOM_FILTER = "bloom";

    private final HashConverter64 hashFunction;

    private final BloomFilterImpl filter;

    public BloomFilter(DataType type, Options options) {
        int items = options.getInteger("items", 1_000_000);
        double fpp = options.getDouble("fpp", 0.1);
        this.hashFunction = type.accept(FastHash.INSTANCE);
        this.filter = new BloomFilterImpl(items, fpp);
    }

    public String name() {
        return BLOOM_FILTER;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer();
    }

    @Override
    public FileIndexReader createReader(byte[] serializedBytes) {
        return new Reader(serializedBytes);
    }

    private class Writer implements FileIndexWriter {

        private Writer() {}

        @Override
        public void write(Object key) {
            filter.addHash(hashFunction.hash(key));
        }

        @Override
        public byte[] serializedBytes() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            DataOutputStream dos = new DataOutputStream(baos);

            try {
                filter.write(dos);
                byte[] bytes = baos.toByteArray();
                dos.close();
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class Reader implements FileIndexReader {

        public Reader(byte[] serializedBytes) {
            recoverFrom(serializedBytes);
        }

        public void recoverFrom(byte[] serializedBytes) {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedBytes));

            try {
                filter.read(dis);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Boolean visitEqual(FieldRef fieldRef, Object key) {
            return filter.testHash(hashFunction.hash(key));
        }
    }

    /** Bloom filter impl for 64 bit long hash code. */
    private static class BloomFilterImpl {

        private BitSet bitSet;
        private int numBits;
        private int numHashFunctions;

        public BloomFilterImpl(long items, double fpp) {
            this.numBits = optimalNumOfBits(items, fpp);
            this.numHashFunctions = optimalNumOfHashFunctions(items, numBits);
            this.bitSet = new BitSet(numBits);
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

        public void write(DataOutput out) throws IOException {
            out.writeInt(this.numHashFunctions);
            out.writeInt(this.numBits);
            byte[] b = bitSet.toByteArray();
            out.writeInt(b.length);
            out.write(b);
        }

        public void read(DataInput input) throws IOException {
            this.numHashFunctions = input.readInt();
            this.numBits = input.readInt();
            byte[] b = new byte[input.readInt()];
            input.readFully(b);
            this.bitSet = BitSet.valueOf(b);
        }
    }

    static int optimalNumOfHashFunctions(long items, long numBits) {
        return Math.max(1, (int) Math.round((double) numBits / items * Math.log(2)));
    }

    static int optimalNumOfBits(long items, double fpp) {
        return (int) (-items * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }
}
