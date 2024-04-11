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
import org.apache.paimon.utils.BloomFilter64;

import org.apache.hadoop.util.bloom.HashFunction;

import java.util.BitSet;

/**
 * Bloom filter for file index.
 *
 * <p>Note: This class use {@link BloomFilter64} as a base filter. Store the num hash function (one
 * integer) and bit set bytes only. Use {@link HashFunction} to hash the objects, which hash bytes
 * type(like varchar, binary, etc.) using xx hash, hash numeric type by specified number hash(see
 * http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).
 */
public class BloomFilterFileIndex implements FileIndexer {

    public static final String BLOOM_FILTER = "bloom-filter";

    private static final int DEFAULT_ITEMS = 1_000_000;
    private static final double DEFAULT_FPP = 0.1;

    private static final String ITEMS = "items";
    private static final String FPP = "fpp";

    private final DataType dataType;
    private final int items;
    private final double fpp;

    public BloomFilterFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.items = options.getInteger(ITEMS, DEFAULT_ITEMS);
        this.fpp = options.getDouble(FPP, DEFAULT_FPP);
    }

    public String name() {
        return BLOOM_FILTER;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, items, fpp);
    }

    @Override
    public FileIndexReader createReader(byte[] serializedBytes) {
        return new Reader(dataType, serializedBytes);
    }

    private static class Writer implements FileIndexWriter {

        private final BloomFilter64 filter;
        private final FastHash hashFunction;

        public Writer(DataType type, int items, double fpp) {
            this.filter = new BloomFilter64(items, fpp);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        @Override
        public void write(Object key) {
            if (key != null) {
                filter.addHash(hashFunction.hash(key));
            }
        }

        @Override
        public byte[] serializedBytes() {
            int numHashFunctions = filter.getNumHashFunctions();
            byte[] bytes = filter.getBitSet().toByteArray();
            byte[] serialized = new byte[bytes.length + Integer.BYTES];
            serialized[0] = (byte) ((numHashFunctions >>> 24) & 0xFF);
            serialized[1] = (byte) ((numHashFunctions >>> 16) & 0xFF);
            serialized[2] = (byte) ((numHashFunctions >>> 8) & 0xFF);
            serialized[3] = (byte) (numHashFunctions & 0xFF);
            System.arraycopy(bytes, 0, serialized, 4, bytes.length);
            return serialized;
        }
    }

    private static class Reader implements FileIndexReader {

        private final BloomFilter64 filter;
        private final FastHash hashFunction;

        public Reader(DataType type, byte[] serializedBytes) {
            int numHashFunctions =
                    ((serializedBytes[0] << 24)
                            + (serializedBytes[1] << 16)
                            + (serializedBytes[2] << 8)
                            + serializedBytes[3]);
            byte[] bytes = new byte[serializedBytes.length - Integer.BYTES];
            System.arraycopy(serializedBytes, 4, bytes, 0, bytes.length);
            BitSet bitSet = BitSet.valueOf(bytes);
            this.filter = new BloomFilter64(numHashFunctions, bitSet);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        @Override
        public Boolean visitEqual(FieldRef fieldRef, Object key) {
            return key == null || filter.testHash(hashFunction.hash(key));
        }
    }
}
