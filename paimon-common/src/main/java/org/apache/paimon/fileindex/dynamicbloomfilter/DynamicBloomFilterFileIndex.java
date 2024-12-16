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

package org.apache.paimon.fileindex.dynamicbloomfilter;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bloomfilter.FastHash;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.BloomFilter64;
import org.apache.paimon.utils.DynamicBloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/** implementation of dynamic bloom filter file index. */
public class DynamicBloomFilterFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;

    private static final int DEFAULT_ITEMS = 1_000_000;
    private static final double DEFAULT_FPP = 0.1;
    private static final int DEFAULT_MAX_ITEMS = 10_000_000;

    private static final String ITEMS = "items";
    private static final String FPP = "fpp";
    private static final String MAX_ITEMS = "max_items";

    private final DataType dataType;
    private final int items;
    private final double fpp;

    private final int maxItems;

    public DynamicBloomFilterFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.items = options.getInteger(ITEMS, DEFAULT_ITEMS);
        this.fpp = options.getDouble(FPP, DEFAULT_FPP);
        this.maxItems = options.getInteger(MAX_ITEMS, DEFAULT_MAX_ITEMS);
    }

    @Override
    public FileIndexWriter createWriter() {
        return new DynamicBloomFilterFileIndex.Writer(dataType, items, fpp, maxItems);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            DataInput input = new DataInputStream(inputStream);
            byte version = input.readByte();
            if (version > VERSION_1) {
                throw new RuntimeException(
                        String.format(
                                "read dynamicBloomFilter index file fail, "
                                        + "your plugin version is lower than %d",
                                version));
            }
            int numBloomFilters = input.readInt();
            int vectorSize = input.readInt();
            int numHashFunctions = input.readInt();

            BloomFilter64[] bloomFilter64s = new BloomFilter64[numBloomFilters];
            for (int i = 0; i < numBloomFilters; i++) {
                byte[] serializedBytes = new byte[vectorSize / Byte.SIZE];
                input.readFully(serializedBytes);
                BloomFilter64.BitSet bitSet = new BloomFilter64.BitSet(serializedBytes, 0);
                bloomFilter64s[i] = new BloomFilter64(numHashFunctions, bitSet);
            }
            return new DynamicBloomFilterFileIndex.Reader(dataType, bloomFilter64s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Reader extends FileIndexReader {

        private final DynamicBloomFilter dynamicBloomFilter;
        private final FastHash hashFunction;

        public Reader(DataType type, BloomFilter64[] bloomFilter64s) {
            this.dynamicBloomFilter = new DynamicBloomFilter(bloomFilter64s);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object key) {
            return key == null || dynamicBloomFilter.testHash(hashFunction.hash(key))
                    ? REMAIN
                    : SKIP;
        }
    }

    private static class Writer extends FileIndexWriter {

        private final DynamicBloomFilter filter;
        private final FastHash hashFunction;

        public Writer(DataType type, int items, double fpp, int maxItems) {
            this.filter = new DynamicBloomFilter(items, fpp, maxItems);
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
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutput out = new DataOutputStream(bos);
                BloomFilter64[] bloomFilterMatrix = filter.matrix();

                // 1. write meta
                out.writeByte(VERSION_1);
                out.writeInt(bloomFilterMatrix.length);
                // each bloom filter has same num of hashFunction and bitSet
                out.writeInt(bloomFilterMatrix[0].getBitSet().bitSize());
                out.writeInt(bloomFilterMatrix[0].getNumHashFunctions());

                // 2. write each filter's bitset
                for (BloomFilter64 filterMatrix : bloomFilterMatrix) {
                    byte[] serialized = new byte[filterMatrix.getBitSet().bitSize() / Byte.SIZE];
                    filterMatrix.getBitSet().toByteArray(serialized, 0, serialized.length);
                    out.write(serialized);
                }
                return bos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
