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

import org.apache.paimon.fileindex.FileIndexFilterPushDownAnalyzer;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.BloomFilter64;
import org.apache.paimon.utils.BloomFilter64.BitSet;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.util.bloom.HashFunction;

import java.io.IOException;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/**
 * Bloom filter for file index.
 *
 * <p>Note: This class use {@link BloomFilter64} as a base filter. Store the num hash function (one
 * integer) and bit set bytes only. Use {@link HashFunction} to hash the objects, which hash bytes
 * type(like varchar, binary, etc.) using xx hash, hash numeric type by specified number hash(see
 * http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).
 */
public class BloomFilterFileIndex implements FileIndexer {

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

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, items, fpp);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            byte[] serializedBytes = new byte[length];
            IOUtils.readFully(inputStream, serializedBytes);
            return new Reader(dataType, serializedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileIndexFilterPushDownAnalyzer createFilterPushDownAnalyzer() {
        return new FilterPushDownAnalyzer();
    }

    private static class Writer extends FileIndexWriter {

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
            byte[] serialized = new byte[filter.getBitSet().bitSize() / Byte.SIZE + Integer.BYTES];
            // big endian
            serialized[0] = (byte) ((numHashFunctions >>> 24) & 0xFF);
            serialized[1] = (byte) ((numHashFunctions >>> 16) & 0xFF);
            serialized[2] = (byte) ((numHashFunctions >>> 8) & 0xFF);
            serialized[3] = (byte) (numHashFunctions & 0xFF);
            filter.getBitSet().toByteArray(serialized, 4, serialized.length - 4);
            return serialized;
        }
    }

    private static class Reader extends FileIndexReader {

        private final BloomFilter64 filter;
        private final FastHash hashFunction;

        public Reader(DataType type, byte[] serializedBytes) {
            // little endian
            int numHashFunctions =
                    ((serializedBytes[0] << 24)
                            + (serializedBytes[1] << 16)
                            + (serializedBytes[2] << 8)
                            + serializedBytes[3]);
            BitSet bitSet = new BitSet(serializedBytes, 4);
            this.filter = new BloomFilter64(numHashFunctions, bitSet);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object key) {
            return key == null || filter.testHash(hashFunction.hash(key)) ? REMAIN : SKIP;
        }
    }

    private static class FilterPushDownAnalyzer extends FileIndexFilterPushDownAnalyzer {}
}
