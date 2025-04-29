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

package org.apache.paimon.fileindex.token;

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
import org.apache.paimon.utils.BloomFilter64.BitSet;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/**
 * Token bloom filter for file index.
 */
public class TokenBloomFilterFileIndex implements FileIndexer {

    private static final int DEFAULT_ITEMS = 1_000_000;
    private static final double DEFAULT_FPP = 0.1;
    private static final String DELIMITER = " ";

    private static final String ITEMS = "items";
    private static final String FPP = "fpp";
    private static final String TOKEN_DELIMITER = "token.delimiter";

    private final DataType dataType;
    private final int items;
    private final double fpp;
    private final String delimiter;

    public TokenBloomFilterFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.items = options.getInteger(ITEMS, DEFAULT_ITEMS);
        this.fpp = options.getDouble(FPP, DEFAULT_FPP);
        this.delimiter = options.getString(TOKEN_DELIMITER, DELIMITER);
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, items, fpp, delimiter);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            byte[] serializedBytes = new byte[length];
            IOUtils.readFully(inputStream, serializedBytes);
            return new Reader(dataType, serializedBytes, delimiter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Writer extends FileIndexWriter {
        private final BloomFilter64 filter;
        private final FastHash hashFunction;
        private final String delimiter;

        public Writer(DataType type, int items, double fpp, String delimiter) {
            this.filter = new BloomFilter64(items, fpp);
            this.hashFunction = FastHash.getHashFunction(type);
            this.delimiter = delimiter;
        }

        @Override
        public void write(Object key) {
            if (key != null && key instanceof String) {
                String text = (String) key;
                Arrays.stream(text.split(delimiter))
                        .filter(token -> !token.isEmpty())
                        .forEach(token -> filter.addHash(hashFunction.hash(token)));
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
        private final String delimiter;

        public Reader(DataType type, byte[] serializedBytes, String delimiter) {
            int numHashFunctions =
                    ((serializedBytes[0] << 24)
                            + (serializedBytes[1] << 16)
                            + (serializedBytes[2] << 8)
                            + serializedBytes[3]);
            BitSet bitSet = new BitSet(serializedBytes, 4);
            this.filter = new BloomFilter64(numHashFunctions, bitSet);
            this.hashFunction = FastHash.getHashFunction(type);
            this.delimiter = delimiter;
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object key) {
            if (key == null){
                return REMAIN;
            }
            if (key instanceof String) {
                String text = (String) key;
                boolean anyTokenMatch =
                        Arrays.stream(text.split(delimiter))
                                .filter(token -> !token.isEmpty())
                                .anyMatch(token -> filter.testHash(hashFunction.hash(token)));
                return anyTokenMatch ? REMAIN : SKIP;
            }
            return REMAIN;
        }
    }
}
