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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Bloom filter for file index. */
public class BloomFilterFileIndex implements FileIndexer {

    public static final String BLOOM_FILTER = "bloom";

    private final HashConverter64 hashFunction;

    private final BloomFilter64 filter;

    public BloomFilterFileIndex(DataType type, Options options) {
        int items = options.getInteger("items", 1_000_000);
        double fpp = options.getDouble("fpp", 0.1);
        this.hashFunction = type.accept(FastHash.INSTANCE);
        this.filter = new BloomFilter64(items, fpp);
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
}
