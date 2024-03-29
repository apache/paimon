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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.paimon.fileindex.FastHashForNumber;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.ObjectToBytesVisitor;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;

/** Bloom filter for secondary index. */
public class BloomFilter implements FileIndexer {

    public static final String BLOOM_FILTER = "bloom";

    private static final int HASH_NUMBER = 4;

    private final HadoopDynamicBloomFilter filter =
            new HadoopDynamicBloomFilter(50 * 8, HASH_NUMBER, Hash.MURMUR_HASH, 30);
    // reuse
    private final Key filterKey = new Key();

    private final Function<Object, byte[]> converter;

    private final Optional<Function<Object, Long>> hashFunction;


    public BloomFilter(DataType type) {
        this.converter = type.accept(ObjectToBytesVisitor.INSTANCE);
        hashFunction = type.accept(FastHashForNumber.INSTANCE);
    }

    public String name() {
        return BLOOM_FILTER;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer();
    }

    @Override
    public FileIndexReader createReader() {
        return new Reader();
    }

    private class Writer implements FileIndexWriter {


        private Writer() {
        }

        @Override
        public void write(Object key) {
            if (hashFunction.isPresent()) {
                filter.addHash(hashFunction.get().apply(key));
            } else {
                filterKey.set(converter.apply(key), 1.0);
                filter.add(filterKey);
            }
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

        @Override
        public Reader recoverFrom(byte[] serializedBytes) {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedBytes));

            try {
                filter.readFields(dis);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        @Override
        public Boolean visitEqual(FieldRef fieldRef, Object key) {
            if (hashFunction.isPresent()) {
                return filter.membershipTest(hashFunction.get().apply(key));
            } else {
                filterKey.set(converter.apply(key), 1.0);
                return filter.membershipTest(filterKey);
            }
        }
    }
}
