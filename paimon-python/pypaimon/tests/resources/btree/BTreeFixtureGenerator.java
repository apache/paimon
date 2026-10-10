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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.LongArrayList;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/** Generates Python interoperability fixtures using the actual Java writer. */
public final class BTreeFixtureGenerator {

    public static void main(String[] args) throws Exception {
        Path directory = Paths.get(args[0]);
        Files.createDirectories(directory);
        long[][] rows = new long[5][];
        rows[0] = new long[] {10};
        rows[1] = new long[] {20, 40, (1L << 32) + 1};
        rows[2] = new long[128];
        rows[3] = new long[128];
        rows[4] = new long[128];
        for (int i = 0; i < 128; i++) {
            rows[2][i] = 1000 + i;
            rows[3][i] = (1L << 33) + i;
            rows[4][i] = Long.MAX_VALUE - 127 + i;
        }
        int[] expectedTypes = {0, 1, 2, 2, 2};
        StringBuilder expected = new StringBuilder("{\n");
        for (int i = 0; i < rows.length; i++) {
            LongArrayList list = new LongArrayList(rows[i].length);
            for (long row : rows[i]) {
                list.add(row);
            }
            byte[] encoded = BTreePostingList.serialize(list);
            if ((encoded[0] & 255) != expectedTypes[i]) {
                throw new AssertionError("Unexpected posting encoding");
            }
            long[] decoded =
                    BTreePostingList.deserialize(MemorySlice.wrap(encoded), Integer.MAX_VALUE);
            if (!Arrays.equals(decoded, rows[i])) {
                throw new AssertionError("Java posting mismatch");
            }
            Files.write(directory.resolve("posting-" + (i + 1) + ".bin"), encoded);
            expected.append('"').append(i + 1).append("\": ").append(Arrays.toString(rows[i]));
            expected.append(i + 1 == rows.length ? "\n}\n" : ",\n");
        }
        Files.write(
                directory.resolve("expected.json"),
                expected.toString().getBytes(StandardCharsets.UTF_8));
        LongArrayList crossing = new LongArrayList(128);
        for (int i = 0; i < 128; i++) {
            crossing.add((1L << 32) - 64 + i);
        }
        byte[] crossingPosting = BTreePostingList.serialize(crossing);
        if ((crossingPosting[0] & 255) != BTreePostingList.ROARING) {
            throw new AssertionError("Expected Roaring across high-word buckets");
        }
        Files.write(directory.resolve("posting-high-buckets.bin"), crossingPosting);
        for (int version = 1; version <= 2; version++) {
            for (boolean bloom : new boolean[] {false, true}) {
                final String name = "java-v" + version + (bloom ? "-bloom" : "") + ".btree";
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                GlobalIndexFileWriter fileWriter =
                        new GlobalIndexFileWriter() {
                            @Override
                            public String newFileName(String prefix) {
                                return name;
                            }

                            @Override
                            public PositionOutputStream newOutputStream(String fileName) {
                                return new PositionOutputStream() {
                                    @Override
                                    public long getPos() {
                                        return bytes.size();
                                    }

                                    @Override
                                    public void write(int value) {
                                        bytes.write(value);
                                    }

                                    @Override
                                    public void write(byte[] value) {
                                        bytes.write(value, 0, value.length);
                                    }

                                    @Override
                                    public void write(byte[] value, int offset, int length) {
                                        bytes.write(value, offset, length);
                                    }

                                    @Override
                                    public void flush() {}

                                    @Override
                                    public void close() {}
                                };
                            }
                        };
                BTreeIndexWriter writer =
                        new BTreeIndexWriter(
                                fileWriter,
                                KeySerializer.create(new IntType()),
                                32,
                                bloom ? BloomFilter.fixedBuilder(rows.length, 0.01) : null,
                                null,
                                version);
                for (int i = 0; i < rows.length; i++) {
                    for (long row : rows[i]) {
                        writer.write(i + 1, row);
                    }
                }
                writer.write(null, 999);
                ResultEntry entry = writer.finish().get(0);
                Files.write(directory.resolve(name), bytes.toByteArray());
                Files.write(directory.resolve(name + ".meta"), entry.meta());
            }
        }
    }
}
