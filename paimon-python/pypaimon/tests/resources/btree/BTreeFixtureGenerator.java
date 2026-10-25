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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.LongArrayList;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
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
        String[] stringKeys = {"ke", "keep", "keeper", "key", "kf"};
        for (int version = 1; version <= 2; version++) {
            for (boolean bloom : new boolean[] {false, true}) {
                for (boolean string : new boolean[] {false, true}) {
                    final String name =
                            "java-v"
                                    + version
                                    + (string ? "-string" : "")
                                    + (bloom ? "-bloom" : "")
                                    + ".btree";
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
                                    string
                                                ? KeySerializer.create(VarCharType.STRING_TYPE)
                                            : KeySerializer.create(new IntType()),
                                    32,
                                    bloom ? BloomFilter.fixedBuilder(rows.length, 0.01) : null,
                                    null,
                                    version);
                    for (int i = 0; i < rows.length; i++) {
                        for (long row : rows[i]) {
                            writer.write(
                                    string ? BinaryString.fromString(stringKeys[i]) : i + 1, row);
                        }
                    }
                    writer.write(null, 999);
                    ResultEntry entry = writer.finish().get(0);
                    Files.write(directory.resolve(name), bytes.toByteArray());
                    Files.write(directory.resolve(name + ".meta"), entry.meta());
                }
            }
        }
        writeComposite(directory, "java-v2-composite-int.btree", false);
        writeComposite(directory, "java-v2-composite-ac.btree", false);
        writeComposite(directory, "java-v2-composite-reversed.btree", true);
        writeCompositeKeys(directory);
    }

    private static void writeComposite(Path directory, String name, boolean reversed)
            throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        GlobalIndexFileWriter files =
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
                        files,
                        new CompositeKeySerializer(RowType.of(DataTypes.INT(), DataTypes.INT())),
                        32,
                        null,
                        null,
                        2);
        Integer[][] keys = {
            {null, null}, {null, 2}, {1, null}, {1, 1}, {1, 2}, {1, 2}, {2, 1}
        };
        int[] order = reversed ? new int[] {0, 2, 3, 6, 1, 4, 5} : new int[] {0, 1, 2, 3, 4, 5, 6};
        for (int rowId : order) {
            Integer[] key = keys[rowId];
            writer.write(GenericRow.of(
                    reversed ? key[1] : key[0], reversed ? key[0] : key[1]), rowId);
        }
        ResultEntry entry = writer.finish().get(0);
        Files.write(directory.resolve(name), bytes.toByteArray());
        Files.write(directory.resolve(name + ".meta"), entry.meta());
    }

    private static void writeCompositeKeys(Path directory) throws Exception {
        KeySerializer serializer =
                new CompositeKeySerializer(
                        RowType.of(
                                DataTypes.STRING(), DataTypes.DECIMAL(38, 18),
                                DataTypes.TIMESTAMP(9)));
        GenericRow[] rows = {
            GenericRow.of(null, null, null),
            GenericRow.of(
                    BinaryString.fromString("a"),
                    Decimal.fromBigDecimal(new BigDecimal("1.230000000000000000"), 38, 18),
                    Timestamp.fromEpochMillis(0, 1000)),
            GenericRow.of(
                    BinaryString.fromString(new String(new char[130]).replace('\0', 'x')),
                    Decimal.fromBigDecimal(
                            new BigDecimal("-99999999999999999999.999999999999999999"),
                            38, 18),
                    Timestamp.fromEpochMillis(-1, 999999)),
        };
        StringBuilder result =
                new StringBuilder(
                        "# Licensed to the Apache Software Foundation (ASF) under one\n"
                                + "# or more contributor license agreements.  See the NOTICE file\n"
                                + "# distributed with this work for additional information\n"
                                + "# regarding copyright ownership.  The ASF licenses this file\n"
                                + "# to you under the Apache License, Version 2.0 (the\n"
                                + "# \"License\"); you may not use this file except in compliance\n"
                                + "# with the License.  You may obtain a copy of the License at\n"
                                + "#\n"
                                + "#   http://www.apache.org/licenses/LICENSE-2.0\n"
                                + "#\n"
                                + "# Unless required by applicable law or agreed to in writing, software\n"
                                + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                                + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                                + "# See the License for the specific language governing permissions and\n"
                                + "# limitations under the License.\n");
        for (GenericRow row : rows) {
            for (byte value : serializer.serialize(row)) {
                result.append(String.format("%02x", value & 0xff));
            }
            result.append('\n');
        }
        Files.write(
                directory.resolve("java-v2-composite-keys.txt"),
                result.toString().getBytes(StandardCharsets.UTF_8));
    }
}
