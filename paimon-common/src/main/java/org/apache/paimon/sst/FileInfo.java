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

package org.apache.paimon.sst;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.SortUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** Some key file information of an SST File. */
public class FileInfo {
    public static final int FIXED_SIZE = 4 * 6;

    private static final Comparator<byte[]> COMPARATOR = SortUtil::compareBinary;

    private static final String RESERVED_PREFIX = "sst.";
    private static final byte[] RESERVED_PREFIX_BYTES = RESERVED_PREFIX.getBytes();
    private static final String AVG_KEY_LEN = "avg_key_len";
    private static final String AVG_VALUE_LEN = "avg_value_len";
    private static final String MAX_KEY_LEN = "max_key_len";
    private static final String MAX_VALUE_LEN = "max_value_len";
    private static final String MIN_KEY_LEN = "min_key_len";
    private static final String MIN_VALUE_LEN = "min_value_len";

    private final int avgKeyLength;
    private final int avgValueLength;
    private final int minKeyLength;
    private final int maxKeyLength;
    private final int minValueLength;
    private final int maxValueLength;
    private final Map<byte[], byte[]> extras;

    private FileInfo(
            int avgKeyLength,
            int avgValueLength,
            int minKeyLength,
            int maxKeyLength,
            int minValueLength,
            int maxValueLength,
            Map<byte[], byte[]> extras) {
        this.avgKeyLength = avgKeyLength;
        this.avgValueLength = avgValueLength;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;
        this.minValueLength = minValueLength;
        this.maxValueLength = maxValueLength;
        this.extras = extras;
    }

    public int getAvgKeyLength() {
        return avgKeyLength;
    }

    public int getAvgValueLength() {
        return avgValueLength;
    }

    public int getMaxKeyLength() {
        return maxKeyLength;
    }

    public int getMaxValueLength() {
        return maxValueLength;
    }

    public int getMinKeyLength() {
        return minKeyLength;
    }

    public int getMinValueLength() {
        return minValueLength;
    }

    public Map<byte[], byte[]> getExtras() {
        return extras;
    }

    public int memory() {
        int result = FIXED_SIZE;
        for (Map.Entry<byte[], byte[]> entry : extras.entrySet()) {
            result += entry.getKey().length + entry.getValue().length;
        }
        return result;
    }

    public static void writeFileInfo(FileInfo fileInfo, BlockWriter blockWriter) {
        MemorySliceOutput output = new MemorySliceOutput(4);

        output.reset();
        output.writeInt(fileInfo.avgKeyLength);
        blockWriter.add((RESERVED_PREFIX + AVG_KEY_LEN).getBytes(), output.toSlice().copyBytes());

        output.reset();
        output.writeInt(fileInfo.avgValueLength);
        blockWriter.add((RESERVED_PREFIX + AVG_VALUE_LEN).getBytes(), output.toSlice().copyBytes());

        output.reset();
        output.writeInt(fileInfo.minKeyLength);
        blockWriter.add((RESERVED_PREFIX + MIN_KEY_LEN).getBytes(), output.toSlice().copyBytes());

        output.reset();
        output.writeInt(fileInfo.maxKeyLength);
        blockWriter.add((RESERVED_PREFIX + MAX_KEY_LEN).getBytes(), output.toSlice().copyBytes());

        output.reset();
        output.writeInt(fileInfo.minValueLength);
        blockWriter.add((RESERVED_PREFIX + MIN_VALUE_LEN).getBytes(), output.toSlice().copyBytes());

        output.reset();
        output.writeInt(fileInfo.maxValueLength);
        blockWriter.add((RESERVED_PREFIX + MAX_VALUE_LEN).getBytes(), output.toSlice().copyBytes());

        for (Map.Entry<byte[], byte[]> entry : fileInfo.extras.entrySet()) {
            blockWriter.add(entry.getKey(), entry.getValue());
        }
    }

    public static FileInfo readFileInfo(BlockReader blockReader) throws IOException {
        Iterator<BlockEntry> iterator =
                BlockIterator.SequentialBlockIterator.wrap(blockReader.iterator());
        int avgKeyLength = 0;
        int avgValueLength = 0;
        int minKeyLength = 0;
        int maxKeyLength = 0;
        int minValueLength = 0;
        int maxValueLength = 0;
        Map<byte[], byte[]> extras = new TreeMap<>(COMPARATOR);

        byte[] key, value;
        while (iterator.hasNext()) {
            BlockEntry entry = iterator.next();
            key = entry.getKey().copyBytes();
            value = entry.getValue().copyBytes();

            if (isReserved(key)) {
                MemorySlice valueSlice = MemorySlice.wrap(value);
                if (COMPARATOR.compare(key, (RESERVED_PREFIX + AVG_KEY_LEN).getBytes()) == 0) {
                    avgKeyLength = valueSlice.readInt(0);
                } else if (COMPARATOR.compare(key, (RESERVED_PREFIX + AVG_VALUE_LEN).getBytes())
                        == 0) {
                    avgValueLength = valueSlice.readInt(0);
                } else if (COMPARATOR.compare(key, (RESERVED_PREFIX + MIN_KEY_LEN).getBytes())
                        == 0) {
                    minKeyLength = valueSlice.readInt(0);
                } else if (COMPARATOR.compare(key, (RESERVED_PREFIX + MAX_KEY_LEN).getBytes())
                        == 0) {
                    maxKeyLength = valueSlice.readInt(0);
                } else if (COMPARATOR.compare(key, (RESERVED_PREFIX + MIN_VALUE_LEN).getBytes())
                        == 0) {
                    minValueLength = valueSlice.readInt(0);
                } else if (COMPARATOR.compare(key, (RESERVED_PREFIX + MAX_VALUE_LEN).getBytes())
                        == 0) {
                    maxValueLength = valueSlice.readInt(0);
                } else {
                    throw new IOException("Unrecognized key: " + new String(key));
                }
            } else {
                extras.put(key, value);
            }
        }

        return new FileInfo(
                avgKeyLength,
                avgValueLength,
                minKeyLength,
                maxKeyLength,
                minValueLength,
                maxValueLength,
                extras);
    }

    private static boolean isReserved(byte[] key) {
        int prefixLen = RESERVED_PREFIX_BYTES.length;
        return key.length >= prefixLen
                && SortUtil.compareBinary(key, 0, prefixLen, RESERVED_PREFIX_BYTES, 0, prefixLen)
                        == 0;
    }

    @Override
    public String toString() {
        return "FileInfo{"
                + "avgKeyLength="
                + avgKeyLength
                + ", avgValueLength="
                + avgValueLength
                + ", minKeyLength="
                + minKeyLength
                + ", maxKeyLength="
                + maxKeyLength
                + ", minValueLength="
                + minValueLength
                + ", maxValueLength="
                + maxValueLength
                + ", extras="
                + extras
                + '}';
    }

    /** Builder for FileInfo. */
    public static class Builder {
        private double totalKeyLength = 0;
        private double totalValueLength = 0;
        private int minKeyLength = Integer.MAX_VALUE;
        private int maxKeyLength = -1;
        private int minValueLength = Integer.MAX_VALUE;
        private int maxValueLength = -1;
        private int count = 0;
        private final Map<byte[], byte[]> extras = new TreeMap<>(COMPARATOR);

        public Builder() {}

        public void update(byte[] key, byte[] value) {
            totalKeyLength += key.length;
            totalValueLength += value.length;
            count++;
            minKeyLength = Math.min(minKeyLength, key.length);
            maxKeyLength = Math.max(maxKeyLength, key.length);
            minValueLength = Math.min(minValueLength, value.length);
            maxValueLength = Math.max(maxValueLength, value.length);
        }

        public void addExtraValue(byte[] key, byte[] value) throws IOException {
            if (isReserved(key)) {
                throw new IOException("Invalid key prefix: " + new String(key));
            }
            extras.put(key, value);
        }

        public FileInfo build() {
            return count > 0
                    ? new FileInfo(
                            (int) (totalKeyLength / count),
                            (int) (totalValueLength / count),
                            minKeyLength,
                            maxKeyLength,
                            minValueLength,
                            maxValueLength,
                            extras)
                    : new FileInfo(0, 0, 0, 0, 0, 0, extras);
        }
    }
}
