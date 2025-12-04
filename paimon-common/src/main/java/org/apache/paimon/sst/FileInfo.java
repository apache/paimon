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
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import java.util.Arrays;

/** Contains some important information of an SST file. */
public class FileInfo {
    private static final int FIXED_PART_LENGTH = 16;
    private byte[] firstKey = null;
    private int avgKeyLength = 0;
    private int avgValueLength = 0;
    private int maxKeyLength = -1;
    private int maxValueLength = -1;

    public FileInfo(
            byte[] firstKey,
            int avgKeyLength,
            int avgValueLength,
            int maxKeyLength,
            int maxValueLength) {
        this.firstKey = firstKey;
        this.avgKeyLength = avgKeyLength;
        this.avgValueLength = avgValueLength;
        this.maxKeyLength = maxKeyLength;
        this.maxValueLength = maxValueLength;
    }

    public int getMaxKeyLength() {
        return maxKeyLength;
    }

    public int getMaxValueLength() {
        return maxValueLength;
    }

    public long getAvgKeyLength() {
        return avgKeyLength;
    }

    public long getAvgValueLength() {
        return avgValueLength;
    }

    public byte[] getFirstKey() {
        return firstKey;
    }

    public int memorySize() {
        return FIXED_PART_LENGTH + (firstKey == null ? 0 : firstKey.length);
    }

    public static FileInfo readFileInfo(MemorySliceInput sliceInput) {
        byte[] firstKey = null;
        int firstKeyLength = sliceInput.readInt();
        if (firstKeyLength > 0) {
            firstKey = sliceInput.readSlice(firstKeyLength).copyBytes();
        }
        return new FileInfo(
                firstKey,
                sliceInput.readInt(),
                sliceInput.readInt(),
                sliceInput.readInt(),
                sliceInput.readInt());
    }

    public static MemorySlice writeFileInfo(FileInfo fileInfo) {
        MemorySliceOutput memorySlice = new MemorySliceOutput(fileInfo.memorySize());
        writeFileInfo(fileInfo, memorySlice);
        return memorySlice.toSlice();
    }

    public static void writeFileInfo(FileInfo fileInfo, MemorySliceOutput sliceOutput) {
        // 1. write first key
        if (fileInfo.firstKey != null) {
            sliceOutput.writeInt(fileInfo.firstKey.length);
            sliceOutput.writeBytes(fileInfo.firstKey);
        } else {
            sliceOutput.writeInt(0);
        }
        // 2. write other statistics
        sliceOutput.writeInt(fileInfo.avgKeyLength);
        sliceOutput.writeInt(fileInfo.avgValueLength);
        sliceOutput.writeInt(fileInfo.maxKeyLength);
        sliceOutput.writeInt(fileInfo.maxValueLength);
    }

    @Override
    public String toString() {
        return "FileInfo{"
                + "firstKey="
                + Arrays.toString(firstKey)
                + ", avgKeyLength="
                + avgKeyLength
                + ", avgValueLength="
                + avgValueLength
                + ", maxKeyLength="
                + maxKeyLength
                + ", maxValueLength="
                + maxValueLength
                + '}';
    }

    /** Builder for file info. */
    public static class Builder {
        private byte[] firstKey = null;
        private long avgKeyLength = 0;
        private long avgValueLength = 0;
        private int maxKeyLength = -1;
        private int maxValueLength = -1;
        private int rowCount = 0;

        public Builder() {}

        public void update(byte[] key, byte[] value) {
            if (firstKey == null) {
                firstKey = Arrays.copyOf(key, key.length);
            }
            avgValueLength += value.length;
            avgKeyLength += key.length;
            maxKeyLength = Math.max(maxKeyLength, key.length);
            maxValueLength = Math.max(maxValueLength, value.length);
            rowCount++;
        }

        public FileInfo build() {
            return new FileInfo(
                    firstKey,
                    rowCount == 0 ? 0 : (int) (avgKeyLength / rowCount),
                    rowCount == 0 ? 0 : (int) (avgValueLength / rowCount),
                    maxKeyLength,
                    maxValueLength);
        }
    }
}
