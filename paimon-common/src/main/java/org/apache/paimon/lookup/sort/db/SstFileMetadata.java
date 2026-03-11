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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.memory.MemorySlice;

import java.io.File;
import java.util.Comparator;

/**
 * Metadata for an SST file, tracking its key range, size and level for efficient compaction and
 * lookup.
 */
public final class SstFileMetadata {

    private final File file;
    private final MemorySlice minKey;
    private final MemorySlice maxKey;
    private final long fileSize;
    private final long tombstoneCount;

    /** The level this file belongs to in the LSM tree. Mutable because compaction reassigns it. */
    private int level;

    public SstFileMetadata(
            File file, MemorySlice minKey, MemorySlice maxKey, long tombstoneCount, int level) {
        this(file, minKey, maxKey, file.length(), tombstoneCount, level);
    }

    public SstFileMetadata(
            File file,
            MemorySlice minKey,
            MemorySlice maxKey,
            long fileSize,
            long tombstoneCount,
            int level) {
        this.file = file;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.fileSize = fileSize;
        this.tombstoneCount = tombstoneCount;
        this.level = level;
    }

    public File getFile() {
        return file;
    }

    public MemorySlice getMinKey() {
        return minKey;
    }

    public MemorySlice getMaxKey() {
        return maxKey;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public boolean hasTombstones() {
        return tombstoneCount > 0;
    }

    public boolean mightContainKey(MemorySlice key, Comparator<MemorySlice> comparator) {
        return comparator.compare(key, minKey) >= 0 && comparator.compare(key, maxKey) <= 0;
    }
}
