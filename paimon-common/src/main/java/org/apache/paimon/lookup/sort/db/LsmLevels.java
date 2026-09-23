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

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Thread-safe storage and snapshot publication for the SST levels of a local LSM tree. */
class LsmLevels {

    private final int maxLevels;
    private final List<List<SstFileMetadata>> levels;
    private final ReentrantReadWriteLock lock;

    LsmLevels(int maxLevels) {
        this.maxLevels = maxLevels;
        this.levels = new ArrayList<>(maxLevels);
        for (int i = 0; i < maxLevels; i++) {
            this.levels.add(new ArrayList<>());
        }
        this.lock = new ReentrantReadWriteLock();
    }

    void addLevelZeroFile(SstFileMetadata metadata) {
        lock.writeLock().lock();
        try {
            levels.get(0).add(0, metadata);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void addFiles(int level, List<SstFileMetadata> files) {
        lock.writeLock().lock();
        try {
            levels.get(level).addAll(files);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void runWithWriteLock(Runnable action) {
        lock.writeLock().lock();
        try {
            action.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Lookup a key while holding the read lock so a concurrent compaction cannot delete an SST
     * being read.
     */
    @Nullable
    byte[] lookup(
            byte[] serializedKey,
            MemorySlice key,
            Comparator<MemorySlice> keyComparator,
            FileLookup fileLookup)
            throws IOException {
        lock.readLock().lock();
        try {
            for (int level = 0; level < maxLevels; level++) {
                List<SstFileMetadata> levelFiles = levels.get(level);
                if (levelFiles.isEmpty()) {
                    continue;
                }

                if (level == 0) {
                    for (SstFileMetadata metadata : levelFiles) {
                        if (!metadata.mightContainKey(key, keyComparator)) {
                            continue;
                        }
                        byte[] value = fileLookup.lookup(metadata.getFile(), serializedKey);
                        if (value != null) {
                            return value;
                        }
                    }
                } else {
                    SstFileMetadata target = findFileForKey(levelFiles, key, keyComparator);
                    if (target != null) {
                        byte[] value = fileLookup.lookup(target.getFile(), serializedKey);
                        if (value != null) {
                            return value;
                        }
                    }
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Open a snapshot of all SST files which overlap the requested range.
     *
     * <p>Files are ordered from newer levels to older levels; Level-0 files are additionally
     * ordered newest first. The read lock remains held until the returned snapshot is closed so a
     * concurrent compaction cannot delete a file being iterated.
     */
    RangeSnapshot openRangeSnapshot(
            MemorySlice fromInclusive,
            @Nullable MemorySlice toExclusive,
            Comparator<MemorySlice> keyComparator) {
        lock.readLock().lock();
        boolean success = false;
        try {
            List<File> files = new ArrayList<>();
            for (int level = 0; level < maxLevels; level++) {
                List<SstFileMetadata> levelFiles = levels.get(level);
                if (levelFiles.isEmpty()) {
                    continue;
                }

                if (level == 0) {
                    for (SstFileMetadata metadata : levelFiles) {
                        if (overlapsRange(metadata, fromInclusive, toExclusive, keyComparator)) {
                            files.add(metadata.getFile());
                        }
                    }
                    continue;
                }

                int firstFile = findFirstOverlappingFile(levelFiles, fromInclusive, keyComparator);
                for (int i = firstFile; i < levelFiles.size(); i++) {
                    SstFileMetadata metadata = levelFiles.get(i);
                    if (toExclusive != null
                            && keyComparator.compare(metadata.getMinKey(), toExclusive) >= 0) {
                        break;
                    }
                    files.add(metadata.getFile());
                }
            }
            RangeSnapshot snapshot = new RangeSnapshot(files);
            success = true;
            return snapshot;
        } finally {
            if (!success) {
                lock.readLock().unlock();
            }
        }
    }

    List<List<SstFileMetadata>> snapshot() {
        lock.readLock().lock();
        try {
            return copy(levels);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Atomically replace the compacted snapshot while preserving Level-0 files flushed after the
     * snapshot was taken.
     */
    void publishCompaction(
            List<List<SstFileMetadata>> originalLevels,
            List<List<SstFileMetadata>> compactedLevels,
            List<File> compactedFiles,
            UniversalCompactor.FileDeleter fileDeleter)
            throws IOException {
        Set<File> originalFiles = filesInLevels(originalLevels);

        lock.writeLock().lock();
        try {
            List<SstFileMetadata> newLevelZeroFiles = new ArrayList<>();
            for (SstFileMetadata metadata : levels.get(0)) {
                if (!originalFiles.contains(metadata.getFile())) {
                    newLevelZeroFiles.add(metadata);
                }
            }

            for (int level = 1; level < maxLevels; level++) {
                for (SstFileMetadata metadata : levels.get(level)) {
                    if (!originalFiles.contains(metadata.getFile())) {
                        throw new IOException(
                                "Unexpected concurrent update to level " + level + ".");
                    }
                }
            }

            for (int level = 0; level < maxLevels; level++) {
                levels.get(level).clear();
                if (level == 0) {
                    levels.get(level).addAll(newLevelZeroFiles);
                }
                levels.get(level).addAll(compactedLevels.get(level));
            }

            for (File compactedFile : compactedFiles) {
                fileDeleter.deleteFile(compactedFile);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    boolean needsCompaction(int levelZeroFileCountTrigger) {
        return fileCount(0) >= levelZeroFileCountTrigger;
    }

    int fileCount() {
        lock.readLock().lock();
        try {
            int count = 0;
            for (List<SstFileMetadata> levelFiles : levels) {
                count += levelFiles.size();
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    int fileCount(int level) {
        if (level < 0 || level >= maxLevels) {
            return 0;
        }
        lock.readLock().lock();
        try {
            return levels.get(level).size();
        } finally {
            lock.readLock().unlock();
        }
    }

    String stats() {
        lock.readLock().lock();
        try {
            StringBuilder result = new StringBuilder();
            for (int level = 0; level < maxLevels; level++) {
                int count = levels.get(level).size();
                if (count > 0) {
                    if (result.length() > 0) {
                        result.append(", ");
                    }
                    result.append("L").append(level).append("=").append(count);
                }
            }
            return result.length() == 0 ? "empty" : result.toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    int maxLevels() {
        return maxLevels;
    }

    static List<List<SstFileMetadata>> copy(List<List<SstFileMetadata>> levels) {
        List<List<SstFileMetadata>> copy = new ArrayList<>(levels.size());
        for (List<SstFileMetadata> level : levels) {
            copy.add(new ArrayList<>(level));
        }
        return copy;
    }

    private static Set<File> filesInLevels(List<List<SstFileMetadata>> levels) {
        Set<File> files = new HashSet<>();
        for (List<SstFileMetadata> level : levels) {
            for (SstFileMetadata metadata : level) {
                files.add(metadata.getFile());
            }
        }
        return files;
    }

    @Nullable
    private static SstFileMetadata findFileForKey(
            List<SstFileMetadata> sortedFiles,
            MemorySlice key,
            Comparator<MemorySlice> keyComparator) {
        int low = 0;
        int high = sortedFiles.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            SstFileMetadata midFile = sortedFiles.get(mid);
            if (keyComparator.compare(key, midFile.getMinKey()) < 0) {
                high = mid - 1;
            } else if (keyComparator.compare(key, midFile.getMaxKey()) > 0) {
                low = mid + 1;
            } else {
                return midFile;
            }
        }
        return null;
    }

    private static int findFirstOverlappingFile(
            List<SstFileMetadata> sortedFiles,
            MemorySlice fromInclusive,
            Comparator<MemorySlice> keyComparator) {
        int low = 0;
        int high = sortedFiles.size();
        while (low < high) {
            int mid = low + (high - low) / 2;
            if (keyComparator.compare(sortedFiles.get(mid).getMaxKey(), fromInclusive) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private static boolean overlapsRange(
            SstFileMetadata metadata,
            MemorySlice fromInclusive,
            @Nullable MemorySlice toExclusive,
            Comparator<MemorySlice> keyComparator) {
        return keyComparator.compare(metadata.getMaxKey(), fromInclusive) >= 0
                && (toExclusive == null
                        || keyComparator.compare(metadata.getMinKey(), toExclusive) < 0);
    }

    /** SST files in a range protected from concurrent compaction until closed. */
    final class RangeSnapshot implements AutoCloseable {

        private final List<File> files;
        private boolean closed;

        private RangeSnapshot(List<File> files) {
            this.files = files;
        }

        List<File> files() {
            return files;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                lock.readLock().unlock();
            }
        }
    }

    /** Callback for reading one SST file. */
    interface FileLookup {

        @Nullable
        byte[] lookup(File file, byte[] key) throws IOException;
    }
}
