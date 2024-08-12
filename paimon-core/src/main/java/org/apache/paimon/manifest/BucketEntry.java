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

package org.apache.paimon.manifest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.manifest.FileKind.DELETE;

/** Entry representing a bucket. */
public class BucketEntry {

    private final int bucket;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long level0FileCount;
    private final int triggerNum;
    private final long lastFileCreationTime;

    public BucketEntry(
            int bucket,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long level0FileCount,
            int triggerNum,
            long lastFileCreationTime) {
        this.bucket = bucket;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.level0FileCount = level0FileCount;
        this.triggerNum = triggerNum;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    public int bucket() {
        return bucket;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public long fileCount() {
        return fileCount;
    }

    public long triggerNum() {
        return triggerNum;
    }

    public long level0FileCount() {
        return level0FileCount;
    }

    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    public BucketEntry merge(BucketEntry entry) {
        return new BucketEntry(
                bucket,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                level0FileCount + entry.level0FileCount,
                triggerNum,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime));
    }

    public static BucketEntry fromManifestEntry(ManifestEntry entry, int triggerNum) {
        long recordCount = entry.file().rowCount();
        long fileSizeInBytes = entry.file().fileSize();
        long fileCount = 1;
        long level0FileCount = 0;
        if (entry.level() == 0) {
            level0FileCount = 1;
        }
        if (entry.kind() == DELETE) {
            recordCount = -recordCount;
            fileSizeInBytes = -fileSizeInBytes;
            fileCount = -fileCount;
            level0FileCount = -level0FileCount;
        }
        return new BucketEntry(
                entry.bucket(),
                recordCount,
                fileSizeInBytes,
                fileCount,
                level0FileCount,
                triggerNum,
                entry.file().creationTimeEpochMillis());
    }

    public static Collection<BucketEntry> merge(
            Collection<ManifestEntry> fileEntries, int triggerNum) {
        Map<Integer, BucketEntry> bucktes = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            BucketEntry bucketEntry = fromManifestEntry(entry, triggerNum);
            bucktes.compute(
                    entry.bucket(),
                    (part, old) -> old == null ? bucketEntry : old.merge(bucketEntry));
        }
        return bucktes.values();
    }

    public static void merge(Collection<BucketEntry> from, Map<Integer, BucketEntry> to) {
        for (BucketEntry entry : from) {
            to.compute(entry.bucket(), (part, old) -> old == null ? entry : old.merge(entry));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketEntry that = (BucketEntry) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && level0FileCount == that.level0FileCount
                && triggerNum == that.triggerNum
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(bucket, that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bucket,
                recordCount,
                fileSizeInBytes,
                fileCount,
                level0FileCount,
                triggerNum,
                lastFileCreationTime);
    }
}
