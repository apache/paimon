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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Entry representing a bucket. */
@Public
public class BucketEntry {

    private final BinaryRow partition;
    private final int bucket;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long lastFileCreationTime;

    public BucketEntry(
            BinaryRow partition,
            int bucket,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        this.partition = partition;
        this.bucket = bucket;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    public BinaryRow partition() {
        return partition;
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

    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    public BucketEntry merge(BucketEntry entry) {
        return new BucketEntry(
                partition,
                bucket,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime));
    }

    public static BucketEntry fromManifestEntry(ManifestEntry entry) {
        PartitionEntry partitionEntry = PartitionEntry.fromManifestEntry(entry);
        return new BucketEntry(
                partitionEntry.partition(),
                entry.bucket(),
                partitionEntry.recordCount(),
                partitionEntry.fileSizeInBytes(),
                partitionEntry.fileCount(),
                partitionEntry.lastFileCreationTime());
    }

    public static Collection<BucketEntry> merge(Collection<ManifestEntry> fileEntries) {
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            BucketEntry bucketEntry = fromManifestEntry(entry);
            buckets.compute(
                    Pair.of(entry.partition(), entry.bucket()),
                    (part, old) -> old == null ? bucketEntry : old.merge(bucketEntry));
        }
        return buckets.values();
    }

    public static void merge(
            Collection<BucketEntry> from, Map<Pair<BinaryRow, Integer>, BucketEntry> to) {
        for (BucketEntry entry : from) {
            to.compute(
                    Pair.of(entry.partition(), entry.bucket),
                    (part, old) -> old == null ? entry : old.merge(entry));
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
                && lastFileCreationTime == that.lastFileCreationTime
                && bucket == that.bucket
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition, bucket, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }
}
