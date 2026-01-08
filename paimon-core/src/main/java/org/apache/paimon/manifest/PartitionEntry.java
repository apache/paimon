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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;

/** Entry representing a partition. */
@Public
public class PartitionEntry {

    private final BinaryRow partition;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long lastFileCreationTime;
    private final int totalBuckets;

    public PartitionEntry(
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime,
            int totalBuckets) {
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
        this.totalBuckets = totalBuckets;
    }

    public BinaryRow partition() {
        return partition;
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

    public int totalBuckets() {
        return totalBuckets;
    }

    public PartitionEntry merge(PartitionEntry entry) {
        return new PartitionEntry(
                partition,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime),
                entry.totalBuckets);
    }

    public Partition toPartition(InternalRowPartitionComputer computer) {
        return new Partition(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets,
                false);
    }

    public PartitionStatistics toPartitionStatistics(InternalRowPartitionComputer computer) {
        return new PartitionStatistics(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets);
    }

    public static PartitionEntry fromManifestEntry(ManifestEntry entry) {
        return fromDataFile(entry.partition(), entry.kind(), entry.file(), entry.totalBuckets());
    }

    public static PartitionEntry fromDataFile(
            BinaryRow partition, FileKind kind, DataFileMeta file, int totalBuckets) {
        long recordCount = file.rowCount();
        long fileSizeInBytes = file.fileSize();
        long fileCount = 1;
        if (kind == DELETE) {
            recordCount = -recordCount;
            fileSizeInBytes = -fileSizeInBytes;
            fileCount = -fileCount;
        }
        return new PartitionEntry(
                partition,
                recordCount,
                fileSizeInBytes,
                fileCount,
                file.creationTimeEpochMillis(),
                totalBuckets);
    }

    public static Collection<PartitionEntry> merge(Collection<ManifestEntry> fileEntries) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            PartitionEntry partitionEntry = fromManifestEntry(entry);
            partitions.compute(
                    entry.partition(),
                    (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
        }
        return partitions.values();
    }

    public static Collection<PartitionEntry> mergeSplits(Collection<DataSplit> splits) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (DataSplit split : splits) {
            BinaryRow partition = split.partition();
            for (DataFileMeta file : split.dataFiles()) {
                PartitionEntry partitionEntry =
                        fromDataFile(
                                partition,
                                ADD,
                                file,
                                Optional.ofNullable(split.totalBuckets()).orElse(0));
                partitions.compute(
                        partition,
                        (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
            }

            // Ignore before files, because we don't know how to merge them
            // Ignore deletion files, because it is costly to read from it
        }
        return partitions.values();
    }

    public static void merge(Collection<PartitionEntry> from, Map<BinaryRow, PartitionEntry> to) {
        for (PartitionEntry entry : from) {
            to.compute(entry.partition(), (part, old) -> old == null ? entry : old.merge(entry));
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
        PartitionEntry that = (PartitionEntry) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(partition, that.partition)
                && Objects.equals(totalBuckets, that.totalBuckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets);
    }
}
