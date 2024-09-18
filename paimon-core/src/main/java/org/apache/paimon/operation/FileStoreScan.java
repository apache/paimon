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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;

/** Scan operation which produces a plan. */
public interface FileStoreScan {

    FileStoreScan withPartitionFilter(Predicate predicate);

    FileStoreScan withPartitionFilter(List<BinaryRow> partitions);

    FileStoreScan withPartitionFilter(PartitionPredicate predicate);

    FileStoreScan withBucket(int bucket);

    FileStoreScan withBucketFilter(Filter<Integer> bucketFilter);

    FileStoreScan withPartitionBucket(BinaryRow partition, int bucket);

    FileStoreScan withSnapshot(long snapshotId);

    FileStoreScan withSnapshot(Snapshot snapshot);

    FileStoreScan withManifestList(List<ManifestFileMeta> manifests);

    FileStoreScan withKind(ScanMode scanMode);

    FileStoreScan withLevelFilter(Filter<Integer> levelFilter);

    FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter);

    FileStoreScan withManifestCacheFilter(ManifestCacheFilter manifestFilter);

    FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter);

    FileStoreScan withMetrics(ScanMetrics metrics);

    ManifestPlanner manifestPlanner();

    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    /** Produce a {@link Plan}. */
    Plan plan();

    /**
     * Return record count of all changes occurred in this snapshot given the scan.
     *
     * @return total record count of Snapshot.
     */
    default Long totalRecordCount(Snapshot snapshot) {
        return snapshot.totalRecordCount() == null
                ? (Long) recordCount(withSnapshot(snapshot.id()).plan().files())
                : snapshot.totalRecordCount();
    }

    /**
     * Read {@link SimpleFileEntry}s, SimpleFileEntry only retains some critical information, so it
     * cannot perform filtering based on statistical information.
     */
    List<SimpleFileEntry> readSimpleEntries();

    List<PartitionEntry> readPartitionEntries();

    List<BucketEntry> readBucketEntries();

    default List<BinaryRow> listPartitions() {
        return readPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    /** Result plan of this scan. */
    interface Plan {

        @Nullable
        Long watermark();

        /**
         * Snapshot of this plan, return null if the table is empty or the manifest list is
         * specified.
         */
        @Nullable
        Snapshot snapshot();

        /** Result {@link ManifestEntry} files. */
        List<ManifestEntry> files();

        /** Result {@link ManifestEntry} files with specific file kind. */
        default List<ManifestEntry> files(FileKind kind) {
            return files().stream().filter(e -> e.kind() == kind).collect(Collectors.toList());
        }

        /** Return a map group by partition and bucket. */
        static Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupByPartFiles(
                List<ManifestEntry> files) {
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupBy = new LinkedHashMap<>();
            for (ManifestEntry entry : files) {
                groupBy.computeIfAbsent(entry.partition(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                        .add(entry.file());
            }
            return groupBy;
        }
    }
}
