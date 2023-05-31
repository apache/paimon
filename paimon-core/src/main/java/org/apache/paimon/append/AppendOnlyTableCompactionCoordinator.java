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

package org.apache.paimon.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link AppendOnlyFileStoreTable} compact coordinator. */
public class AppendOnlyTableCompactionCoordinator {
    private final SnapshotManager snapshotManager;
    private final AppendOnlyFileStoreScan scan;

    private Long snapshotId;
    private final long targetFileSize;
    private final int minFileNum;
    private final int maxFileNum;

    private final Map<BinaryRow, PartitionCompactCoordinator> partitionCompactCoordinators =
            new HashMap<>();

    public AppendOnlyTableCompactionCoordinator(
            AppendOnlyFileStoreTable table, long targetFileSize, int minFileNum, int maxFileNum) {
        this.scan = table.store().newScan();
        this.snapshotManager = table.snapshotManager();
        this.targetFileSize = targetFileSize;
        this.minFileNum = minFileNum;
        this.maxFileNum = maxFileNum;
        this.snapshotId = snapshotManager.latestSnapshotId();

        if (snapshotId != null) {
            updateFull();
        }
    }

    public void updateRestored() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null || latestSnapshotId.equals(snapshotId)) {
            return;
        }

        if (snapshotId == null) {
            snapshotId = latestSnapshotId;
            updateFull();
        } else {
            updateDelta(latestSnapshotId);
            snapshotId = latestSnapshotId;
        }
    }

    private void updateFull() {
        scan.withSnapshot(snapshotId).plan().files().stream()
                .collect(Collectors.groupingBy(ManifestEntry::partition))
                .forEach(
                        (k, v) ->
                                partitionCompactCoordinators
                                        .computeIfAbsent(k, PartitionCompactCoordinator::new)
                                        .addFiles(
                                                v.stream()
                                                        .map(ManifestEntry::file)
                                                        .filter(
                                                                dataFileMeta ->
                                                                        dataFileMeta.fileSize()
                                                                                < targetFileSize)
                                                        .collect(Collectors.toList())));
    }

    public void updateDelta(Long targetSnapshotId) {
        for (long id = snapshotId + 1; id <= targetSnapshotId; id++) {
            scan.withSnapshot(id).withKind(ScanKind.DELTA).plan().files().stream()
                    .collect(Collectors.groupingBy(ManifestEntry::partition))
                    .forEach(
                            (k, v) ->
                                    partitionCompactCoordinators
                                            .computeIfAbsent(k, PartitionCompactCoordinator::new)
                                            .updateRestoredFiles(v));
        }
    }

    // generate compaction task to the next stage
    public List<AppendOnlyCompactionTask> compactPlan() {
        return partitionCompactCoordinators.values().stream()
                .filter(t -> t.toCompact.size() > 1)
                .flatMap(t -> t.plan().stream())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public HashSet<DataFileMeta> listRestoredFiles() {
        HashSet<DataFileMeta> sets = new HashSet<>();
        partitionCompactCoordinators
                .values()
                .forEach(
                        partitionCompactCoordinator ->
                                sets.addAll(partitionCompactCoordinator.getToCompact()));
        return sets;
    }

    /** Coordinator for a single partition. */
    private class PartitionCompactCoordinator {
        BinaryRow partition;
        HashSet<DataFileMeta> toCompact = new HashSet<>();

        public PartitionCompactCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        public List<AppendOnlyCompactionTask> plan() {
            return pickCompact();
        }

        public HashSet<DataFileMeta> getToCompact() {
            return toCompact;
        }

        private List<AppendOnlyCompactionTask> pickCompact() {
            List<List<DataFileMeta>> waitCompact = pack();
            return waitCompact.stream()
                    .map(files -> new AppendOnlyCompactionTask(partition, files))
                    .collect(Collectors.toList());
        }

        public void addFiles(List<DataFileMeta> dataFileMetas) {
            toCompact.addAll(dataFileMetas);
        }

        public void updateRestoredFiles(List<ManifestEntry> entries) {
            entries.forEach(
                    entry -> {
                        if (entry.kind() == FileKind.ADD) {
                            if (entry.file().fileSize() < targetFileSize) {
                                toCompact.add(entry.file());
                            }
                        } else if (entry.kind() == FileKind.DELETE) {
                            toCompact.remove(entry.file());
                        } else {
                            throw new RuntimeException("unknown file kind: " + entry.kind());
                        }
                    });
        }

        private List<List<DataFileMeta>> pack() {
            // we compact smaller files first
            ArrayList<DataFileMeta> files = new ArrayList<>(toCompact);
            files.sort(Comparator.comparingLong(DataFileMeta::fileSize));

            List<List<DataFileMeta>> result = new ArrayList<>();

            List<DataFileMeta> current = new ArrayList<>();
            long totalFileSize = 0L;
            int fileNum = 0;
            for (DataFileMeta fileMeta : files) {
                totalFileSize += fileMeta.fileSize();
                fileNum++;
                current.add(fileMeta);
                if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum) {
                    result.add(new ArrayList<>(current));
                    // remove it from coordinator memory, won't join in compaction again
                    current.forEach(toCompact::remove);
                    current.clear();
                    totalFileSize = 0;
                    fileNum = 0;
                }
            }

            return result;
        }
    }
}
