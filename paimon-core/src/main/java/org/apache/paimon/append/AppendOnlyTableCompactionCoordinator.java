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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
        scan = table.store().newScan();
        snapshotManager = table.snapshotManager();
        this.targetFileSize = targetFileSize;
        this.minFileNum = minFileNum;
        this.maxFileNum = maxFileNum;
        snapshotId = snapshotManager.latestSnapshotId();

        if (snapshotId != null) {
            updateFull();
        }
    }

    public void updateRestore() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            return;
        }

        if (snapshotId == null) {
            updateFull();
        } else {
            updateDelta(latestSnapshotId);
        }
        snapshotId = latestSnapshotId;
    }

    private void updateFull() {
        scan.withSnapshot(snapshotId).plan().files().stream()
                .collect(Collectors.groupingBy(ManifestEntry::partition))
                .forEach(
                        (k, v) ->
                                partitionCompactCoordinators
                                        .computeIfAbsent(k, PartitionCompactCoordinator::new)
                                        .addRestoredFiles(
                                                v.stream()
                                                        .map(ManifestEntry::file)
                                                        .filter(
                                                                dataFileMeta ->
                                                                        dataFileMeta.fileSize()
                                                                                < targetFileSize)
                                                        .collect(Collectors.toList())));
    }

    private void updateDelta(Long targetSnapshotId) {
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

    public void addAll(List<CommitMessage> commitMessages) {
        commitMessages.forEach(
                commitMessage ->
                        partitionCompactCoordinators
                                .computeIfAbsent(
                                        commitMessage.partition(), PartitionCompactCoordinator::new)
                                .addIncremental(
                                        ((CommitMessageImpl) commitMessage)
                                                .newFilesIncrement()
                                                .newFiles()));
    }

    public void add(CommitMessage commitMessage) {
        partitionCompactCoordinators
                .computeIfAbsent(commitMessage.partition(), PartitionCompactCoordinator::new)
                .addIncremental(((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
    }

    public List<CompactionTask> compactPlan() {
        return partitionCompactCoordinators.values().stream()
                .filter(t -> t.newFiles.size() > 0)
                .flatMap(t -> t.plan().stream())
                .collect(Collectors.toList());
    }

    /** Coordinator for a single partition. */
    private class PartitionCompactCoordinator {
        BinaryRow partition;
        HashSet<DataFileMeta> restoredFiles = new HashSet<>();
        HashSet<DataFileMeta> newFiles = new HashSet<>();

        public PartitionCompactCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        public List<CompactionTask> plan() {
            List<CompactionTask> tasks = pickCompact();
            newFiles.clear();
            return tasks;
        }

        private List<CompactionTask> pickCompact() {
            Queue<DataFileMeta> toCompactNew = new ArrayDeque<>(newFiles);
            Queue<DataFileMeta> toCompactRestored = new ArrayDeque<>(restoredFiles);
            List<CompactionTask> tasks = new ArrayList<>();

            List<DataFileMeta> candidateNewFiles = new ArrayList<>();
            List<DataFileMeta> candidateRestoredFiles = new ArrayList<>();
            List<DataFileMeta> remainedNewFiles = new ArrayList<>();

            long totalFileSize = 0L;
            int fileNum = 0;
            while (!toCompactNew.isEmpty()) {
                DataFileMeta file = toCompactNew.poll();
                long fileSize = file.fileSize();
                if (fileSize > targetFileSize) {
                    remainedNewFiles.add(file);
                    continue;
                }

                candidateNewFiles.add(file);
                totalFileSize += fileSize;
                fileNum++;

                if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum) {
                    tasks.add(
                            new CompactionTask(
                                    partition,
                                    new ArrayList<>(candidateNewFiles),
                                    Collections.EMPTY_LIST,
                                    true));
                    candidateNewFiles.clear();
                    totalFileSize = 0;
                    fileNum = 0;
                }
            }

            while (!toCompactRestored.isEmpty()) {
                DataFileMeta file = toCompactRestored.poll();
                long fileSize = file.fileSize();
                if (fileSize > targetFileSize) {
                    // big file will not participate in compaction again
                    restoredFiles.remove(file);
                    continue;
                }
                candidateRestoredFiles.add(file);
                totalFileSize += fileSize;
                fileNum++;

                if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum) {
                    if (candidateNewFiles.size() > 0) {
                        tasks.add(
                                new CompactionTask(
                                        partition,
                                        new ArrayList<>(candidateNewFiles),
                                        candidateRestoredFiles,
                                        true));
                        candidateNewFiles.clear();
                    } else {
                        tasks.add(
                                new CompactionTask(
                                        partition,
                                        Collections.EMPTY_LIST,
                                        new ArrayList<>(candidateRestoredFiles),
                                        true));
                    }

                    restoredFiles.removeAll(candidateRestoredFiles);
                    candidateRestoredFiles.clear();
                    totalFileSize = 0;
                    fileNum = 0;
                }
            }

            remainedNewFiles.addAll(candidateNewFiles);

            if (remainedNewFiles.size() > 0) {
                tasks.add(
                        new CompactionTask(
                                partition, remainedNewFiles, Collections.EMPTY_LIST, false));
            }

            return tasks;
        }

        public void addRestoredFiles(List<DataFileMeta> dataFileMetas) {
            restoredFiles.addAll(dataFileMetas);
        }

        public void addIncremental(List<DataFileMeta> dataFileMetas) {
            newFiles.addAll(dataFileMetas);
        }

        public void updateRestoredFiles(List<ManifestEntry> entries) {
            entries.forEach(
                    entry -> {
                        if (entry.kind() == FileKind.ADD) {
                            if (entry.file().fileSize() < targetFileSize) {
                                restoredFiles.add(entry.file());
                            }
                        } else if (entry.kind() == FileKind.DELETE) {
                            restoredFiles.remove(entry.file());
                        } else {
                            throw new RuntimeException("unknown file kind: " + entry.kind());
                        }
                    });
        }
    }
}
