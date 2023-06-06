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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link AppendOnlyFileStoreTable} compact coordinator. */
public class AppendOnlyTableCompactionCoordinator {
    private final long targetFileSize;
    private final int minFileNum;
    private final int maxFileNum;

    private final Map<BinaryRow, PartitionCompactCoordinator> partitionCompactCoordinators =
            new HashMap<>();

    public AppendOnlyTableCompactionCoordinator(AppendOnlyFileStoreTable table) {
        CoreOptions coreOptions = table.coreOptions();
        this.targetFileSize = coreOptions.targetFileSize();
        this.minFileNum = coreOptions.compactionMinFileNum();
        this.maxFileNum = coreOptions.compactionMaxFileNum();
    }

    public void notifyNewFiles(BinaryRow partition, List<DataFileMeta> files) {
        partitionCompactCoordinators
                .computeIfAbsent(partition, PartitionCompactCoordinator::new)
                .addFiles(
                        files.stream()
                                .filter(file -> file.fileSize() < targetFileSize)
                                .collect(Collectors.toList()));
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

        private List<List<DataFileMeta>> pack() {
            // we compact smaller files first
            // step 1, sort files by file size, pick the smaller first
            ArrayList<DataFileMeta> files = new ArrayList<>(toCompact);
            files.sort(Comparator.comparingLong(DataFileMeta::fileSize));

            // step 2, when files picked size greater than targetFileSize(meanwhile file num greater
            // than minFileNum) or file numbers bigger than maxFileNum, we pack it to a compaction
            // task
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
