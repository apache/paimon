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
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link AppendOnlyFileStoreTable} compact coordinator. */
public class AppendOnlyTableCompactionCoordinator {
    protected static final int REMOVE_AGE = 10;
    protected static final int COMPACT_AGE = 5;

    private final StreamTableScan scan;
    private final long targetFileSize;
    private final int minFileNum;
    private final int maxFileNum;

    protected final Map<BinaryRow, PartitionCompactCoordinator> partitionCompactCoordinators =
            new HashMap<>();

    public AppendOnlyTableCompactionCoordinator(AppendOnlyFileStoreTable table) {
        scan = table.copy(compactScanType()).newStreamScan();
        CoreOptions coreOptions = table.coreOptions();
        this.targetFileSize = coreOptions.targetFileSize();
        this.minFileNum = coreOptions.compactionMinFileNum();
        this.maxFileNum = coreOptions.compactionMaxFileNum();
    }

    public void invokeScan() {
        scan.plan()
                .splits()
                .forEach(
                        split -> {
                            DataSplit dataSplit = (DataSplit) split;
                            notifyNewFiles(dataSplit.partition(), dataSplit.files());
                        });
    }

    protected void notifyNewFiles(BinaryRow partition, List<DataFileMeta> files) {
        partitionCompactCoordinators
                .computeIfAbsent(partition, PartitionCompactCoordinator::new)
                .addFiles(
                        files.stream()
                                .filter(file -> file.fileSize() < targetFileSize)
                                .collect(Collectors.toList()));
    }

    // generate compaction task to the next stage
    public List<AppendOnlyCompactionTask> compactPlan() {
        // first loop to found compaction tasks
        List<AppendOnlyCompactionTask> tasks =
                partitionCompactCoordinators.values().stream()
                        .flatMap(s -> s.plan().stream())
                        .collect(Collectors.toList());

        // second loop to eliminate empty or old(with only one file) coordinator
        new ArrayList<>(partitionCompactCoordinators.values())
                .stream()
                        .filter(PartitionCompactCoordinator::readyToRemove)
                        .map(PartitionCompactCoordinator::partition)
                        .forEach(partitionCompactCoordinators::remove);

        return tasks;
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

    private Map<String, String> compactScanType() {
        return new HashMap<String, String>() {
            {
                put(CoreOptions.STREAMING_COMPACT.key(), "non-bucket");
            }
        };
    }

    /** Coordinator for a single partition. */
    private class PartitionCompactCoordinator {

        BinaryRow partition;
        HashSet<DataFileMeta> toCompact = new HashSet<>();
        int age = 0;

        public PartitionCompactCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        public List<AppendOnlyCompactionTask> plan() {
            return pickCompact();
        }

        public HashSet<DataFileMeta> getToCompact() {
            return toCompact;
        }

        public BinaryRow partition() {
            return partition;
        }

        private List<AppendOnlyCompactionTask> pickCompact() {
            List<List<DataFileMeta>> waitCompact = agePack();
            return waitCompact.stream()
                    .map(files -> new AppendOnlyCompactionTask(partition, files))
                    .collect(Collectors.toList());
        }

        public void addFiles(List<DataFileMeta> dataFileMetas) {
            toCompact.addAll(dataFileMetas);
        }

        public boolean readyToRemove() {
            return toCompact.size() == 0 || age > REMOVE_AGE;
        }

        private List<List<DataFileMeta>> agePack() {
            List<List<DataFileMeta>> packed = pack();
            if (packed.size() == 0) {
                // non-packed, we need to grow up age, and check whether to compact once
                if (++age > COMPACT_AGE && toCompact.size() > 1) {
                    List<DataFileMeta> all = new ArrayList<>(toCompact);
                    // empty the restored files, wait to be removed
                    toCompact.clear();
                    packed = Collections.singletonList(all);
                }
            } else {
                // reset age
                age = 0;
            }

            return packed;
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
            FileBin fileBin = new FileBin();
            for (DataFileMeta fileMeta : files) {
                fileBin.addFile(fileMeta);
                if (fileBin.binReady()) {
                    result.add(new ArrayList<>(fileBin.bin));
                    // remove it from coordinator memory, won't join in compaction again
                    fileBin.reset();
                }
            }
            return result;
        }

        /**
         * A file bin for {@link PartitionCompactCoordinator} determine whether ready to compact.
         */
        private class FileBin {
            List<DataFileMeta> bin = new ArrayList<>();
            long totalFileSize = 0;
            int fileNum = 0;

            public void reset() {
                bin.forEach(toCompact::remove);
                bin.clear();
                totalFileSize = 0;
                fileNum = 0;
            }

            public void addFile(DataFileMeta file) {
                totalFileSize += file.fileSize();
                fileNum++;
                bin.add(file);
            }

            public boolean binReady() {
                return (totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum;
            }
        }
    }
}
