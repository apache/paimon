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
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Compact coordinator for append only tables.
 *
 * <p>Note: {@link AppendCompactCoordinator} scan files in snapshot, read APPEND and COMPACT
 * snapshot then load those new files. It will try it best to generate compaction task for the
 * restored files scanned in snapshot, but to reduce memory usage, it won't remain single file for a
 * long time. After ten times scan, single file with one partition will be ignored and removed from
 * memory, which means, it will not participate in compaction again until restart the compaction
 * job.
 *
 * <p>When a third task delete file in latest snapshot(including batch delete/update and overwrite),
 * the file in coordinator will still remain and participate in compaction task. When this happens,
 * compaction job will fail in commit stage, and fail-over to rescan the restored files in latest
 * snapshot.
 */
public class AppendCompactCoordinator {

    private static final int FILES_BATCH = 100_000;

    protected static final int REMOVE_AGE = 10;
    protected static final int COMPACT_AGE = 5;

    private final SnapshotManager snapshotManager;
    private final long targetFileSize;
    private final long compactionFileSize;
    private final double deleteThreshold;
    private final long openFileCost;
    private final int minFileNum;
    private final DvMaintainerCache dvMaintainerCache;
    private final FilesIterator filesIterator;

    final Map<BinaryRow, SubCoordinator> subCoordinators = new HashMap<>();

    public AppendCompactCoordinator(FileStoreTable table, boolean isStreaming) {
        this(table, isStreaming, null);
    }

    public AppendCompactCoordinator(
            FileStoreTable table,
            boolean isStreaming,
            @Nullable PartitionPredicate partitionPredicate) {
        checkArgument(table.primaryKeys().isEmpty());
        this.snapshotManager = table.snapshotManager();
        CoreOptions options = table.coreOptions();
        this.targetFileSize = options.targetFileSize(false);
        this.compactionFileSize = options.compactionFileSize(false);
        this.deleteThreshold = options.compactionDeleteRatioThreshold();
        this.openFileCost = options.splitOpenFileCost();
        this.minFileNum = options.compactionMinFileNum();
        this.dvMaintainerCache =
                options.deletionVectorsEnabled()
                        ? new DvMaintainerCache(table.store().newIndexFileHandler())
                        : null;
        this.filesIterator = new FilesIterator(table, isStreaming, partitionPredicate);
    }

    public List<AppendCompactTask> run() {
        // scan files in snapshot
        if (scan()) {
            // do plan compact tasks
            return compactPlan();
        }

        return Collections.emptyList();
    }

    @VisibleForTesting
    boolean scan() {
        Map<BinaryRow, List<DataFileMeta>> files = new HashMap<>();
        for (int i = 0; i < FILES_BATCH; i++) {
            ManifestEntry entry;
            try {
                entry = filesIterator.next();
            } catch (EndOfScanException e) {
                if (!files.isEmpty()) {
                    files.forEach(this::notifyNewFiles);
                    return true;
                }
                throw e;
            }
            if (entry == null) {
                break;
            }
            BinaryRow partition = entry.partition();
            files.computeIfAbsent(partition, k -> new ArrayList<>()).add(entry.file());
        }

        if (files.isEmpty()) {
            return false;
        }

        files.forEach(this::notifyNewFiles);
        return true;
    }

    @VisibleForTesting
    FilesIterator filesIterator() {
        return filesIterator;
    }

    @VisibleForTesting
    void notifyNewFiles(BinaryRow partition, List<DataFileMeta> files) {
        List<DataFileMeta> toCompact =
                files.stream()
                        .filter(file -> shouldCompact(partition, file))
                        .collect(Collectors.toList());
        subCoordinators
                .computeIfAbsent(partition, pp -> new SubCoordinator(partition))
                .addFiles(toCompact);
    }

    @VisibleForTesting
    // generate compaction task to the next stage
    List<AppendCompactTask> compactPlan() {
        // first loop to found compaction tasks
        List<AppendCompactTask> tasks =
                subCoordinators.values().stream()
                        .flatMap(s -> s.plan().stream())
                        .collect(Collectors.toList());

        // second loop to eliminate empty or old(with only one file) coordinator
        new ArrayList<>(subCoordinators.values())
                .stream()
                        .filter(SubCoordinator::readyToRemove)
                        .map(SubCoordinator::partition)
                        .forEach(subCoordinators::remove);

        return tasks;
    }

    @VisibleForTesting
    HashSet<DataFileMeta> listRestoredFiles() {
        HashSet<DataFileMeta> result = new HashSet<>();
        subCoordinators.values().stream().map(SubCoordinator::toCompact).forEach(result::addAll);
        return result;
    }

    /** Coordinator for a single partition. */
    class SubCoordinator {

        private final BinaryRow partition;
        private final HashSet<DataFileMeta> toCompact = new HashSet<>();
        int age = 0;

        public SubCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        public List<AppendCompactTask> plan() {
            return pickCompact();
        }

        public BinaryRow partition() {
            return partition;
        }

        public HashSet<DataFileMeta> toCompact() {
            return toCompact;
        }

        private List<AppendCompactTask> pickCompact() {
            List<List<DataFileMeta>> waitCompact = agePack();
            return waitCompact.stream()
                    .map(files -> new AppendCompactTask(partition, files))
                    .collect(Collectors.toList());
        }

        public void addFiles(List<DataFileMeta> dataFileMetas) {
            // reset age
            age = 0;
            // add to compact
            toCompact.addAll(dataFileMetas);
        }

        public boolean readyToRemove() {
            return toCompact.isEmpty() || age > REMOVE_AGE;
        }

        private List<List<DataFileMeta>> agePack() {
            List<List<DataFileMeta>> packed;
            if (dvMaintainerCache == null) {
                packed = pack(toCompact);
            } else {
                packed = packInDeletionVectorVMode(toCompact);
            }
            if (packed.isEmpty()) {
                // non-packed, we need to grow up age, and check whether to compact once
                if (++age > COMPACT_AGE && toCompact.size() > 1) {
                    List<DataFileMeta> all = new ArrayList<>(toCompact);
                    // empty the restored files, wait to be removed
                    toCompact.clear();
                    packed = Collections.singletonList(all);
                }
            }

            return packed;
        }

        private List<List<DataFileMeta>> pack(Set<DataFileMeta> toCompact) {
            // we don't know how many parallel compact works there should be, so in order to pack
            // better, we will sort them first
            ArrayList<DataFileMeta> files = new ArrayList<>(toCompact);
            files.sort(Comparator.comparingLong(DataFileMeta::fileSize));

            List<List<DataFileMeta>> result = new ArrayList<>();
            FileBin fileBin = new FileBin();
            for (DataFileMeta fileMeta : files) {
                fileBin.addFile(fileMeta);
                if (fileBin.enoughContent()) {
                    result.add(fileBin.drain());
                }
            }

            if (fileBin.enoughInputFiles()) {
                result.add(fileBin.drain());
            }
            // else skip these small files that are too few

            return result;
        }

        private List<List<DataFileMeta>> packInDeletionVectorVMode(Set<DataFileMeta> toCompact) {
            // we group the data files by their related index files.
            // In the subsequent compact task, if any files with deletion vectors are compacted, we
            // need to rewrite their corresponding deleted files. To avoid duplicate deleted files,
            // we must group them according to the deleted files
            Map<String, List<DataFileMeta>> filesWithDV = new HashMap<>();
            Set<DataFileMeta> rest = new HashSet<>();
            for (DataFileMeta dataFile : toCompact) {
                String indexFile =
                        dvMaintainerCache
                                .dvMaintainer(partition)
                                .getIndexFilePath(dataFile.fileName());
                if (indexFile == null) {
                    rest.add(dataFile);
                } else {
                    filesWithDV.computeIfAbsent(indexFile, f -> new ArrayList<>()).add(dataFile);
                }
            }

            // To avoid too small a compact task, merge them
            List<List<DataFileMeta>> dvGroups = new ArrayList<>(filesWithDV.values());
            dvGroups.sort(Comparator.comparingLong(this::fileSizeOfList));

            List<List<DataFileMeta>> result = new ArrayList<>();
            FileBin fileBin = new FileBin();
            for (List<DataFileMeta> dvGroup : dvGroups) {
                fileBin.addFiles(dvGroup);
                if (fileBin.enoughContent()) {
                    result.add(fileBin.drain());
                }
            }

            // for file with deletion vectors, must do compaction
            if (!fileBin.bin.isEmpty()) {
                result.add(fileBin.drain());
            }

            if (rest.size() > 1) {
                result.addAll(pack(rest));
            }
            return result;
        }

        private long fileSizeOfList(List<DataFileMeta> list) {
            return list.stream().mapToLong(DataFileMeta::fileSize).sum();
        }

        /** A file bin for {@link SubCoordinator} determine whether ready to compact. */
        private class FileBin {
            List<DataFileMeta> bin = new ArrayList<>();
            long totalFileSize = 0;

            public List<DataFileMeta> drain() {
                List<DataFileMeta> result = new ArrayList<>(bin);
                bin.forEach(toCompact::remove);
                bin.clear();
                totalFileSize = 0;
                return result;
            }

            public void addFiles(List<DataFileMeta> files) {
                files.forEach(this::addFile);
            }

            public void addFile(DataFileMeta file) {
                totalFileSize += file.fileSize() + openFileCost;
                bin.add(file);
            }

            private boolean enoughContent() {
                return bin.size() > 1 && totalFileSize >= targetFileSize * 2;
            }

            private boolean enoughInputFiles() {
                return bin.size() >= minFileNum;
            }
        }
    }

    private class DvMaintainerCache {

        private final IndexFileHandler indexFileHandler;

        /** Should be thread safe, ManifestEntryFilter will be invoked in many threads. */
        private final Map<BinaryRow, AppendDeleteFileMaintainer> cache = new ConcurrentHashMap<>();

        private DvMaintainerCache(IndexFileHandler indexFileHandler) {
            this.indexFileHandler = indexFileHandler;
        }

        private void refresh() {
            this.cache.clear();
        }

        private AppendDeleteFileMaintainer dvMaintainer(BinaryRow partition) {
            AppendDeleteFileMaintainer maintainer = cache.get(partition);
            if (maintainer == null) {
                synchronized (this) {
                    maintainer =
                            BaseAppendDeleteFileMaintainer.forUnawareAppend(
                                    indexFileHandler, snapshotManager.latestSnapshot(), partition);
                }
                cache.put(partition, maintainer);
            }
            return maintainer;
        }
    }

    /** Iterator to read files. */
    class FilesIterator {

        private final SnapshotReader snapshotReader;
        private final boolean streamingMode;

        @Nullable private Long nextSnapshot = null;
        @Nullable private Iterator<ManifestEntry> currentIterator;

        public FilesIterator(
                FileStoreTable table,
                boolean isStreaming,
                @Nullable PartitionPredicate partitionPredicate) {
            this.snapshotReader = table.newSnapshotReader();
            if (partitionPredicate != null) {
                snapshotReader.withPartitionFilter(partitionPredicate);
            }
            // drop stats to reduce memory
            if (table.coreOptions().manifestDeleteFileDropStats()) {
                snapshotReader.dropStats();
            }
            this.streamingMode = isStreaming;
        }

        private void assignNewIterator() {
            currentIterator = null;
            if (nextSnapshot == null) {
                nextSnapshot = snapshotManager.latestSnapshotId();
                if (nextSnapshot == null) {
                    if (!streamingMode) {
                        throw new EndOfScanException();
                    }
                    return;
                }
                snapshotReader.withMode(ScanMode.ALL);
            } else {
                if (!streamingMode) {
                    throw new EndOfScanException();
                }
                snapshotReader.withMode(ScanMode.DELTA);
            }

            if (!snapshotManager.snapshotExists(nextSnapshot)) {
                return;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshot);
            nextSnapshot++;

            if (dvMaintainerCache != null) {
                dvMaintainerCache.refresh();
            }
            currentIterator =
                    snapshotReader
                            .withManifestEntryFilter(
                                    entry -> shouldCompact(entry.partition(), entry.file()))
                            .withSnapshot(snapshot)
                            .readFileIterator();
        }

        @Nullable
        public ManifestEntry next() {
            while (true) {
                if (currentIterator == null) {
                    assignNewIterator();
                    if (currentIterator == null) {
                        return null;
                    }
                }

                if (currentIterator.hasNext()) {
                    ManifestEntry entry = currentIterator.next();
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    } else {
                        return entry;
                    }
                }
                currentIterator = null;
            }
        }
    }

    private boolean shouldCompact(BinaryRow partition, DataFileMeta file) {
        return file.fileSize() < compactionFileSize || tooHighDeleteRatio(partition, file);
    }

    private boolean tooHighDeleteRatio(BinaryRow partition, DataFileMeta file) {
        if (dvMaintainerCache != null) {
            DeletionFile deletionFile =
                    dvMaintainerCache.dvMaintainer(partition).getDeletionFile(file.fileName());
            if (deletionFile != null) {
                Long cardinality = deletionFile.cardinality();
                long rowCount = file.rowCount();
                return cardinality == null || cardinality > rowCount * deleteThreshold;
            }
        }
        return false;
    }

    @VisibleForTesting
    AppendDeleteFileMaintainer dvMaintainer(BinaryRow partition) {
        return dvMaintainerCache.dvMaintainer(partition);
    }
}
