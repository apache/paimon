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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileOperationThreadPool;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Base class for file deletion including methods for clean data files, manifest files and empty
 * data directories.
 */
public abstract class FileDeletionBase<T extends Snapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(FileDeletionBase.class);

    protected final FileIO fileIO;
    protected final FileStorePathFactory pathFactory;
    protected final ManifestFile manifestFile;
    protected final ManifestList manifestList;
    protected final IndexFileHandler indexFileHandler;
    protected final StatsFileHandler statsFileHandler;
    private final boolean cleanEmptyDirectories;
    protected final Map<BinaryRow, Set<Integer>> deletionBuckets;

    private final Executor fileExecutor;
    private final int fileOperationParallelism;

    protected boolean changelogDecoupled;

    /** Used to record which tag is cached. */
    private long cachedTag = 0;

    /** Used to cache data files used by current tag. */
    private final Map<BinaryRow, Map<Integer, Set<String>>> cachedTagDataFiles = new HashMap<>();

    public FileDeletionBase(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
            boolean cleanEmptyDirectories,
            int fileOperationThreadNum) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.indexFileHandler = indexFileHandler;
        this.statsFileHandler = statsFileHandler;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.deletionBuckets = new ConcurrentHashMap<>();
        this.fileExecutor = FileOperationThreadPool.getExecutorService(fileOperationThreadNum);
        this.fileOperationParallelism =
                Math.max(
                        1,
                        fileOperationThreadNum > 0
                                ? fileOperationThreadNum
                                : Runtime.getRuntime().availableProcessors());
    }

    public Executor fileExecutor() {
        return fileExecutor;
    }

    /**
     * Clean data files that will not be used anymore in the snapshot.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skipper if the test result of a data file is true, it will be skipped when deleting;
     *     else it will be deleted
     */
    public abstract void cleanUnusedDataFiles(T snapshot, Predicate<ExpireFileEntry> skipper);

    /**
     * Clean metadata files that will not be used anymore of a snapshot, including data manifests,
     * index manifests and manifest lists.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skippingSet manifests that should not be deleted
     */
    public abstract void cleanUnusedManifests(T snapshot, Set<String> skippingSet);

    public void setChangelogDecoupled(boolean changelogDecoupled) {
        this.changelogDecoupled = changelogDecoupled;
    }

    /** Try to delete data directories that may be empty after data file deletion. */
    public void cleanEmptyDirectories() {
        if (!cleanEmptyDirectories || deletionBuckets.isEmpty()) {
            return;
        }

        Map<Integer, Set<Path>> deduplicate = new HashMap<>();
        for (Map.Entry<BinaryRow, Set<Integer>> entry : deletionBuckets.entrySet()) {
            List<Path> toDeleteEmptyDirectory = new ArrayList<>();
            // try to delete bucket directories
            for (Integer bucket : entry.getValue()) {
                toDeleteEmptyDirectory.add(pathFactory.bucketPath(entry.getKey(), bucket));
            }
            deleteFiles(toDeleteEmptyDirectory, this::tryDeleteEmptyDirectory);

            List<Path> hierarchicalPaths = pathFactory.getHierarchicalPartitionPath(entry.getKey());
            int hierarchies = hierarchicalPaths.size();
            if (hierarchies == 0) {
                continue;
            }

            if (tryDeleteEmptyDirectory(hierarchicalPaths.get(hierarchies - 1))) {
                // deduplicate high level partition directories
                for (int hierarchy = 0; hierarchy < hierarchies - 1; hierarchy++) {
                    Path path = hierarchicalPaths.get(hierarchy);
                    deduplicate.computeIfAbsent(hierarchy, i -> new HashSet<>()).add(path);
                }
            }
        }

        // from deepest to shallowest
        for (int hierarchy = deduplicate.size() - 1; hierarchy >= 0; hierarchy--) {
            deduplicate.get(hierarchy).forEach(this::tryDeleteEmptyDirectory);
        }

        deletionBuckets.clear();
    }

    protected void recordDeletionBuckets(ExpireFileEntry entry) {
        deletionBuckets
                .computeIfAbsent(entry.partition(), p -> ConcurrentHashMap.newKeySet())
                .add(entry.bucket());
    }

    public void cleanUnusedDataFiles(String manifestList, Predicate<ExpireFileEntry> skipper) {
        deleteDataFiles(planUnusedDataFiles(manifestList, skipper));
    }

    public List<Path> planUnusedDataFiles(String manifestList, Predicate<ExpireFileEntry> skipper) {
        List<ManifestFileMeta> manifests = tryReadManifestList(manifestList);
        // data file path -> (original manifest entry, extra file paths)
        Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        try {
            getDataFileToDelete(dataFileToDelete, readExpireFileEntries(manifests));
        } catch (Exception e) {
            // cancel deletion if any exception occurs
            LOG.warn("Failed to read some manifest files. Cancel deletion.", e);
            return Collections.emptyList();
        }

        return planUnusedDataFile(dataFileToDelete, skipper);
    }

    protected void doCleanUnusedDataFile(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            Predicate<ExpireFileEntry> skipper) {
        deleteDataFiles(planUnusedDataFile(dataFileToDelete, skipper));
    }

    protected List<Path> planUnusedDataFile(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            Predicate<ExpireFileEntry> skipper) {
        List<Path> actualDataFileToDelete = new ArrayList<>();
        dataFileToDelete.forEach(
                (path, pair) -> {
                    ExpireFileEntry entry = pair.getLeft();
                    // check whether we should skip the data file
                    if (!skipper.test(entry)) {
                        // delete data files
                        actualDataFileToDelete.add(path);
                        actualDataFileToDelete.addAll(pair.getRight());

                        recordDeletionBuckets(entry);
                    }
                });
        return actualDataFileToDelete;
    }

    protected void getDataFileToDelete(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            Iterable<ExpireFileEntry> dataFileEntries) {
        // we cannot delete a data file directly when we meet a DELETE entry, because that
        // file might be upgraded
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : dataFileEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            Path dataFilePath = dataFilePathFactory.toPath(entry);
            switch (entry.kind()) {
                case ADD:
                    dataFileToDelete.remove(dataFilePath);
                    break;
                case DELETE:
                    List<Path> extraFiles = new ArrayList<>(entry.extraFiles().size());
                    for (String file : entry.extraFiles()) {
                        extraFiles.add(dataFilePathFactory.toAlignedPath(file, entry));
                    }
                    dataFileToDelete.put(dataFilePath, Pair.of(entry, extraFiles));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    protected void getDataFileToDelete(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            List<ExpireFileEntry> dataFileEntries) {
        getDataFileToDelete(dataFileToDelete, (Iterable<ExpireFileEntry>) dataFileEntries);
    }

    /**
     * Delete added file in the manifest list files. Added files marked as "ADD" in manifests.
     *
     * @param manifestListName name of manifest list
     */
    public void deleteAddedDataFiles(String manifestListName) {
        deleteDataFiles(planAddedDataFiles(manifestListName));
    }

    public List<Path> planAddedDataFiles(String manifestListName) {
        List<ManifestFileMeta> manifests = tryReadManifestList(manifestListName);
        return planAddedDataFiles(readExpireFileEntriesIgnoringErrors(manifests));
    }

    private Iterable<ExpireFileEntry> readExpireFileEntriesIgnoringErrors(
            List<ManifestFileMeta> manifests) {
        return ManifestReadThreadPool.sequentialBatchedExecute(
                manifest -> {
                    try {
                        return manifestFile.readExpireFileEntries(
                                manifest.fileName(), manifest.fileSize());
                    } catch (Exception e) {
                        // We want to delete the data file, so just ignore the unavailable files
                        LOG.info(
                                "Failed to read manifest " + manifest.fileName() + ". Ignore it.",
                                e);
                        return Collections.emptyList();
                    }
                },
                manifests,
                fileOperationParallelism);
    }

    private Iterable<ExpireFileEntry> readExpireFileEntries(List<ManifestFileMeta> manifests) {
        return ManifestReadThreadPool.sequentialBatchedExecute(
                manifest ->
                        manifestFile.readExpireFileEntries(
                                manifest.fileName(), manifest.fileSize()),
                manifests,
                fileOperationParallelism);
    }

    private List<Path> planAddedDataFiles(Iterable<ExpireFileEntry> manifestEntries) {
        List<Path> dataFiles = new ArrayList<>();
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            if (entry.kind() == FileKind.ADD) {
                dataFiles.add(dataFilePathFactory.toPath(entry));
                recordDeletionBuckets(entry);
            }
        }
        return dataFiles;
    }

    public void deleteDataFiles(Collection<Path> dataFiles) {
        deleteFiles(new LinkedHashSet<>(dataFiles), fileIO::deleteQuietly);
    }

    public void cleanUnusedStatisticsManifests(Snapshot snapshot, Set<String> skippingSet) {
        deletePlannedManifests(
                Collections.singletonList(planUnusedStatisticsManifests(snapshot, skippingSet)));
    }

    public ManifestDeletionPlan planUnusedStatisticsManifests(
            Snapshot snapshot, Set<String> skippingSet) {
        ManifestDeletionPlan plan = new ManifestDeletionPlan();
        if (snapshot.statistics() != null && !skippingSet.contains(snapshot.statistics())) {
            plan.addStatistic(snapshot.statistics());
        }
        return plan;
    }

    public void cleanUnusedIndexManifests(Snapshot snapshot, Set<String> skippingSet) {
        deletePlannedManifests(
                Collections.singletonList(planUnusedIndexManifests(snapshot, skippingSet)));
    }

    public ManifestDeletionPlan planUnusedIndexManifests(
            Snapshot snapshot, Set<String> skippingSet) {
        ManifestDeletionPlan plan = new ManifestDeletionPlan();
        // clean index manifests
        String indexManifest = snapshot.indexManifest();
        // check exists, it may have been deleted by other snapshots
        if (indexManifest != null) {
            List<IndexManifestEntry> indexManifestEntries;
            try {
                indexManifestEntries = indexFileHandler.readManifestWithIOException(indexManifest);
            } catch (FileNotFoundException e) {
                return plan;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            indexManifestEntries.removeIf(
                    entry -> skippingSet.contains(entry.indexFile().fileName()));
            plan.addIndexFiles(indexManifestEntries);

            if (!skippingSet.contains(indexManifest)) {
                plan.addIndexManifest(indexManifest);
            }
        }
        return plan;
    }

    public void cleanUnusedManifestList(String manifestName, Set<String> skippingSet) {
        deletePlannedManifests(
                Collections.singletonList(planUnusedManifestList(manifestName, skippingSet)));
    }

    public ManifestDeletionPlan planUnusedManifestList(
            String manifestName, Set<String> skippingSet) {
        return planUnusedManifestList(manifestName, skippingSet, true);
    }

    private ManifestDeletionPlan planUnusedManifestList(
            String manifestName, Set<String> skippingSet, boolean updateSkippingSet) {
        ManifestDeletionPlan plan = new ManifestDeletionPlan();
        List<String> toDeleteManifests = new ArrayList<>();
        List<ManifestFileMeta> toExpireManifests = tryReadManifestList(manifestName);
        for (ManifestFileMeta manifest : toExpireManifests) {
            String fileName = manifest.fileName();
            if (!skippingSet.contains(fileName)) {
                toDeleteManifests.add(fileName);
                // to avoid other snapshots trying to delete again
                if (updateSkippingSet) {
                    skippingSet.add(fileName);
                }
            }
        }
        if (!skippingSet.contains(manifestName)) {
            toDeleteManifests.add(manifestName);
        }

        plan.addManifestFiles(toDeleteManifests);
        return plan;
    }

    protected void cleanUnusedManifests(
            Snapshot snapshot,
            Set<String> skippingSet,
            boolean deleteDataManifestLists,
            boolean deleteChangelog) {
        deletePlannedManifests(
                Collections.singletonList(
                        planUnusedManifests(
                                snapshot, skippingSet, deleteDataManifestLists, deleteChangelog)));
    }

    protected ManifestDeletionPlan planUnusedManifests(
            Snapshot snapshot,
            Set<String> skippingSet,
            boolean deleteDataManifestLists,
            boolean deleteChangelog) {
        return planUnusedManifests(
                snapshot, skippingSet, deleteDataManifestLists, deleteChangelog, true);
    }

    protected ManifestDeletionPlan planUnusedManifests(
            Snapshot snapshot,
            Set<String> skippingSet,
            boolean deleteDataManifestLists,
            boolean deleteChangelog,
            boolean updateSkippingSet) {
        ManifestDeletionPlan plan = new ManifestDeletionPlan();
        if (deleteDataManifestLists) {
            // deleteDataManifestLists will be false
            // with changelog decouple + none changelog producer.
            // Why don't we clean base manifest in this scenario?
            // Because cleanUnusedManifestList is compared with the earliest snapshot.
            // For none changelog producer, changelog files are the level 0 files.
            // Even if these files are not used by the earliest snapshot,
            // we have to keep them as changelog, and clean then in ChangelogDeletion.
            plan.merge(
                    planUnusedManifestList(
                            snapshot.baseManifestList(), skippingSet, updateSkippingSet));
            plan.merge(
                    planUnusedManifestList(
                            snapshot.deltaManifestList(), skippingSet, updateSkippingSet));
        }
        if (deleteChangelog && snapshot.changelogManifestList() != null) {
            plan.merge(
                    planUnusedManifestList(
                            snapshot.changelogManifestList(), skippingSet, updateSkippingSet));
        }
        plan.merge(planUnusedIndexManifests(snapshot, skippingSet));
        plan.merge(planUnusedStatisticsManifests(snapshot, skippingSet));
        return plan;
    }

    public void deletePlannedManifests(Collection<ManifestDeletionPlan> plans) {
        Set<String> manifests = new LinkedHashSet<>();
        Set<IndexManifestEntry> indexFiles = new LinkedHashSet<>();
        Set<String> indexManifests = new LinkedHashSet<>();
        Set<String> statistics = new LinkedHashSet<>();
        for (ManifestDeletionPlan plan : plans) {
            manifests.addAll(plan.manifests);
            indexFiles.addAll(plan.indexFiles);
            indexManifests.addAll(plan.indexManifests);
            statistics.addAll(plan.statistics);
        }

        deleteFiles(manifests, manifestFile::delete);
        deleteFiles(indexFiles, indexFileHandler::deleteIndexFile);
        deleteFiles(indexManifests, indexFileHandler::deleteManifest);
        deleteFiles(statistics, statsFileHandler::deleteStats);
    }

    public Predicate<ExpireFileEntry> createDataFileSkipperForTags(
            List<Snapshot> taggedSnapshots, long expiringSnapshotId) throws Exception {
        int index = SnapshotManager.findPreviousSnapshot(taggedSnapshots, expiringSnapshotId);
        // refresh tag data files
        if (index >= 0) {
            Snapshot previousTag = taggedSnapshots.get(index);
            if (previousTag.id() != cachedTag) {
                cachedTag = 0;
                cachedTagDataFiles.clear();
                addMergedDataFiles(cachedTagDataFiles, previousTag);
                // update cachedTag after read tag successfully
                cachedTag = previousTag.id();
            }
            return entry -> containsDataFile(cachedTagDataFiles, entry);
        }
        return entry -> false;
    }

    public Predicate<ExpireFileEntry> createDataFileSkipperForTag(Snapshot tag) throws Exception {
        Map<BinaryRow, Map<Integer, Set<String>>> tagDataFiles = new HashMap<>();
        addMergedDataFiles(tagDataFiles, tag);
        return entry -> containsDataFile(tagDataFiles, entry);
    }

    /**
     * It is possible that a job was killed during expiration and some manifest files have been
     * deleted, so if the clean methods need to get manifests of a snapshot to be cleaned, we should
     * try to read manifests and return empty list if failed instead of calling {@link
     * ManifestList#readDataManifests} directly.
     */
    protected List<ManifestFileMeta> tryReadManifestList(String manifestListName) {
        try {
            return manifestList.read(manifestListName);
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file {}", manifestListName, e);
            return Collections.emptyList();
        }
    }

    /**
     * NOTE: This method is used for building data file skipping set. If failed to read some
     * manifests, it will throw exception which callers must handle.
     */
    protected void addMergedDataFiles(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, Snapshot snapshot)
            throws IOException {
        for (ExpireFileEntry entry :
                readMergedDataFiles(manifestList.readDataManifests(snapshot))) {
            dataFiles
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                    .add(entry.fileName());
        }
    }

    protected Collection<ExpireFileEntry> readMergedDataFiles(List<ManifestFileMeta> manifests)
            throws IOException {
        Map<Identifier, ExpireFileEntry> map = new HashMap<>();
        FileEntry.mergeEntries(readExpireFileEntries(manifests), map);
        return map.values();
    }

    protected boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ExpireFileEntry entry) {
        Map<Integer, Set<String>> buckets = dataFiles.get(entry.partition());
        if (buckets != null) {
            Set<String> fileNames = buckets.get(entry.bucket());
            if (fileNames != null) {
                return fileNames.contains(entry.fileName());
            }
        }
        return false;
    }

    public Set<String> manifestSkippingSet(List<Snapshot> skippingSnapshots) {
        if (skippingSnapshots.size() <= 1) {
            Set<String> skippingSet = new HashSet<>();
            for (Snapshot skippingSnapshot : skippingSnapshots) {
                skippingSet.addAll(manifestSkippingSet(skippingSnapshot));
            }
            return skippingSet;
        }

        List<CompletableFuture<Set<String>>> futures = new ArrayList<>();
        for (Snapshot skippingSnapshot : skippingSnapshots) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> manifestSkippingSet(skippingSnapshot),
                            ManifestReadThreadPool.getExecutorService(fileOperationParallelism)));
        }

        Set<String> skippingSet = new HashSet<>();
        for (CompletableFuture<Set<String>> future : futures) {
            skippingSet.addAll(getSkippingSet(future));
        }
        return skippingSet;
    }

    private Set<String> getSkippingSet(CompletableFuture<Set<String>> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private Set<String> manifestSkippingSet(Snapshot skippingSnapshot) {
        Set<String> skippingSet = new HashSet<>();

        // data manifests
        skippingSet.add(skippingSnapshot.baseManifestList());
        skippingSet.add(skippingSnapshot.deltaManifestList());
        manifestList.readDataManifests(skippingSnapshot).stream()
                .map(ManifestFileMeta::fileName)
                .forEach(skippingSet::add);

        // index manifests
        String indexManifest = skippingSnapshot.indexManifest();
        if (indexManifest != null) {
            skippingSet.add(indexManifest);
            indexFileHandler.readManifest(indexManifest).stream()
                    .map(IndexManifestEntry::indexFile)
                    .map(IndexFileMeta::fileName)
                    .forEach(skippingSet::add);
        }

        // statistics
        if (skippingSnapshot.statistics() != null) {
            skippingSet.add(skippingSnapshot.statistics());
        }

        return skippingSet;
    }

    private boolean tryDeleteEmptyDirectory(Path path) {
        try {
            fileIO.delete(path, false);
            return true;
        } catch (IOException e) {
            LOG.debug("Failed to delete directory '{}'. Check whether it is empty.", path);
            return false;
        }
    }

    protected <F> void deleteFiles(Collection<F> files, Consumer<F> deletion) {
        if (files.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> deletionFutures = new ArrayList<>(files.size());
        for (F file : files) {
            deletionFutures.add(
                    CompletableFuture.runAsync(() -> deletion.accept(file), fileExecutor));
        }

        waitForDeletion(deletionFutures);
    }

    private void waitForDeletion(List<CompletableFuture<Void>> deletionFutures) {
        if (deletionFutures.isEmpty()) {
            return;
        }

        try {
            CompletableFuture.allOf(deletionFutures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /** Planned manifest, index, and statistics files to clean. */
    public static class ManifestDeletionPlan {

        private final List<String> manifests = new ArrayList<>();
        private final List<IndexManifestEntry> indexFiles = new ArrayList<>();
        private final List<String> indexManifests = new ArrayList<>();
        private final List<String> statistics = new ArrayList<>();

        public static ManifestDeletionPlan empty() {
            return new ManifestDeletionPlan();
        }

        private void addManifestFiles(Collection<String> manifestFiles) {
            manifests.addAll(manifestFiles);
        }

        private void addIndexFiles(Collection<IndexManifestEntry> files) {
            indexFiles.addAll(files);
        }

        private void addIndexManifest(String indexManifest) {
            indexManifests.add(indexManifest);
        }

        private void addStatistic(String statistic) {
            statistics.add(statistic);
        }

        private void merge(ManifestDeletionPlan plan) {
            manifests.addAll(plan.manifests);
            indexFiles.addAll(plan.indexFiles);
            indexManifests.addAll(plan.indexManifests);
            statistics.addAll(plan.statistics);
        }
    }
}
