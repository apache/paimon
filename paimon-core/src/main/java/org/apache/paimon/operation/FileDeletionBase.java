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
import org.apache.paimon.append.dataevolution.DataEvolutionRowIdAssignment;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestBucketFilter;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileOperationThreadPool;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
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
    @Nullable private final Integer manifestReadParallelism;

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
            int fileOperationThreadNum,
            @Nullable Integer manifestReadParallelism) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.indexFileHandler = indexFileHandler;
        this.statsFileHandler = statsFileHandler;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.deletionBuckets = new ConcurrentHashMap<>();
        this.fileExecutor = FileOperationThreadPool.getExecutorService(fileOperationThreadNum);
        this.manifestReadParallelism = manifestReadParallelism;
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
    public abstract void cleanDeletedDataFiles(T snapshot, Predicate<ExpireFileEntry> skipper);

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
            executeAll(toDeleteEmptyDirectory, this::tryDeleteEmptyDirectory);

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

    /** Plan data files referenced by DELETE entries in the snapshot's delta manifest list. */
    public DataFileDeletionPlan planDeletedInDeltaManifest(T snapshot) {
        String deltaManifestList = snapshot.deltaManifestList();
        // data file path -> (original manifest entry, extra file paths)
        Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        try {
            Iterable<ExpireFileEntry> dataFileEntries =
                    readExpireFileEntries(tryReadManifestList(deltaManifestList));
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
        } catch (Exception e) {
            // cancel deletion if any exception occurs
            LOG.warn("Failed to read some manifest files. Cancel deletion.", e);
            return DataFileDeletionPlan.empty();
        }

        return new DataFileDeletionPlan(dataFileToDelete);
    }

    /** Plan data files referenced by DELETE entries in the snapshot's delta manifest list. */
    public List<Path> planDeletedInDeltaManifest(T snapshot, Predicate<ExpireFileEntry> skipper) {
        return dataFilesToDelete(planDeletedInDeltaManifest(snapshot), skipper);
    }

    public List<Path> dataFilesToDelete(
            DataFileDeletionPlan plan, Predicate<ExpireFileEntry> skipper) {
        // apply skipper
        List<Path> actualDataFileToDelete = new ArrayList<>();
        plan.dataFileToDelete.forEach(
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

    /** Plan data files referenced by ADD entries in the snapshot's changelog manifest list. */
    public List<Path> planAddedInChangelogManifest(T snapshot) {
        List<ManifestFileMeta> manifests = tryReadManifestList(snapshot.changelogManifestList());
        Iterable<ExpireFileEntry> entries =
                ManifestReadThreadPool.sequentialBatchedExecute(
                        manifest -> {
                            try {
                                return manifestFile.readExpireFileEntries(manifest.fileName());
                            } catch (Exception e) {
                                // We want to delete the data file, so just ignore the unavailable
                                // files
                                LOG.info(
                                        "Failed to read manifest {}. Ignore it.",
                                        manifest.fileName(),
                                        e);
                                return Collections.emptyList();
                            }
                        },
                        manifests,
                        manifestReadParallelism);

        List<Path> dataFiles = new ArrayList<>();
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : entries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            if (entry.kind() == FileKind.ADD) {
                dataFiles.add(dataFilePathFactory.toPath(entry));
                recordDeletionBuckets(entry);
            }
        }
        return dataFiles;
    }

    private Iterable<ExpireFileEntry> readExpireFileEntries(List<ManifestFileMeta> manifests) {
        return ManifestReadThreadPool.sequentialBatchedExecute(
                manifest -> manifestFile.readExpireFileEntries(manifest.fileName()),
                manifests,
                manifestReadParallelism);
    }

    public void cleanDataFiles(Collection<Path> dataFiles) {
        executeAll(new LinkedHashSet<>(dataFiles), fileIO::deleteQuietly);
    }

    private void collectUnusedStatisticsManifests(
            Snapshot snapshot, Set<String> skippingSet, Set<String> statistics) {
        if (snapshot.statistics() != null && skippingSet.add(snapshot.statistics())) {
            statistics.add(snapshot.statistics());
        }
    }

    private void collectUnusedIndexManifests(
            Snapshot snapshot,
            Set<String> skippingSet,
            Set<IndexManifestEntry> indexFiles,
            Set<String> indexManifests) {
        // clean index manifests
        String indexManifest = snapshot.indexManifest();
        // check exists, it may have been deleted by other snapshots
        if (indexManifest != null) {
            List<IndexManifestEntry> indexManifestEntries;
            try {
                indexManifestEntries = indexFileHandler.readManifestWithIOException(indexManifest);
            } catch (FileNotFoundException e) {
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (IndexManifestEntry entry : indexManifestEntries) {
                if (skippingSet.add(entry.indexFile().fileName())) {
                    indexFiles.add(entry);
                }
            }

            if (skippingSet.add(indexManifest)) {
                indexManifests.add(indexManifest);
            }
        }
    }

    protected void collectUnusedManifestList(
            String manifestName, Set<String> skippingSet, Set<String> manifests) {
        List<ManifestFileMeta> toExpireManifests = tryReadManifestList(manifestName);
        for (ManifestFileMeta manifest : toExpireManifests) {
            String fileName = manifest.fileName();
            if (skippingSet.add(fileName)) {
                manifests.add(fileName);
                if (manifest.extraFiles() != null) {
                    for (String extraFile : manifest.extraFiles()) {
                        if (skippingSet.add(extraFile)) {
                            manifests.add(extraFile);
                        }
                    }
                }
            }
        }
        if (skippingSet.add(manifestName)) {
            manifests.add(manifestName);
        }
    }

    protected List<Runnable> planManifestsCleaner(
            Snapshot snapshot,
            Set<String> skippingSet,
            boolean deleteDataManifestLists,
            boolean deleteChangelog) {
        Set<String> manifests = new LinkedHashSet<>();
        Set<IndexManifestEntry> indexFiles = new LinkedHashSet<>();
        Set<String> indexManifests = new LinkedHashSet<>();
        Set<String> statistics = new LinkedHashSet<>();
        if (deleteDataManifestLists) {
            // deleteDataManifestLists will be false
            // with changelog decouple + none changelog producer.
            // Why don't we clean base manifest in this scenario?
            // Because cleanUnusedManifestList is compared with the earliest snapshot.
            // For none changelog producer, changelog files are the level 0 files.
            // Even if these files are not used by the earliest snapshot,
            // we have to keep them as changelog, and clean then in ChangelogDeletion.
            collectUnusedManifestList(snapshot.baseManifestList(), skippingSet, manifests);
            collectUnusedManifestList(snapshot.deltaManifestList(), skippingSet, manifests);
        }
        if (deleteChangelog && snapshot.changelogManifestList() != null) {
            collectUnusedManifestList(snapshot.changelogManifestList(), skippingSet, manifests);
        }
        collectUnusedIndexManifests(snapshot, skippingSet, indexFiles, indexManifests);
        collectUnusedStatisticsManifests(snapshot, skippingSet, statistics);

        String reassignPlan = DataEvolutionRowIdAssignment.planFile(snapshot);
        if (reassignPlan != null && skippingSet.add(reassignPlan)) {
            manifests.add(reassignPlan);
        }

        List<Runnable> tasks = new ArrayList<>();
        for (String manifest : manifests) {
            tasks.add(() -> manifestFile.delete(manifest));
        }
        for (IndexManifestEntry indexFile : indexFiles) {
            tasks.add(() -> indexFileHandler.deleteIndexFile(indexFile));
        }
        for (String indexManifest : indexManifests) {
            tasks.add(() -> indexFileHandler.deleteManifest(indexManifest));
        }
        for (String statistic : statistics) {
            tasks.add(() -> statsFileHandler.deleteStats(statistic));
        }
        return tasks;
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
     * Creates a tag skipper restricted to the files in the supplied deletion plans.
     *
     * <p>A tag may reference every data file in a large table. Building an index for all of them
     * makes snapshot expiration use memory proportional to the table size (and, when tags are read
     * concurrently, to the number of tags). Only files that are candidates for the current
     * expiration batch can be deleted, so merge only matching tag entries instead.
     */
    public Predicate<ExpireFileEntry> createDataFileSkipperForTag(
            Snapshot tag, Collection<DataFileDeletionPlan> plans) throws Exception {
        Map<BinaryRow, Map<Integer, Set<String>>> candidates = new HashMap<>();
        for (DataFileDeletionPlan plan : plans) {
            for (Pair<ExpireFileEntry, List<Path>> pair : plan.dataFileToDelete.values()) {
                addDataFile(candidates, pair.getLeft());
            }
        }
        if (candidates.isEmpty()) {
            return entry -> false;
        }

        Collection<ExpireFileEntry> matchingEntries =
                readMergedDataFiles(
                        manifestList.readDataManifests(tag),
                        createCandidateBucketFilter(candidates),
                        entry -> containsDataFile(candidates, entry));
        Map<BinaryRow, Map<Integer, Set<String>>> taggedCandidates = new HashMap<>();
        for (ExpireFileEntry entry : matchingEntries) {
            addDataFile(taggedCandidates, entry);
        }
        return entry -> containsDataFile(taggedCandidates, entry);
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
            addDataFile(dataFiles, entry);
        }
    }

    protected Collection<ExpireFileEntry> readMergedDataFiles(List<ManifestFileMeta> manifests)
            throws IOException {
        Map<Identifier, ExpireFileEntry> map = new HashMap<>();
        FileEntry.mergeEntries(readExpireFileEntries(manifests), map);
        return map.values();
    }

    protected Collection<ExpireFileEntry> readMergedDataFiles(
            List<ManifestFileMeta> manifests,
            BucketFilter bucketFilter,
            Predicate<ProjectedManifestEntry> filter)
            throws IOException {
        Map<Identifier, ExpireFileEntry> map = new HashMap<>();
        FileEntry.mergeEntries(
                ManifestReadThreadPool.sequentialBatchedExecute(
                        manifest -> {
                            if (!bucketFilter.mayContain(manifest)) {
                                return Collections.emptyList();
                            }
                            return manifestFile.readExpireFileEntries(
                                    manifest.fileName(), bucketFilter, filter);
                        },
                        manifests,
                        manifestReadParallelism),
                map);
        return map.values();
    }

    private BucketFilter createCandidateBucketFilter(
            Map<BinaryRow, Map<Integer, Set<String>>> candidates) {
        NavigableSet<Integer> candidateBuckets = new TreeSet<>();
        for (Map<Integer, Set<String>> buckets : candidates.values()) {
            candidateBuckets.addAll(buckets.keySet());
        }

        ManifestBucketFilter filter =
                new ManifestBucketFilter() {
                    @Override
                    public boolean test(BinaryRow partition, Integer bucket, Integer totalBuckets) {
                        Map<Integer, Set<String>> buckets = candidates.get(partition);
                        return buckets != null && buckets.containsKey(bucket);
                    }

                    @Override
                    public boolean mayContain(int minBucket, int maxBucket, int totalBuckets) {
                        Integer firstCandidate = candidateBuckets.ceiling(minBucket);
                        return firstCandidate != null && firstCandidate <= maxBucket;
                    }
                };
        return new BucketFilter(false, null, null, filter);
    }

    private void addDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ExpireFileEntry entry) {
        dataFiles
                .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                .add(entry.fileName());
    }

    protected boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ExpireFileEntry entry) {
        return containsDataFile(dataFiles, entry.partition(), entry.bucket(), entry.fileName());
    }

    private boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ProjectedManifestEntry entry) {
        return containsDataFile(dataFiles, entry.partition(), entry.bucket(), entry.fileName());
    }

    private boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles,
            BinaryRow partition,
            int bucket,
            String fileName) {
        Map<Integer, Set<String>> buckets = dataFiles.get(partition);
        if (buckets != null) {
            Set<String> fileNames = buckets.get(bucket);
            if (fileNames != null) {
                return fileNames.contains(fileName);
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
                            ManifestReadThreadPool.getExecutorService(manifestReadParallelism)));
        }

        Set<String> skippingSet = new HashSet<>();
        for (CompletableFuture<Set<String>> future : futures) {
            try {
                skippingSet.addAll(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }
        return skippingSet;
    }

    private Set<String> manifestSkippingSet(Snapshot skippingSnapshot) {
        Set<String> skippingSet = new HashSet<>();

        // data manifests
        skippingSet.add(skippingSnapshot.baseManifestList());
        skippingSet.add(skippingSnapshot.deltaManifestList());
        manifestList
                .readDataManifests(skippingSnapshot)
                .forEach(manifest -> addManifestToSkippingSet(skippingSet, manifest));

        // index manifests
        String indexManifest = skippingSnapshot.indexManifest();
        if (indexManifest != null) {
            skippingSet.add(indexManifest);
            indexFileHandler.readManifest(indexManifest).stream()
                    .map(IndexManifestEntry::indexFile)
                    .map(IndexFileMeta::fileName)
                    .forEach(skippingSet::add);
        }

        String reassignPlan = DataEvolutionRowIdAssignment.planFile(skippingSnapshot);
        if (reassignPlan != null) {
            skippingSet.add(reassignPlan);
        }

        // statistics
        if (skippingSnapshot.statistics() != null) {
            skippingSet.add(skippingSnapshot.statistics());
        }

        return skippingSet;
    }

    protected static void addManifestToSkippingSet(
            Set<String> skippingSet, ManifestFileMeta manifest) {
        skippingSet.add(manifest.fileName());
        if (manifest.extraFiles() != null) {
            skippingSet.addAll(manifest.extraFiles());
        }
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

    protected <F> void executeAll(Collection<F> files, Consumer<F> consumer) {
        List<Runnable> tasks = new ArrayList<>(files.size());
        for (F f : files) {
            tasks.add(() -> consumer.accept(f));
        }
        executeAll(tasks);
    }

    public void executeAll(Collection<Runnable> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(tasks.size());
        for (Runnable runnable : tasks) {
            futures.add(CompletableFuture.runAsync(runnable, fileExecutor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /** Candidate data files from one snapshot delta manifest. */
    public static class DataFileDeletionPlan {

        private final Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete;

        private DataFileDeletionPlan(
                Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete) {
            this.dataFileToDelete = dataFileToDelete;
        }

        private static DataFileDeletionPlan empty() {
            return new DataFileDeletionPlan(Collections.emptyMap());
        }
    }
}
