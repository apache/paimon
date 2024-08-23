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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecute;

/**
 * To remove the data files and metadata files that are not used by table (so-called "orphan
 * files").
 *
 * <p>It will ignore exception when listing all files because it's OK to not delete unread files.
 *
 * <p>To avoid deleting newly written files, it only deletes orphan files older than 1 day by
 * default. The interval can be modified by {@link #olderThan}.
 *
 * <p>To avoid conflicting with snapshot expiration, tag deletion and rollback, it will skip the
 * snapshot/tag when catching {@link FileNotFoundException} in the process of listing used files.
 *
 * <p>To avoid deleting files that are used but not read by mistaken, it will stop removing process
 * when failed to read used files.
 */
public class OrphanFilesClean {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanFilesClean.class);

    private final ThreadPoolExecutor executor;

    private static final int READ_FILE_RETRY_NUM = 3;
    private static final int READ_FILE_RETRY_INTERVAL = 5;
    private static final int SHOW_LIMIT = 200;

    private final FileStoreTable table;
    private final FileIO fileIO;
    private final Path location;
    private final int partitionKeysNum;

    private final List<Path> deleteFiles;
    private long olderThanMillis = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    private Consumer<Path> fileCleaner;

    public OrphanFilesClean(FileStoreTable table) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.location = table.location();
        this.partitionKeysNum = table.partitionKeys().size();

        this.deleteFiles = new ArrayList<>();
        this.fileCleaner =
                path -> {
                    try {
                        if (fileIO.isDir(path)) {
                            fileIO.deleteDirectoryQuietly(path);
                        } else {
                            fileIO.deleteQuietly(path);
                        }
                    } catch (IOException ignored) {
                    }
                };
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().deleteFileThreadNum(), "ORPHAN_FILES_CLEAN");
    }

    public OrphanFilesClean olderThan(String timestamp) {
        // The FileStatus#getModificationTime returns milliseconds
        this.olderThanMillis =
                DateTimeUtils.parseTimestampData(timestamp, 3, TimeZone.getDefault())
                        .getMillisecond();
        return this;
    }

    public OrphanFilesClean fileCleaner(Consumer<Path> fileCleaner) {
        this.fileCleaner = fileCleaner;
        return this;
    }

    public List<Path> clean() throws IOException, ExecutionException, InterruptedException {
        List<String> branches = table.branchManager().branches();
        branches.add(BranchManager.DEFAULT_MAIN_BRANCH);

        List<String> abnormalBranches = new ArrayList<>();
        for (String branch : branches) {
            if (!new SchemaManager(table.fileIO(), table.location(), branch).latest().isPresent()) {
                abnormalBranches.add(branch);
            }
        }
        if (!abnormalBranches.isEmpty()) {
            LOG.warn(
                    "Branches {} have no schemas. Orphan files cleaning aborted. "
                            + "Please check these branches manually.",
                    abnormalBranches);
            return Collections.emptyList();
        }

        Map<String, Path> candidates = getCandidateDeletingFiles();
        Set<String> usedFiles = new HashSet<>();

        for (String branch : branches) {
            FileStoreTable branchTable = table.switchToBranch(branch);
            SnapshotManager snapshotManager = branchTable.snapshotManager();

            // specially handle the snapshot directory
            List<Path> nonSnapshotFiles = snapshotManager.tryGetNonSnapshotFiles(this::oldEnough);
            nonSnapshotFiles.forEach(fileCleaner);
            deleteFiles.addAll(nonSnapshotFiles);

            // specially handle the changelog directory
            List<Path> nonChangelogFiles = snapshotManager.tryGetNonChangelogFiles(this::oldEnough);
            nonChangelogFiles.forEach(fileCleaner);
            deleteFiles.addAll(nonChangelogFiles);

            usedFiles.addAll(getUsedFiles(branchTable));
        }

        Set<String> deleted = new HashSet<>(candidates.keySet());
        deleted.removeAll(usedFiles);
        deleted.stream().map(candidates::get).forEach(fileCleaner);

        deleteFiles.addAll(deleted.stream().map(candidates::get).collect(Collectors.toList()));
        return deleteFiles;
    }

    /** Get all the files used by snapshots and tags. */
    private Set<String> getUsedFiles(FileStoreTable branchTable) throws IOException {
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        TagManager tagManager = branchTable.tagManager();

        // safely get all snapshots to be read
        Set<Snapshot> readSnapshots = new HashSet<>(snapshotManager.safelyGetAllSnapshots());
        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();
        readSnapshots.addAll(taggedSnapshots);
        readSnapshots.addAll(snapshotManager.safelyGetAllChangelogs());

        return Sets.newHashSet(
                randomlyExecute(
                        executor, snapshot -> getUsedFiles(branchTable, snapshot), readSnapshots));
    }

    private List<String> getUsedFiles(FileStoreTable branchTable, Snapshot snapshot) {
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        ManifestFile manifestFile = branchTable.store().manifestFileFactory().create();

        if (snapshot instanceof Changelog) {
            return getUsedFilesForChangelog(manifestList, manifestFile, (Changelog) snapshot);
        } else {
            return getUsedFilesForSnapshot(
                    manifestList,
                    manifestFile,
                    branchTable.store().newIndexFileHandler(),
                    snapshot);
        }
    }

    /**
     * Get all the candidate deleting files in the specified directories and filter them by
     * olderThanMillis.
     */
    private Map<String, Path> getCandidateDeletingFiles() {
        List<Path> fileDirs = listPaimonFileDirs();
        Function<Path, List<Path>> processor =
                path ->
                        tryBestListingDirs(path).stream()
                                .filter(this::oldEnough)
                                .map(FileStatus::getPath)
                                .collect(Collectors.toList());
        Iterator<Path> allPaths = randomlyExecute(executor, processor, fileDirs);
        Map<String, Path> result = new HashMap<>();
        while (allPaths.hasNext()) {
            Path next = allPaths.next();
            result.put(next.getName(), next);
        }
        return result;
    }

    private List<String> getUsedFilesForChangelog(
            ManifestList manifestList, ManifestFile manifestFile, Changelog changelog) {
        List<String> files = new ArrayList<>();
        List<ManifestFileMeta> manifestFileMetas = new ArrayList<>();
        try {
            // try to read manifests
            // changelog manifest
            List<ManifestFileMeta> changelogManifest = new ArrayList<>();
            if (changelog.changelogManifestList() != null) {
                files.add(changelog.changelogManifestList());
                changelogManifest =
                        retryReadingFiles(
                                () ->
                                        manifestList.readWithIOException(
                                                changelog.changelogManifestList()));
                if (changelogManifest != null) {
                    manifestFileMetas.addAll(changelogManifest);
                }
            }

            // base manifest
            if (manifestList.exists(changelog.baseManifestList())) {
                files.add(changelog.baseManifestList());
                List<ManifestFileMeta> baseManifest =
                        retryReadingFiles(
                                () ->
                                        manifestList.readWithIOException(
                                                changelog.baseManifestList()));
                if (baseManifest != null) {
                    manifestFileMetas.addAll(baseManifest);
                }
            }

            // delta manifest
            List<ManifestFileMeta> deltaManifest = null;
            if (manifestList.exists(changelog.deltaManifestList())) {
                files.add(changelog.deltaManifestList());
                deltaManifest =
                        retryReadingFiles(
                                () ->
                                        manifestList.readWithIOException(
                                                changelog.deltaManifestList()));
                if (deltaManifest != null) {
                    manifestFileMetas.addAll(deltaManifest);
                }
            }

            files.addAll(
                    manifestFileMetas.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList()));

            // data file
            List<String> manifestFileName = new ArrayList<>();
            if (changelog.changelogManifestList() != null) {
                manifestFileName.addAll(
                        changelogManifest == null
                                ? new ArrayList<>()
                                : changelogManifest.stream()
                                        .map(ManifestFileMeta::fileName)
                                        .collect(Collectors.toList()));
            } else {
                manifestFileName.addAll(
                        deltaManifest == null
                                ? new ArrayList<>()
                                : deltaManifest.stream()
                                        .map(ManifestFileMeta::fileName)
                                        .collect(Collectors.toList()));
            }

            // try to read data files
            List<String> dataFiles = retryReadingDataFiles(manifestFile, manifestFileName);
            if (dataFiles == null) {
                return Collections.emptyList();
            }
            files.addAll(dataFiles);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    /**
     * If getting null when reading some files, the snapshot/tag is being deleted, so just return an
     * empty result.
     */
    private List<String> getUsedFilesForSnapshot(
            ManifestList manifestList,
            ManifestFile manifestFile,
            IndexFileHandler indexFileHandler,
            Snapshot snapshot) {
        List<String> files = new ArrayList<>();
        addManifestList(files, snapshot);

        try {
            // try to read manifests
            List<ManifestFileMeta> manifestFileMetas =
                    retryReadingFiles(
                            () -> readAllManifestsWithIOException(manifestList, snapshot));
            if (manifestFileMetas == null) {
                return Collections.emptyList();
            }
            List<String> manifestFileName =
                    manifestFileMetas.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList());
            files.addAll(manifestFileName);

            // try to read data files
            List<String> dataFiles = retryReadingDataFiles(manifestFile, manifestFileName);
            if (dataFiles == null) {
                return Collections.emptyList();
            }
            files.addAll(dataFiles);

            // try to read index files
            String indexManifest = snapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                files.add(indexManifest);

                List<IndexManifestEntry> indexManifestEntries =
                        retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));
                if (indexManifestEntries == null) {
                    return Collections.emptyList();
                }

                indexManifestEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(IndexFileMeta::fileName)
                        .forEach(files::add);
            }

            // try to read statistic
            if (snapshot.statistics() != null) {
                files.add(snapshot.statistics());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    private void addManifestList(List<String> used, Snapshot snapshot) {
        used.add(snapshot.baseManifestList());
        used.add(snapshot.deltaManifestList());
        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            used.add(changelogManifestList);
        }
    }

    /**
     * Retry reading files when {@link IOException} was thrown by the reader. If the exception is
     * {@link FileNotFoundException}, return null. Finally, if retry times reaches the limits,
     * rethrow the IOException.
     */
    @Nullable
    private <T> T retryReadingFiles(ReaderWithIOException<T> reader) throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) {
            try {
                return reader.read();
            } catch (FileNotFoundException e) {
                return null;
            } catch (IOException e) {
                caught = e;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_FILE_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        throw caught;
    }

    private List<ManifestFileMeta> readAllManifestsWithIOException(
            ManifestList manifestList, Snapshot snapshot) throws IOException {
        List<ManifestFileMeta> result = new ArrayList<>();

        result.addAll(manifestList.readWithIOException(snapshot.baseManifestList()));
        result.addAll(manifestList.readWithIOException(snapshot.deltaManifestList()));

        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            result.addAll(manifestList.readWithIOException(changelogManifestList));
        }

        return result;
    }

    @Nullable
    private List<String> retryReadingDataFiles(
            ManifestFile manifestFile, List<String> manifestNames) throws IOException {
        List<String> dataFiles = new ArrayList<>();
        for (String manifestName : manifestNames) {
            List<ManifestEntry> manifestEntries =
                    retryReadingFiles(() -> manifestFile.readWithIOException(manifestName));
            if (manifestEntries == null) {
                return null;
            }

            manifestEntries.stream()
                    .map(ManifestEntry::file)
                    .forEach(
                            f -> {
                                dataFiles.add(f.fileName());
                                dataFiles.addAll(f.extraFiles());
                            });
        }
        return dataFiles;
    }

    /** List directories that contains data files and manifest files. */
    private List<Path> listPaimonFileDirs() {
        List<Path> paimonFileDirs = new ArrayList<>();

        paimonFileDirs.add(new Path(location, "manifest"));
        paimonFileDirs.add(new Path(location, "index"));
        paimonFileDirs.add(new Path(location, "statistics"));
        paimonFileDirs.addAll(listAndCleanDataDirs(location, partitionKeysNum));

        return paimonFileDirs;
    }

    /**
     * If failed to list directory, just return an empty result because it's OK to not delete them.
     */
    private List<FileStatus> tryBestListingDirs(Path dir) {
        try {
            if (!fileIO.exists(dir)) {
                return Collections.emptyList();
            }

            List<FileStatus> status =
                    retryReadingFiles(
                            () -> {
                                FileStatus[] s = fileIO.listStatus(dir);
                                return s == null ? Collections.emptyList() : Arrays.asList(s);
                            });
            return status == null ? Collections.emptyList() : status;
        } catch (IOException e) {
            LOG.debug("Failed to list directory {}, skip it.", dir, e);
            return Collections.emptyList();
        }
    }

    private boolean oldEnough(FileStatus status) {
        return status.getModificationTime() < olderThanMillis;
    }

    /**
     * List directories that contains data files and may clean non Paimon data dirs/files. The
     * argument level is used to control recursive depth.
     */
    private List<Path> listAndCleanDataDirs(Path dir, int level) {
        List<FileStatus> dirs = tryBestListingDirs(dir);

        if (level == 0) {
            // return bucket paths
            return filterAndCleanDataDirs(
                    dirs,
                    p -> p.getName().startsWith(BUCKET_PATH_PREFIX),
                    // if buckets are under partition, we can do clean
                    partitionKeysNum -> partitionKeysNum != 0);
        }

        List<Path> partitionPaths =
                filterAndCleanDataDirs(
                        dirs,
                        p -> p.getName().contains("="),
                        // if partitions are under a parent partition, we can do clean
                        partitionKeysNum -> level != partitionKeysNum);

        // dive into the next partition level
        return Lists.newArrayList(
                randomlyExecute(executor, p -> listAndCleanDataDirs(p, level - 1), partitionPaths));
    }

    private List<Path> filterAndCleanDataDirs(
            List<FileStatus> statuses, Predicate<Path> filter, Predicate<Integer> cleanCondition) {
        List<Path> filtered = new ArrayList<>();
        List<FileStatus> mayBeClean = new ArrayList<>();

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (filter.test(path)) {
                filtered.add(path);
            } else {
                mayBeClean.add(status);
            }
        }

        if (cleanCondition.test(partitionKeysNum)) {
            mayBeClean.stream()
                    .filter(this::oldEnough)
                    .map(FileStatus::getPath)
                    .forEach(
                            p -> {
                                fileCleaner.accept(p);
                                synchronized (deleteFiles) {
                                    deleteFiles.add(p);
                                }
                            });
        }

        return filtered;
    }

    /** A helper functional interface for method {@link #retryReadingFiles}. */
    @FunctionalInterface
    private interface ReaderWithIOException<T> {

        T read() throws IOException;
    }

    public static List<String> showDeletedFiles(List<Path> deleteFiles, int showLimit) {
        int showSize = Math.min(deleteFiles.size(), showLimit);
        List<String> result = new ArrayList<>();
        if (deleteFiles.size() > showSize) {
            result.add(
                    String.format(
                            "Total %s files, only %s lines are displayed.",
                            deleteFiles.size(), showSize));
        }
        for (int i = 0; i < showSize; i++) {
            result.add(deleteFiles.get(i).toUri().getPath());
        }
        return result;
    }

    public static List<OrphanFilesClean> createOrphanFilesCleans(
            Catalog catalog,
            Map<String, String> tableConfig,
            String databaseName,
            @Nullable String tableName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<OrphanFilesClean> orphanFilesCleans = new ArrayList<>();
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier).copy(tableConfig);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            orphanFilesCleans.add(new OrphanFilesClean((FileStoreTable) table));
        }

        return orphanFilesCleans;
    }

    public static String[] executeOrphanFilesClean(List<OrphanFilesClean> tableCleans) {
        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<List<Path>>> tasks = new ArrayList<>();
        for (OrphanFilesClean clean : tableCleans) {
            tasks.add(executorService.submit(clean::clean));
        }

        List<Path> cleanOrphanFiles = new ArrayList<>();
        for (Future<List<Path>> task : tasks) {
            try {
                cleanOrphanFiles.addAll(task.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        executorService.shutdownNow();
        return showDeletedFiles(cleanOrphanFiles, SHOW_LIMIT).toArray(new String[0]);
    }
}
