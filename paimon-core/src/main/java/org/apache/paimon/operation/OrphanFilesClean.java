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
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;

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

    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    private final TagManager tagManager;
    private final FileIO fileIO;
    private final Path location;
    private final int partitionKeysNum;
    private final ManifestList manifestList;
    private final ManifestFile manifestFile;
    private final IndexFileHandler indexFileHandler;

    // an estimated value of how many files were deleted
    private int deletedFilesNum = 0;
    private final List<Path> deleteFiles;
    private long olderThanMillis = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);

    public OrphanFilesClean(FileStoreTable table) {
        this.snapshotManager = table.snapshotManager();
        this.tagManager = table.tagManager();
        this.fileIO = table.fileIO();
        this.location = table.location();
        this.partitionKeysNum = table.partitionKeys().size();
        this.schemaManager = new SchemaManager(fileIO, location);

        FileStore<?> store = table.store();
        this.manifestList = store.manifestListFactory().create();
        this.manifestFile = store.manifestFileFactory().create();
        this.indexFileHandler = store.newIndexFileHandler();
        this.deleteFiles = new ArrayList<>();
    }

    public OrphanFilesClean olderThan(String timestamp) {
        // The FileStatus#getModificationTime returns milliseconds
        this.olderThanMillis =
                DateTimeUtils.parseTimestampData(timestamp, 3, DateTimeUtils.LOCAL_TZ)
                        .getMillisecond();
        return this;
    }

    public int clean() throws IOException, ExecutionException, InterruptedException {
        if (snapshotManager.earliestSnapshotId() == null) {
            LOG.info("No snapshot found, skip removing.");
            return 0;
        }

        // specially handle the snapshot directory
        List<Path> nonSnapshotFiles = snapshotManager.tryGetNonSnapshotFiles(this::oldEnough);
        nonSnapshotFiles.forEach(this::deleteFileOrDirQuietly);
        deletedFilesNum += nonSnapshotFiles.size();
        deleteFiles.addAll(nonSnapshotFiles);

        // specially handle the changelog directory
        List<Path> nonChangelogFiles = snapshotManager.tryGetNonChangelogFiles(this::oldEnough);
        nonChangelogFiles.forEach(this::deleteFileOrDirQuietly);
        deletedFilesNum += nonChangelogFiles.size();
        deleteFiles.addAll(nonChangelogFiles);

        Map<String, Path> candidates = getCandidateDeletingFiles();
        Set<String> usedFiles = getUsedFiles();

        Set<String> deleted = new HashSet<>(candidates.keySet());
        deleted.removeAll(usedFiles);

        for (String file : deleted) {
            Path path = candidates.get(file);
            deleteFileOrDirQuietly(path);
        }
        deletedFilesNum += deleted.size();
        deleteFiles.addAll(deleted.stream().map(candidates::get).collect(Collectors.toList()));

        return deletedFilesNum;
    }

    @VisibleForTesting
    List<Path> getDeleteFiles() {
        return deleteFiles;
    }

    /** Get all the files used by snapshots and tags. */
    private Set<String> getUsedFiles()
            throws IOException, ExecutionException, InterruptedException {
        // safely get all snapshots to be read
        Set<Snapshot> readSnapshots = new HashSet<>(snapshotManager.safelyGetAllSnapshots());
        readSnapshots.addAll(tagManager.taggedSnapshots());
        readSnapshots.addAll(snapshotManager.safelyGetAllChangelogs());

        return FileUtils.COMMON_IO_FORK_JOIN_POOL
                .submit(
                        () ->
                                readSnapshots
                                        .parallelStream()
                                        .flatMap(
                                                snapshot -> {
                                                    if (snapshot instanceof Changelog) {
                                                        return PickFilesUtil
                                                                .getUsedFilesForChangelog(
                                                                        snapshot,
                                                                        manifestList,
                                                                        manifestFile,
                                                                        schemaManager)
                                                                .stream();
                                                    } else {
                                                        return PickFilesUtil
                                                                .getUsedFilesForSnapshot(
                                                                        snapshot,
                                                                        snapshotManager,
                                                                        manifestList,
                                                                        manifestFile,
                                                                        schemaManager,
                                                                        indexFileHandler)
                                                                .stream();
                                                    }
                                                })
                                        .collect(Collectors.toSet()))
                .get();
    }

    /**
     * Get all the candidate deleting files in the specified directories and filter them by
     * olderThanMillis.
     */
    private Map<String, Path> getCandidateDeletingFiles() {
        List<Path> fileDirs = listPaimonFileDirs();
        try {
            return FileUtils.COMMON_IO_FORK_JOIN_POOL
                    .submit(
                            () ->
                                    fileDirs.parallelStream()
                                            .flatMap(
                                                    p ->
                                                            PickFilesUtil.tryBestListingDirs(
                                                                    p, fileIO)
                                                                    .stream())
                                            .filter(this::oldEnough)
                                            .map(FileStatus::getPath)
                                            .collect(
                                                    Collectors.toMap(
                                                            Path::getName, Function.identity())))
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            LOG.debug("Failed to get candidate deleting files.", e);
            return Collections.emptyMap();
        }
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

    private boolean oldEnough(FileStatus status) {
        return status.getModificationTime() < olderThanMillis;
    }

    /**
     * List directories that contains data files and may clean non Paimon data dirs/files. The
     * argument level is used to control recursive depth.
     */
    private List<Path> listAndCleanDataDirs(Path dir, int level) {
        List<FileStatus> dirs = PickFilesUtil.tryBestListingDirs(dir, fileIO);

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
        try {
            return FileUtils.COMMON_IO_FORK_JOIN_POOL
                    .submit(
                            () ->
                                    partitionPaths
                                            .parallelStream()
                                            .flatMap(
                                                    p ->
                                                            listAndCleanDataDirs(p, level - 1)
                                                                    .stream())
                                            .collect(Collectors.toList()))
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            LOG.debug("Failed to list partition directory {}", dir, e);
            return Collections.emptyList();
        }
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
                                deleteFileOrDirQuietly(p);
                                synchronized (deleteFiles) {
                                    deleteFiles.add(p);
                                    deletedFilesNum++;
                                }
                            });
        }

        return filtered;
    }

    private void deleteFileOrDirQuietly(Path path) {
        try {
            if (fileIO.isDir(path)) {
                fileIO.deleteDirectoryQuietly(path);
            } else {
                fileIO.deleteQuietly(path);
            }
        } catch (IOException ignored) {
        }
    }

    /** A helper functional interface for method {@link PickFilesUtil}. */
    @FunctionalInterface
    interface ReaderWithIOException<T> {
        T read() throws IOException;
    }
}
