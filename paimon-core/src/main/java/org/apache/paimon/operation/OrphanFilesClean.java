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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.SerializableConsumer;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/**
 * To remove the data files and metadata files that are not used by table (so-called "orphan
 * files").
 *
 * <p>It will ignore exception when listing all files because it's OK to not delete unread files.
 *
 * <p>To avoid deleting newly written files, it only deletes orphan files older than {@code
 * olderThanMillis} (1 day by default).
 *
 * <p>To avoid conflicting with snapshot expiration, tag deletion and rollback, it will skip the
 * snapshot/tag when catching {@link FileNotFoundException} in the process of listing used files.
 *
 * <p>To avoid deleting files that are used but not read by mistaken, it will stop removing process
 * when failed to read used files.
 */
public abstract class OrphanFilesClean implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(OrphanFilesClean.class);

    protected static final int READ_FILE_RETRY_NUM = 3;
    protected static final int READ_FILE_RETRY_INTERVAL = 5;

    protected final FileStoreTable table;
    protected final FileIO fileIO;
    protected final long olderThanMillis;
    protected final SerializableConsumer<Path> fileCleaner;
    protected final int partitionKeysNum;
    protected final Path location;

    public OrphanFilesClean(
            FileStoreTable table, long olderThanMillis, SerializableConsumer<Path> fileCleaner) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.partitionKeysNum = table.partitionKeys().size();
        this.location = table.location();
        this.olderThanMillis = olderThanMillis;
        this.fileCleaner = fileCleaner;
    }

    protected List<String> validBranches() {
        List<String> branches = table.branchManager().branches();
        branches.add(BranchManager.DEFAULT_MAIN_BRANCH);

        List<String> abnormalBranches = new ArrayList<>();
        for (String branch : branches) {
            if (!new SchemaManager(table.fileIO(), table.location(), branch).latest().isPresent()) {
                abnormalBranches.add(branch);
            }
        }
        if (!abnormalBranches.isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Branches %s have no schemas. Orphan files cleaning aborted. "
                                    + "Please check these branches manually.",
                            abnormalBranches));
        }
        return branches;
    }

    protected void cleanSnapshotDir(List<String> branches, Consumer<Path> deletedFileConsumer) {
        for (String branch : branches) {
            FileStoreTable branchTable = table.switchToBranch(branch);
            SnapshotManager snapshotManager = branchTable.snapshotManager();

            // specially handle the snapshot directory
            List<Path> nonSnapshotFiles = snapshotManager.tryGetNonSnapshotFiles(this::oldEnough);
            nonSnapshotFiles.forEach(fileCleaner);
            nonSnapshotFiles.forEach(deletedFileConsumer);

            // specially handle the changelog directory
            List<Path> nonChangelogFiles = snapshotManager.tryGetNonChangelogFiles(this::oldEnough);
            nonChangelogFiles.forEach(fileCleaner);
            nonChangelogFiles.forEach(deletedFileConsumer);
        }
    }

    protected void collectWithoutDataFile(
            String branch,
            Consumer<String> usedFileConsumer,
            Consumer<ManifestFileMeta> manifestConsumer)
            throws IOException {
        for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
            collectWithoutDataFile(branch, snapshot, usedFileConsumer, manifestConsumer);
        }
    }

    protected Set<Snapshot> safelyGetAllSnapshots(String branch) throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        TagManager tagManager = branchTable.tagManager();
        Set<Snapshot> readSnapshots = new HashSet<>(snapshotManager.safelyGetAllSnapshots());
        readSnapshots.addAll(tagManager.taggedSnapshots());
        readSnapshots.addAll(snapshotManager.safelyGetAllChangelogs());
        return readSnapshots;
    }

    protected void collectWithoutDataFile(
            String branch,
            Snapshot snapshot,
            Consumer<String> usedFileConsumer,
            Consumer<ManifestFileMeta> manifestConsumer)
            throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        IndexFileHandler indexFileHandler = branchTable.store().newIndexFileHandler();
        List<ManifestFileMeta> manifestFileMetas = new ArrayList<>();
        // changelog manifest
        if (snapshot.changelogManifestList() != null) {
            usedFileConsumer.accept(snapshot.changelogManifestList());
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () ->
                                    manifestList.readWithIOException(
                                            snapshot.changelogManifestList()),
                            emptyList()));
        }

        // delta manifest
        if (snapshot.deltaManifestList() != null) {
            usedFileConsumer.accept(snapshot.deltaManifestList());
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () -> manifestList.readWithIOException(snapshot.deltaManifestList()),
                            emptyList()));
        }

        // base manifest
        usedFileConsumer.accept(snapshot.baseManifestList());
        manifestFileMetas.addAll(
                retryReadingFiles(
                        () -> manifestList.readWithIOException(snapshot.baseManifestList()),
                        emptyList()));

        // collect manifests
        for (ManifestFileMeta manifest : manifestFileMetas) {
            manifestConsumer.accept(manifest);
            usedFileConsumer.accept(manifest.fileName());
        }

        // index files
        String indexManifest = snapshot.indexManifest();
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            usedFileConsumer.accept(indexManifest);
            retryReadingFiles(
                            () -> indexFileHandler.readManifestWithIOException(indexManifest),
                            Collections.<IndexManifestEntry>emptyList())
                    .stream()
                    .map(IndexManifestEntry::indexFile)
                    .map(IndexFileMeta::fileName)
                    .forEach(usedFileConsumer);
        }

        // statistic file
        if (snapshot.statistics() != null) {
            usedFileConsumer.accept(snapshot.statistics());
        }
    }

    /** List directories that contains data files and manifest files. */
    protected List<Path> listPaimonFileDirs() {
        List<Path> paimonFileDirs = new ArrayList<>();

        paimonFileDirs.add(new Path(location, "manifest"));
        paimonFileDirs.add(new Path(location, "index"));
        paimonFileDirs.add(new Path(location, "statistics"));
        paimonFileDirs.addAll(listFileDirs(location, partitionKeysNum));

        return paimonFileDirs;
    }

    /**
     * List directories that contains data files. The argument level is used to control recursive
     * depth.
     */
    private List<Path> listFileDirs(Path dir, int level) {
        List<FileStatus> dirs = tryBestListingDirs(dir);

        if (level == 0) {
            // return bucket paths
            return filterDirs(dirs, p -> p.getName().startsWith(BUCKET_PATH_PREFIX));
        }

        List<Path> partitionPaths = filterDirs(dirs, p -> p.getName().contains("="));

        List<Path> result = new ArrayList<>();
        for (Path partitionPath : partitionPaths) {
            result.addAll(listFileDirs(partitionPath, level - 1));
        }
        return result;
    }

    private List<Path> filterDirs(List<FileStatus> statuses, Predicate<Path> filter) {
        List<Path> filtered = new ArrayList<>();

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (filter.test(path)) {
                filtered.add(path);
            }
            // ignore unknown dirs
        }

        return filtered;
    }

    /**
     * If failed to list directory, just return an empty result because it's OK to not delete them.
     */
    protected List<FileStatus> tryBestListingDirs(Path dir) {
        try {
            if (!fileIO.exists(dir)) {
                return emptyList();
            }

            return retryReadingFiles(
                    () -> {
                        FileStatus[] s = fileIO.listStatus(dir);
                        return s == null ? emptyList() : Arrays.asList(s);
                    },
                    emptyList());
        } catch (IOException e) {
            LOG.debug("Failed to list directory {}, skip it.", dir, e);
            return emptyList();
        }
    }

    /**
     * Retry reading files when {@link IOException} was thrown by the reader. If the exception is
     * {@link FileNotFoundException}, return default value. Finally, if retry times reaches the
     * limits, rethrow the IOException.
     */
    protected static <T> T retryReadingFiles(ReaderWithIOException<T> reader, T defaultValue)
            throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) {
            try {
                return reader.read();
            } catch (FileNotFoundException e) {
                return defaultValue;
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

    protected boolean oldEnough(FileStatus status) {
        return status.getModificationTime() < olderThanMillis;
    }

    /** A helper functional interface for method {@link #retryReadingFiles}. */
    @FunctionalInterface
    protected interface ReaderWithIOException<T> {

        T read() throws IOException;
    }

    public static SerializableConsumer<Path> createFileCleaner(
            Catalog catalog, @Nullable Boolean dryRun) {
        SerializableConsumer<Path> fileCleaner;
        if (Boolean.TRUE.equals(dryRun)) {
            fileCleaner = path -> {};
        } else {
            FileIO fileIO = catalog.fileIO();
            fileCleaner =
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
        }
        return fileCleaner;
    }

    public static long olderThanMillis(@Nullable String olderThan) {
        return isNullOrWhitespaceOnly(olderThan)
                ? System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)
                : DateTimeUtils.parseTimestampData(olderThan, 3, TimeZone.getDefault())
                        .getMillisecond();
    }
}
