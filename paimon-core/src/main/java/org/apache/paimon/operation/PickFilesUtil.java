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
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;

/** util class for get all files of a snapshot/tag/table. */
public class PickFilesUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PickFilesUtil.class);

    private static final int READ_FILE_RETRY_NUM = 3;
    private static final int READ_FILE_RETRY_INTERVAL = 5;

    /** Get table all files in the specified directories. */
    public static Map<String, Pair<Path, Long>> getTableAllFiles(
            Path tableRoot, int partitionKeysNum, FileIO fileIO) {
        List<Path> fileDirs = listPaimonFileDirs(tableRoot, partitionKeysNum, fileIO);
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
                                            .map(
                                                    fileStatus ->
                                                            Pair.of(
                                                                    fileStatus.getPath(),
                                                                    fileStatus.getLen()))
                                            .collect(
                                                    Collectors.toMap(
                                                            pair -> pair.getLeft().getName(),
                                                            Function.identity())))
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            LOG.debug("Failed to get candidate deleting files.", e);
            return Collections.emptyMap();
        }
    }

    /** List directories that contains data files and manifest files. */
    public static List<Path> listPaimonFileDirs(
            Path tableRoot, int partitionKeysNum, FileIO fileIO) {
        List<Path> paimonFileDirs = new ArrayList<>();

        paimonFileDirs.add(new Path(tableRoot, "snapshot"));
        paimonFileDirs.add(new Path(tableRoot, "changelog"));
        paimonFileDirs.add(new Path(tableRoot, "tag"));
        paimonFileDirs.add(new Path(tableRoot, "schema"));
        paimonFileDirs.add(new Path(tableRoot, "manifest"));
        paimonFileDirs.add(new Path(tableRoot, "index"));
        paimonFileDirs.add(new Path(tableRoot, "statistics"));
        paimonFileDirs.addAll(listDataDirs(tableRoot, partitionKeysNum, fileIO));

        return paimonFileDirs;
    }

    public static List<Path> filterDataDirs(List<FileStatus> statuses, Predicate<Path> filter) {
        List<Path> filtered = new ArrayList<>();

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (filter.test(path)) {
                filtered.add(path);
            }
        }

        return filtered;
    }

    /**
     * List directories that contains data files and may clean non Paimon data dirs/files. The
     * argument level is used to control recursive depth.
     */
    public static List<Path> listDataDirs(Path dir, int level, FileIO fileIO) {
        List<FileStatus> dirs = PickFilesUtil.tryBestListingDirs(dir, fileIO);

        if (level == 0) {
            // return bucket paths
            return filterDataDirs(dirs, p -> p.getName().startsWith(BUCKET_PATH_PREFIX));
        }

        List<Path> partitionPaths = filterDataDirs(dirs, p -> p.getName().contains("="));

        // dive into the next partition level
        try {
            return FileUtils.COMMON_IO_FORK_JOIN_POOL
                    .submit(
                            () ->
                                    partitionPaths
                                            .parallelStream()
                                            .flatMap(
                                                    p ->
                                                            listDataDirs(p, level - 1, fileIO)
                                                                    .stream())
                                            .collect(Collectors.toList()))
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            LOG.debug("Failed to list partition directory {}", dir, e);
            return Collections.emptyList();
        }
    }

    /**
     * If failed to list directory, just return an empty result because it's OK to not delete them.
     */
    public static List<FileStatus> tryBestListingDirs(Path dir, FileIO fileIO) {
        try {
            if (!fileIO.exists(dir)) {
                return Collections.emptyList();
            }

            List<FileStatus> status =
                    PickFilesUtil.retryReadingFiles(
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

    public static List<String> getUsedFilesForChangelog(
            Snapshot snapshot,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager) {
        List<String> files = new ArrayList<>();
        if (snapshot.changelogManifestList() != null) {
            files.add(snapshot.changelogManifestList());
        }

        try {
            // try to read manifests
            List<ManifestFileMeta> manifestFileMetas =
                    retryReadingFiles(
                            () ->
                                    manifestList.readWithIOException(
                                            snapshot.changelogManifestList()));
            if (manifestFileMetas == null) {
                return Collections.emptyList();
            }
            List<String> manifestFileName =
                    manifestFileMetas.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList());
            files.addAll(manifestFileName);

            // try to read data files
            List<String> dataFiles = retryReadingDataFiles(manifestFileName, manifestFile);
            if (dataFiles == null) {
                return Collections.emptyList();
            }
            files.addAll(dataFiles);

            // Changelog has no index files and statistic file

            // add schema file
            files.add(schemaManager.schemaFileName(snapshot.schemaId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return files;
    }

    public static List<String> getUsedFilesForTag(
            Snapshot snapshot,
            TagManager tagManager,
            String tagName,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler) {
        List<String> files = new ArrayList<>();
        files.add(tagManager.tagPath(tagName).getName());
        files.addAll(
                getUsedFilesInternal(
                        snapshot, manifestList, manifestFile, schemaManager, indexFileHandler));
        return files;
    }

    /**
     * If getting null when reading some files, the snapshot/tag is being deleted, so just return an
     * empty result.
     */
    public static List<String> getUsedFilesForSnapshot(
            Snapshot snapshot,
            SnapshotManager snapshotManager,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler) {
        List<String> files = new ArrayList<>();
        files.add(snapshotManager.snapshotPath(snapshot.id()).getName());
        files.addAll(
                getUsedFilesInternal(
                        snapshot, manifestList, manifestFile, schemaManager, indexFileHandler));
        return files;
    }

    /**
     * If getting null when reading some files, the snapshot/tag is being deleted, so just return an
     * empty result.
     */
    public static List<String> getUsedFilesInternal(
            Snapshot snapshot,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler) {
        List<String> files = new ArrayList<>();
        addManifestList(files, snapshot);

        try {
            // try to read manifests
            List<ManifestFileMeta> manifestFileMetas =
                    retryReadingFiles(
                            () -> readAllManifestsWithIOException(snapshot, manifestList));
            if (manifestFileMetas == null) {
                return Collections.emptyList();
            }
            List<String> manifestFileName =
                    manifestFileMetas.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList());
            files.addAll(manifestFileName);

            // try to read data files
            List<String> dataFiles = retryReadingDataFiles(manifestFileName, manifestFile);
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

            // add statistic file
            if (snapshot.statistics() != null) {
                files.add(snapshot.statistics());
            }

            // add schema file
            files.add(schemaManager.schemaFileName(snapshot.schemaId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    public static void addManifestList(List<String> used, Snapshot snapshot) {
        used.add(snapshot.baseManifestList());
        used.add(snapshot.deltaManifestList());
        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            used.add(changelogManifestList);
        }
    }

    public static List<ManifestFileMeta> readAllManifestsWithIOException(
            Snapshot snapshot, ManifestList manifestList) throws IOException {
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
    public static List<String> retryReadingDataFiles(
            List<String> manifestNames, ManifestFile manifestFile) throws IOException {
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

    /**
     * Retry reading files when {@link IOException} was thrown by the reader. If the exception is
     * {@link FileNotFoundException}, return null. Finally, if retry times reaches the limits,
     * rethrow the IOException.
     */
    @Nullable
    public static <T> T retryReadingFiles(OrphanFilesClean.ReaderWithIOException<T> reader)
            throws IOException {
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
}
