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

package org.apache.paimon.flink.clone;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SupplierWithIOException;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Util class for get used files' paths of a table's latest snapshot. */
public class CloneFilesUtil {

    private static final int READ_FILE_RETRY_NUM = 3;
    private static final int READ_FILE_RETRY_INTERVAL = 5;

    /**
     * Retrieves a map of schema file types to their corresponding list of file paths for a given
     * snapshot。 The schema file types include: Snapshot, Schema, ManifestList, StatisticFile and
     * IndexFile .
     *
     * @param table The FileStoreTable object representing the table.
     * @param snapshotId The ID of the snapshot to retrieve files for.
     * @return A map where the key is the FileType and the value is a list of file paths.
     * @throws FileNotFoundException If the snapshot file is not found.
     */
    public static Map<FileType, List<Path>> getSchemaUsedFilesForSnapshot(
            FileStoreTable table, long snapshotId) throws IOException {
        FileStore<?> store = table.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();
        Map<FileType, List<Path>> filesMap = new HashMap<>();
        if (snapshot != null) {
            FileStorePathFactory pathFactory = store.pathFactory();
            // 1. add the Snapshot file
            filesMap.computeIfAbsent(FileType.SNAPSHOT_FILE, k -> new ArrayList<>())
                    .add(snapshotManager.snapshotPath(snapshotId));
            // 2. add the ManifestList files
            addManifestList(filesMap, snapshot, pathFactory);

            // 3. try to read index files
            String indexManifest = snapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                filesMap.computeIfAbsent(FileType.INDEX_FILE, k -> new ArrayList<>())
                        .add(pathFactory.indexManifestFileFactory().toPath(indexManifest));

                List<IndexManifestEntry> indexManifestEntries =
                        retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));
                if (indexManifestEntries != null) {
                    indexManifestEntries.stream()
                            .map(IndexManifestEntry::indexFile)
                            .map(indexFileHandler::filePath)
                            .forEach(
                                    filePath ->
                                            filesMap.computeIfAbsent(
                                                            FileType.INDEX_FILE,
                                                            k -> new ArrayList<>())
                                                    .add(filePath));
                }
            }

            // 4. add statistic file
            if (snapshot.statistics() != null) {
                filesMap.computeIfAbsent(FileType.STATISTICS_FILE, k -> new ArrayList<>())
                        .add(pathFactory.statsFileFactory().toPath(snapshot.statistics()));
            }
        }

        // 5. add the Schema files
        for (long id : schemaManager.listAllIds()) {
            filesMap.computeIfAbsent(FileType.SCHEMA_FILE, k -> new ArrayList<>())
                    .add(schemaManager.toSchemaPath(id));
        }

        return filesMap;
    }

    /**
     * Retrieves a map of data file types to their corresponding list of file paths for a given
     * snapshot. The data file types include: DataFile and ChangelogFile.
     *
     * @param table The FileStoreTable object representing the table.
     * @param snapshotId The ID of the snapshot to retrieve files for.
     * @return A map where the key is the FileType and the value is a list of file paths. the pair
     *     is the data file's absolute path and data file's relative path.
     * @throws FileNotFoundException If the snapshot file is not found.
     */
    public static Map<FileType, List<Pair<Path, Path>>> getDataUsedFilesForSnapshot(
            FileStoreTable table, long snapshotId) throws FileNotFoundException {
        FileStore<?> store = table.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
        Map<FileType, List<Pair<Path, Path>>> filesMap = new HashMap<>();
        if (snapshot != null) {
            // try to read data files
            List<Pair<Path, Path>> dataFiles = new ArrayList<>();
            List<SimpleFileEntry> simpleFileEntries =
                    store.newScan().withSnapshot(snapshot).readSimpleEntries();
            for (SimpleFileEntry simpleFileEntry : simpleFileEntries) {
                FileStorePathFactory fileStorePathFactory = store.pathFactory();
                Path dataFilePath =
                        fileStorePathFactory
                                .createDataFilePathFactory(
                                        simpleFileEntry.partition(), simpleFileEntry.bucket())
                                .toPath(simpleFileEntry);
                Path relativeBucketPath =
                        fileStorePathFactory.relativeBucketPath(
                                simpleFileEntry.partition(), simpleFileEntry.bucket());
                Path relativeTablePath = new Path("/" + relativeBucketPath, dataFilePath.getName());
                dataFiles.add(Pair.of(dataFilePath, relativeTablePath));
            }

            // When scanning, dataFiles are listed from older to newer.
            // By reversing dataFiles, newer files will be copied first.
            //
            // We do this because new files are from the latest partition, and are prone to be
            // deleted. Older files however, are from previous partitions and should not be changed
            // very often.
            Collections.reverse(dataFiles);
            filesMap.computeIfAbsent(FileType.DATA_FILE, k -> new ArrayList<>()).addAll(dataFiles);
        }
        return filesMap;
    }

    /**
     * Retrieves a map of manifest file types to their corresponding list of file paths for a given
     * snapshot. The manifest file types include: ManifestFile.
     *
     * @param table The FileStoreTable object representing the table.
     * @param snapshotId The ID of the snapshot to retrieve files for.
     * @return A map where the key is the FileType and the value is a list of file paths.
     * @throws FileNotFoundException If the snapshot file is not found.
     */
    public static Map<FileType, List<Path>> getManifestUsedFilesForSnapshot(
            FileStoreTable table, long snapshotId) throws IOException {
        FileStore<?> store = table.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
        ManifestList manifestList = store.manifestListFactory().create();
        Map<FileType, List<Path>> filesMap = new HashMap<>();
        // try to read manifests
        List<ManifestFileMeta> manifestFileMetas =
                retryReadingFiles(() -> readAllManifestsWithIOException(snapshot, manifestList));
        if (manifestFileMetas == null) {
            return filesMap;
        }
        List<String> manifestFileName =
                manifestFileMetas.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toList());
        filesMap.computeIfAbsent(FileType.MANIFEST_FILE, k -> new ArrayList<>())
                .addAll(
                        manifestFileName.stream()
                                .map(store.pathFactory()::toManifestFilePath)
                                .collect(Collectors.toList()));
        return filesMap;
    }

    private static void addManifestList(
            Map<FileType, List<Path>> filesMap,
            Snapshot snapshot,
            FileStorePathFactory pathFactory) {
        filesMap.computeIfAbsent(FileType.MANIFEST_LIST_FILE, k -> new ArrayList<>())
                .add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
        filesMap.get(FileType.MANIFEST_LIST_FILE)
                .add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));
        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            filesMap.computeIfAbsent(FileType.CHANGELOG_MANIFEST_LIST_FILE, k -> new ArrayList<>())
                    .add(pathFactory.toManifestListPath(changelogManifestList));
        }
    }

    private static List<ManifestFileMeta> readAllManifestsWithIOException(
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
    private static <T> T retryReadingFiles(SupplierWithIOException<T> reader) throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) {
            try {
                return reader.get();
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

    public static List<CloneFileInfo> toCloneFileInfos(
            Map<FileType, List<Path>> filesMap,
            Path sourceTableRoot,
            String sourceIdentifier,
            String targetIdentifier,
            long snapshotId) {
        List<CloneFileInfo> result = new ArrayList<>();
        for (Map.Entry<FileType, List<Path>> entry : filesMap.entrySet()) {
            for (Path file : entry.getValue()) {
                Path relativePath = getPathExcludeTableRoot(file, sourceTableRoot);
                result.add(
                        new CloneFileInfo(
                                file.toUri().toString(),
                                relativePath.toString(),
                                sourceIdentifier,
                                targetIdentifier,
                                entry.getKey(),
                                snapshotId));
            }
        }
        return result;
    }

    public static List<CloneFileInfo> toCloneFileInfos(
            Map<FileType, List<Pair<Path, Path>>> filesMap,
            String sourceIdentifier,
            String targetIdentifier,
            long snapshotId) {
        List<CloneFileInfo> result = new ArrayList<>();
        for (Map.Entry<FileType, List<Pair<Path, Path>>> entry : filesMap.entrySet()) {
            for (Pair<Path, Path> file : entry.getValue()) {
                result.add(
                        new CloneFileInfo(
                                file.getLeft().toUri().toString(),
                                file.getRight().toString(),
                                sourceIdentifier,
                                targetIdentifier,
                                entry.getKey(),
                                snapshotId));
            }
        }
        return result;
    }

    public static Path getPathExcludeTableRoot(Path absolutePath, Path sourceTableRoot) {
        String fileAbsolutePath = absolutePath.toUri().toString();
        String sourceTableRootPath = sourceTableRoot.toString();

        Preconditions.checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "File absolute path does not start with source table root path. This is unexpected. "
                        + "fileAbsolutePath is: "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is: "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }
}
