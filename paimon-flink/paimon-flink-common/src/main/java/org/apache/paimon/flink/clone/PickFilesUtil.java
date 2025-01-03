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
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Util class for get used files' paths of a table's latest snapshot. */
public class PickFilesUtil {

    private static final int READ_FILE_RETRY_NUM = 3;
    private static final int READ_FILE_RETRY_INTERVAL = 5;

    public static List<Path> getUsedFilesForLatestSnapshot(FileStoreTable table) {
        FileStore<?> store = table.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        Snapshot snapshot = snapshotManager.latestSnapshot();
        ManifestList manifestList = store.manifestListFactory().create();
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();

        List<Path> files = new ArrayList<>();
        if (snapshot != null) {
            files.add(snapshotManager.snapshotPath(snapshot.id()));
            files.addAll(
                    getUsedFilesInternal(
                            snapshot,
                            store.pathFactory(),
                            store.newScan(),
                            manifestList,
                            indexFileHandler));
        }
        for (long id : schemaManager.listAllIds()) {
            files.add(schemaManager.toSchemaPath(id));
        }
        return files;
    }

    private static List<Path> getUsedFilesInternal(
            Snapshot snapshot,
            FileStorePathFactory pathFactory,
            FileStoreScan scan,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler) {
        List<Path> files = new ArrayList<>();
        addManifestList(files, snapshot, pathFactory);

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
            files.addAll(
                    manifestFileName.stream()
                            .map(pathFactory::toManifestFilePath)
                            .collect(Collectors.toList()));

            // try to read data files
            List<Path> dataFiles = new ArrayList<>();
            List<SimpleFileEntry> simpleFileEntries =
                    scan.withSnapshot(snapshot).readSimpleEntries();
            for (SimpleFileEntry simpleFileEntry : simpleFileEntries) {
                Path dataFilePath =
                        pathFactory
                                .createDataFilePathFactory(
                                        simpleFileEntry.partition(), simpleFileEntry.bucket())
                                .toPath(simpleFileEntry);
                dataFiles.add(dataFilePath);
            }

            // When scanning, dataFiles are listed from older to newer.
            // By reversing dataFiles, newer files will be copied first.
            //
            // We do this because new files are from the latest partition, and are prone to be
            // deleted. Older files however, are from previous partitions and should not be changed
            // very often.
            Collections.reverse(dataFiles);
            files.addAll(dataFiles);

            // try to read index files
            String indexManifest = snapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                files.add(pathFactory.indexManifestFileFactory().toPath(indexManifest));

                List<IndexManifestEntry> indexManifestEntries =
                        retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));
                if (indexManifestEntries == null) {
                    return Collections.emptyList();
                }

                indexManifestEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(indexFileHandler::filePath)
                        .forEach(files::add);
            }

            // add statistic file
            if (snapshot.statistics() != null) {
                files.add(pathFactory.statsFileFactory().toPath(snapshot.statistics()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    private static void addManifestList(
            List<Path> used, Snapshot snapshot, FileStorePathFactory pathFactory) {
        used.add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
        used.add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));
        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            used.add(pathFactory.toManifestListPath(changelogManifestList));
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
    private static <T> T retryReadingFiles(ReaderWithIOException<T> reader) throws IOException {
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

    /** A helper functional interface for method {@link #retryReadingFiles}. */
    @FunctionalInterface
    interface ReaderWithIOException<T> {
        T read() throws IOException;
    }
}
