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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SerializableConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecute;

/** Local {@link OrphanFilesClean}, it will use thread pool to execute deletion. */
public class LocalOrphanFilesClean extends OrphanFilesClean {

    private final ThreadPoolExecutor executor;

    private static final int SHOW_LIMIT = 200;

    private final List<Path> deleteFiles;

    public LocalOrphanFilesClean(FileStoreTable table) {
        this(table, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    }

    public LocalOrphanFilesClean(FileStoreTable table, long olderThanMillis) {
        this(table, olderThanMillis, path -> table.fileIO().deleteQuietly(path));
    }

    public LocalOrphanFilesClean(
            FileStoreTable table, long olderThanMillis, SerializableConsumer<Path> fileCleaner) {
        super(table, olderThanMillis, fileCleaner);
        this.deleteFiles = new ArrayList<>();
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().deleteFileThreadNum(), "ORPHAN_FILES_CLEAN");
    }

    public List<Path> clean() throws IOException, ExecutionException, InterruptedException {
        List<String> branches = validBranches();

        // specially handle to clear snapshot dir
        cleanSnapshotDir(branches, deleteFiles::add);

        // delete candidate files
        Map<String, Path> candidates = getCandidateDeletingFiles();

        // find used files
        Set<String> usedFiles =
                branches.stream()
                        .flatMap(branch -> getUsedFiles(branch).stream())
                        .collect(Collectors.toSet());

        // delete unused files
        Set<String> deleted = new HashSet<>(candidates.keySet());
        deleted.removeAll(usedFiles);
        deleted.stream().map(candidates::get).forEach(fileCleaner);
        deleteFiles.addAll(deleted.stream().map(candidates::get).collect(Collectors.toList()));

        return deleteFiles;
    }

    private List<String> getUsedFiles(String branch) {
        List<String> usedFiles = new ArrayList<>();
        ManifestFile manifestFile =
                table.switchToBranch(branch).store().manifestFileFactory().create();
        try {
            List<String> manifests = new ArrayList<>();
            collectWithoutDataFile(
                    branch, usedFiles::add, manifest -> manifests.add(manifest.fileName()));
            usedFiles.addAll(retryReadingDataFiles(manifestFile, manifests));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return usedFiles;
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

    private List<String> retryReadingDataFiles(
            ManifestFile manifestFile, List<String> manifestNames) throws IOException {
        List<String> dataFiles = new ArrayList<>();
        for (String manifestName : manifestNames) {
            retryReadingFiles(
                            () -> manifestFile.readWithIOException(manifestName),
                            Collections.<ManifestEntry>emptyList())
                    .stream()
                    .map(ManifestEntry::file)
                    .forEach(
                            f -> {
                                dataFiles.add(f.fileName());
                                dataFiles.addAll(f.extraFiles());
                            });
        }
        return dataFiles;
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

    public static List<LocalOrphanFilesClean> createOrphanFilesCleans(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<LocalOrphanFilesClean> orphanFilesCleans = new ArrayList<>();
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        Map<String, String> dynamicOptions =
                parallelism == null
                        ? Collections.emptyMap()
                        : new HashMap<String, String>() {
                            {
                                put(
                                        CoreOptions.DELETE_FILE_THREAD_NUM.key(),
                                        parallelism.toString());
                            }
                        };

        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier).copy(dynamicOptions);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            orphanFilesCleans.add(
                    new LocalOrphanFilesClean(
                            (FileStoreTable) table, olderThanMillis, fileCleaner));
        }

        return orphanFilesCleans;
    }

    public static String[] executeOrphanFilesClean(List<LocalOrphanFilesClean> tableCleans) {
        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<List<Path>>> tasks = new ArrayList<>();
        for (LocalOrphanFilesClean clean : tableCleans) {
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
