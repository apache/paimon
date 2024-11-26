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
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Local {@link OrphanFilesClean}, it will use thread pool to execute deletion.
 *
 * <p>Note that, this class will be used when the orphan clean mode is local, else orphan clean will
 * use distributed one. See `FlinkOrphanFilesClean` and `SparkOrphanFilesClean`.
 */
public class LocalOrphanFilesClean extends OrphanFilesClean {

    private final ThreadPoolExecutor executor;

    private final List<Path> deleteFiles;

    private final AtomicLong deletedFilesLenInBytes = new AtomicLong(0);

    private Set<String> candidateDeletes;

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

    public CleanOrphanFilesResult clean()
            throws IOException, ExecutionException, InterruptedException {
        List<String> branches = validBranches();

        // specially handle to clear snapshot dir
        cleanSnapshotDir(branches, deleteFiles::add, deletedFilesLenInBytes::addAndGet);

        // delete candidate files
        Map<String, Pair<Path, Long>> candidates = getCandidateDeletingFiles();
        if (candidates.isEmpty()) {
            return new CleanOrphanFilesResult(
                    deleteFiles, deleteFiles.size(), deletedFilesLenInBytes.get());
        }
        candidateDeletes = new HashSet<>(candidates.keySet());

        // find used files
        Set<String> usedFiles =
                branches.stream()
                        .flatMap(branch -> getUsedFiles(branch).stream())
                        .collect(Collectors.toSet());

        // delete unused files
        candidateDeletes.removeAll(usedFiles);
        candidateDeletes.stream()
                .map(candidates::get)
                .forEach(
                        deleteFileInfo -> {
                            deletedFilesLenInBytes.addAndGet(deleteFileInfo.getRight());
                            fileCleaner.accept(deleteFileInfo.getLeft());
                        });
        deleteFiles.addAll(
                candidateDeletes.stream()
                        .map(candidates::get)
                        .map(Pair::getLeft)
                        .collect(Collectors.toList()));
        candidateDeletes.clear();

        return new CleanOrphanFilesResult(
                deleteFiles, deleteFiles.size(), deletedFilesLenInBytes.get());
    }

    private void collectWithoutDataFile(
            String branch, Consumer<String> usedFileConsumer, Consumer<String> manifestConsumer)
            throws IOException {
        randomlyOnlyExecute(
                executor,
                snapshot -> {
                    try {
                        collectWithoutDataFile(
                                branch, snapshot, usedFileConsumer, manifestConsumer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                safelyGetAllSnapshots(branch));
    }

    private Set<String> getUsedFiles(String branch) {
        Set<String> usedFiles = ConcurrentHashMap.newKeySet();
        ManifestFile manifestFile =
                table.switchToBranch(branch).store().manifestFileFactory().create();
        try {
            Set<String> manifests = ConcurrentHashMap.newKeySet();
            collectWithoutDataFile(branch, usedFiles::add, manifests::add);
            randomlyOnlyExecute(
                    executor,
                    manifestName -> {
                        try {
                            retryReadingFiles(
                                            () -> manifestFile.readWithIOException(manifestName),
                                            Collections.<ManifestEntry>emptyList())
                                    .stream()
                                    .map(ManifestEntry::file)
                                    .forEach(
                                            f -> {
                                                if (candidateDeletes.contains(f.fileName())) {
                                                    usedFiles.add(f.fileName());
                                                }
                                                f.extraFiles().stream()
                                                        .filter(candidateDeletes::contains)
                                                        .forEach(usedFiles::add);
                                            });
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    manifests);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return usedFiles;
    }

    /**
     * Get all the candidate deleting files in the specified directories and filter them by
     * olderThanMillis.
     */
    private Map<String, Pair<Path, Long>> getCandidateDeletingFiles() {
        List<Path> fileDirs = listPaimonFileDirs();
        Function<Path, List<Pair<Path, Long>>> processor =
                path ->
                        tryBestListingDirs(path).stream()
                                .filter(this::oldEnough)
                                .map(status -> Pair.of(status.getPath(), status.getLen()))
                                .collect(Collectors.toList());
        Iterator<Pair<Path, Long>> allFilesInfo =
                randomlyExecuteSequentialReturn(executor, processor, fileDirs);
        Map<String, Pair<Path, Long>> result = new HashMap<>();
        while (allFilesInfo.hasNext()) {
            Pair<Path, Long> fileInfo = allFilesInfo.next();
            result.put(fileInfo.getLeft().getName(), fileInfo);
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

        List<LocalOrphanFilesClean> orphanFilesCleans = new ArrayList<>(tableNames.size());
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

    public static CleanOrphanFilesResult executeDatabaseOrphanFiles(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<LocalOrphanFilesClean> tableCleans =
                createOrphanFilesCleans(
                        catalog,
                        databaseName,
                        tableName,
                        olderThanMillis,
                        fileCleaner,
                        parallelism);

        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<CleanOrphanFilesResult>> tasks = new ArrayList<>(tableCleans.size());
        for (LocalOrphanFilesClean clean : tableCleans) {
            tasks.add(executorService.submit(clean::clean));
        }

        long deletedFileCount = 0;
        long deletedFileTotalLenInBytes = 0;
        for (Future<CleanOrphanFilesResult> task : tasks) {
            try {
                deletedFileCount += task.get().getDeletedFileCount();
                deletedFileTotalLenInBytes += task.get().getDeletedFileTotalLenInBytes();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        executorService.shutdownNow();
        return new CleanOrphanFilesResult(deletedFileCount, deletedFileTotalLenInBytes);
    }
}
