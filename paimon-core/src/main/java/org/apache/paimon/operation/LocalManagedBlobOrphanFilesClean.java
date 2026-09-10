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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;
import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/** Local {@link ManagedBlobOrphanFilesClean}. */
public class LocalManagedBlobOrphanFilesClean extends ManagedBlobOrphanFilesClean
        implements AutoCloseable {

    /**
     * Upper bound for waiting cancelled table cleanups after a database-wide failure. {@code
     * shutdownNow()} only requests interruption; a FileIO call may ignore it until a socket
     * timeout. Waiting forever would hide the original failure.
     */
    private static final long TERMINATION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(120);

    private final ThreadPoolExecutor executor;

    public LocalManagedBlobOrphanFilesClean(
            FileStoreTable table, long olderThanMillis, boolean dryRun) {
        super(table, olderThanMillis, dryRun);
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().fileOperationThreadNum(),
                        "MANAGED_BLOB_ORPHAN_FILES_CLEAN");
    }

    public CleanOrphanFilesResult clean() throws IOException {
        List<Path> deleteFiles = new ArrayList<>();
        long deletedFilesLenInBytes = 0;
        Map<String, Pair<Path, Long>> candidates = getCandidatePacks();
        if (candidates.isEmpty()) {
            return new CleanOrphanFilesResult(0, 0, deleteFiles);
        }
        if (candidates.containsKey(SKIP_MANAGED_BLOB_GC)) {
            LOG.warn(
                    "Skip managed blob pack GC for table {} because a listed pack path cannot be resolved safely.",
                    table.fullName());
            return new CleanOrphanFilesResult(0, 0, deleteFiles);
        }

        List<String> topologyBefore = snapshotTopology();
        Set<String> usedPacks = collectUsedPacks();
        betweenUsedCollections();
        Set<String> usedPacks2 = collectUsedPacks();
        if (shouldAbortPackGc(topologyBefore, usedPacks, usedPacks2)) {
            return new CleanOrphanFilesResult(0, 0, deleteFiles);
        }

        for (Map.Entry<String, Pair<Path, Long>> candidate : candidates.entrySet()) {
            throwIfInterrupted();
            if (usedPacks2.contains(candidate.getKey())) {
                continue;
            }
            Pair<Path, Long> info = candidate.getValue();
            if (cleanManagedBlobFile(info.getLeft())) {
                deletedFilesLenInBytes += info.getRight();
                deleteFiles.add(info.getLeft());
            }
        }

        throwIfInterrupted();
        if (!dryRun) {
            cleanEmptyDataDirectory(deleteFiles);
        }
        return new CleanOrphanFilesResult(deleteFiles.size(), deletedFilesLenInBytes, deleteFiles);
    }

    private static void throwIfInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Interrupted while cleaning managed blob orphan files.");
        }
    }

    @Override
    protected Set<String> collectUsedPacks() {
        ReachabilityScan scan = newReachabilityScan();
        return validBranches().stream()
                .flatMap(branch -> getUsedPacks(branch, scan).stream())
                .collect(Collectors.toSet());
    }

    private Set<String> getUsedPacks(String branch, ReachabilityScan scan) {
        Set<String> used = ConcurrentHashMap.newKeySet();
        try {
            executeSnapshotsInCompletionOrder(
                    executor,
                    snapshot -> {
                        try {
                            emitUsedPacks(branch, snapshot, scan, used::add);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    safelyGetAllSnapshots(branch));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return used;
    }

    /**
     * Waits in completion order and cancels remaining snapshot tasks on the first failure so a
     * later failed snapshot is not hidden by an earlier one stuck in uninterruptible I/O.
     *
     * <p>{@link Future#cancel(boolean)} only requests interruption, so a task blocked in a FileIO
     * call can keep running after this method throws. This does not leak: every caller reaches
     * {@link #close()} (directly, or via {@link #executeDatabase}, which additionally awaits
     * termination up to {@link #TERMINATION_TIMEOUT_MS}), and that shuts the pool down. Any new
     * caller must preserve that guarantee.
     */
    static <U> void executeSnapshotsInCompletionOrder(
            ExecutorService executor, Consumer<U> processor, Collection<U> input) {
        if (input.isEmpty()) {
            return;
        }
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<Future<Void>> futures = new ArrayList<>(input.size());
        for (U u : input) {
            futures.add(
                    completionService.submit(
                            () -> {
                                Thread.currentThread().setContextClassLoader(cl);
                                processor.accept(u);
                                return null;
                            }));
        }
        try {
            for (int i = 0; i < futures.size(); i++) {
                completionService.take().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelAll(futures);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            cancelAll(futures);
            throw new RuntimeException(e);
        }
    }

    private static void cancelAll(List<? extends Future<?>> futures) {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private Map<String, Pair<Path, Long>> getCandidatePacks() {
        List<Path> fileDirs = listPaimonFileDirs();
        Iterator<Pair<Path, Long>> packs =
                randomlyExecuteSequentialReturn(executor, packLister(), fileDirs);
        Map<String, Pair<Path, Long>> result = new HashMap<>();
        while (packs.hasNext()) {
            Pair<Path, Long> fileInfo = packs.next();
            Optional<String> identity = packIdentityForCleanup(fileInfo.getLeft());
            if (!identity.isPresent()) {
                result.clear();
                result.put(SKIP_MANAGED_BLOB_GC, fileInfo);
                return result;
            }
            result.put(identity.get(), fileInfo);
        }
        return result;
    }

    private Function<Path, List<Pair<Path, Long>>> packLister() {
        return path ->
                tryBestListingDirs(path).stream()
                        .filter(status -> !status.isDir())
                        .filter(this::oldEnough)
                        .filter(status -> isManagedBlobPackName(status.getPath().getName()))
                        .map(status -> Pair.of(status.getPath(), status.getLen()))
                        .collect(Collectors.toList());
    }

    private void cleanEmptyDataDirectory(List<Path> deleted) {
        if (deleted.isEmpty()) {
            return;
        }
        Set<Path> bucketDirs =
                deleted.stream()
                        .map(Path::getParent)
                        .filter(path -> path.toString().contains(BUCKET_PATH_PREFIX))
                        .collect(Collectors.toSet());
        randomlyOnlyExecute(executor, this::tryDeleteEmptyDirectory, bucketDirs);
        Set<Path> partitionDirs =
                bucketDirs.stream().map(Path::getParent).collect(Collectors.toSet());
        tryCleanDataDirectory(partitionDirs, partitionKeysNum);
    }

    public static List<LocalManagedBlobOrphanFilesClean> createCleans(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            @Nullable Integer parallelism,
            boolean dryRun)
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
                                        CoreOptions.FILE_OPERATION_THREAD_NUM.key(),
                                        parallelism.toString());
                            }
                        };

        List<LocalManagedBlobOrphanFilesClean> cleans = new ArrayList<>(tableNames.size());
        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier).copy(dynamicOptions);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-blobs action. The table type is '%s'.",
                    table.getClass().getName());
            cleans.add(
                    new LocalManagedBlobOrphanFilesClean(
                            (FileStoreTable) table, olderThanMillis, dryRun));
        }
        return cleans;
    }

    public static CleanOrphanFilesResult executeDatabase(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            @Nullable Integer parallelism,
            boolean dryRun)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<LocalManagedBlobOrphanFilesClean> tableCleans =
                createCleans(
                        catalog, databaseName, tableName, olderThanMillis, parallelism, dryRun);
        ExecutorService executorService =
                Executors.newFixedThreadPool(
                        Runtime.getRuntime().availableProcessors(),
                        newDaemonThreadFactory("MANAGED-BLOB-ORPHAN-DB-CLEAN"));
        return executeDatabase(tableCleans, executorService, TERMINATION_TIMEOUT_MS);
    }

    static CleanOrphanFilesResult executeDatabase(
            List<LocalManagedBlobOrphanFilesClean> tableCleans, ExecutorService executorService) {
        return executeDatabase(tableCleans, executorService, TERMINATION_TIMEOUT_MS);
    }

    static CleanOrphanFilesResult executeDatabase(
            List<LocalManagedBlobOrphanFilesClean> tableCleans,
            ExecutorService executorService,
            long terminationTimeoutMs) {
        List<Future<CleanOrphanFilesResult>> tasks = new ArrayList<>(tableCleans.size());
        CompletionService<CleanOrphanFilesResult> completionService =
                new ExecutorCompletionService<>(executorService);
        try {
            for (LocalManagedBlobOrphanFilesClean clean : tableCleans) {
                tasks.add(completionService.submit(clean::clean));
            }

            long deletedFileCount = 0;
            long deletedFileTotalLenInBytes = 0;
            for (int i = 0; i < tasks.size(); i++) {
                try {
                    Future<CleanOrphanFilesResult> task = completionService.take();
                    CleanOrphanFilesResult result = task.get();
                    deletedFileCount += result.getDeletedFileCount();
                    deletedFileTotalLenInBytes += result.getDeletedFileTotalLenInBytes();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            return new CleanOrphanFilesResult(deletedFileCount, deletedFileTotalLenInBytes);
        } finally {
            for (Future<CleanOrphanFilesResult> task : tasks) {
                task.cancel(true);
            }
            for (LocalManagedBlobOrphanFilesClean clean : tableCleans) {
                clean.close();
            }
            executorService.shutdownNow();

            List<ExecutorService> toAwait = new ArrayList<>(tableCleans.size() + 1);
            toAwait.add(executorService);
            for (LocalManagedBlobOrphanFilesClean clean : tableCleans) {
                toAwait.add(clean.executor);
            }

            boolean restoreInterrupt = Thread.interrupted();
            restoreInterrupt |= awaitTermination(terminationTimeoutMs, toAwait);
            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits until every executor terminates or {@code timeoutMs} elapses, whichever is first. Does
     * not throw, so a database-wide failure still surfaces after a stuck FileIO call.
     */
    private static boolean awaitTermination(
            long timeoutMs, List<ExecutorService> executorServices) {
        boolean interrupted = false;
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        long startNanos = System.nanoTime();
        for (ExecutorService executorService : executorServices) {
            long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
            if (remainingNanos <= 0) {
                LOG.warn(
                        "Timed out waiting for managed blob orphan cleanup executors to terminate. "
                                + "A leftover FileIO call may still be running.");
                break;
            }
            try {
                if (!executorService.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    LOG.warn(
                            "Timed out waiting for managed blob orphan cleanup executors to terminate. "
                                    + "A leftover FileIO call may still be running.");
                    break;
                }
            } catch (InterruptedException e) {
                interrupted = true;
                break;
            }
        }
        return interrupted;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
