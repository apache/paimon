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

package org.apache.paimon.lookup.sort.db;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Coordinates asynchronous compaction, failure propagation, backpressure, and shutdown. */
class AsyncLsmCompactor {

    private final LsmLevels levels;
    private final LsmCompactor compactor;
    private final ExecutorService executor;
    private final int levelZeroFileCountTrigger;
    private final LsmCompactor.FileSupplier fileSupplier;
    private final LsmCompactor.FileDeleter fileDeleter;
    private final ThreadLocal<List<File>> deferredCompactionDeletes;

    @Nullable private Future<?> compactionFuture;

    AsyncLsmCompactor(
            LsmLevels levels,
            CompactorFactory compactorFactory,
            ExecutorService executor,
            int levelZeroFileCountTrigger,
            LsmCompactor.FileSupplier fileSupplier,
            LsmCompactor.FileDeleter fileDeleter) {
        this.levels = levels;
        this.executor = executor;
        this.levelZeroFileCountTrigger = levelZeroFileCountTrigger;
        this.fileSupplier = fileSupplier;
        this.fileDeleter = fileDeleter;
        this.deferredCompactionDeletes = new ThreadLocal<>();
        this.compactor = compactorFactory.create(this::deferOrDeleteCompactedFile);
    }

    void scheduleIfNeeded() throws IOException {
        checkFailure();
        if (!needsCompaction() || compactionFuture != null) {
            return;
        }

        try {
            compactionFuture =
                    executor.submit(
                            () -> {
                                while (needsCompaction()) {
                                    compactLevelSnapshot(false);
                                }
                                return null;
                            });
        } catch (RuntimeException e) {
            throw new IOException("Failed to schedule background compaction.", e);
        }
    }

    void applyBackpressure() throws IOException {
        if ((long) levels.fileCount(0) >= (long) levelZeroFileCountTrigger * 2) {
            await();
        }
    }

    void fullCompact() throws IOException {
        checkFailure();
        await();
        compactLevelSnapshot(true);
    }

    void checkFailure() throws IOException {
        Future<?> future = compactionFuture;
        if (future != null && future.isDone()) {
            awaitFuture(future);
            clearFuture(future);
        }
    }

    void await() throws IOException {
        Future<?> future = compactionFuture;
        if (future == null) {
            return;
        }
        awaitFuture(future);
        clearFuture(future);
    }

    void close() throws IOException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            throw new IOException("Interrupted while closing.", e);
        }
    }

    private boolean needsCompaction() {
        return levels.needsCompaction(levelZeroFileCountTrigger);
    }

    private void compactLevelSnapshot(boolean fullCompaction) throws IOException {
        List<List<SstFileMetadata>> originalLevels = levels.snapshot();
        if (!fullCompaction && originalLevels.get(0).size() < levelZeroFileCountTrigger) {
            return;
        }

        List<List<SstFileMetadata>> compactedLevels = LsmLevels.copy(originalLevels);
        List<File> generatedFiles = new ArrayList<>();
        List<File> compactedFiles = new ArrayList<>();
        boolean published = false;
        deferredCompactionDeletes.set(compactedFiles);
        try {
            LsmCompactor.FileSupplier trackingFileSupplier =
                    () -> {
                        File file = fileSupplier.newSstFile();
                        generatedFiles.add(file);
                        return file;
                    };
            if (fullCompaction) {
                compactor.fullCompact(compactedLevels, levels.maxLevels(), trackingFileSupplier);
            } else {
                compactor.maybeCompact(compactedLevels, levels.maxLevels(), trackingFileSupplier);
            }
            levels.publishCompaction(originalLevels, compactedLevels, compactedFiles, fileDeleter);
            published = true;
        } finally {
            deferredCompactionDeletes.remove();
            if (!published) {
                for (File generatedFile : generatedFiles) {
                    fileDeleter.deleteFile(generatedFile);
                }
            }
        }
    }

    private void deferOrDeleteCompactedFile(File file) {
        List<File> deferredDeletes = deferredCompactionDeletes.get();
        if (deferredDeletes != null) {
            deferredDeletes.add(file);
        } else {
            fileDeleter.deleteFile(file);
        }
    }

    private void clearFuture(Future<?> future) {
        if (compactionFuture == future) {
            compactionFuture = null;
        }
    }

    private static void awaitFuture(Future<?> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for background compaction.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Background compaction failed.", cause);
        } catch (CancellationException e) {
            throw new IOException("Background compaction was cancelled.", e);
        }
    }

    /** Creates the compactor with the coordinated file-deletion callback. */
    interface CompactorFactory {

        LsmCompactor create(LsmCompactor.FileDeleter fileDeleter);
    }
}
