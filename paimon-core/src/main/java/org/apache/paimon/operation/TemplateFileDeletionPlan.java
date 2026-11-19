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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Batches the exact paths of template files recorded by DROP operations. */
public final class TemplateFileDeletionPlan {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateFileDeletionPlan.class);
    private static final int BATCH_SIZE = 1024;
    private static final String CLEANUP_DIRECTORY = ".paimon-template-drops";
    private static final String TABLE_PREFIX = "table-";
    private static final String SEALED = "sealed";
    private static final String TABLE_DROPPED = "table-dropped";

    private final FileIO fileIO;
    private final Path tablePath;
    private final Path plan;
    private final Set<Path> paths = new LinkedHashSet<>();
    private int batchCount;

    private TemplateFileDeletionPlan(FileIO fileIO, Path tablePath, Path directory, String prefix) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.plan = new Path(directory, prefix + UUID.randomUUID());
    }

    /** Collect a table's file paths under its own directory before the table is dropped. */
    public static TemplateFileDeletionPlan prepare(FileIO fileIO, Path tablePath) {
        return new TemplateFileDeletionPlan(
                fileIO, tablePath, tableCleanupPath(tablePath), TABLE_PREFIX);
    }

    void add(Path path) throws IOException {
        paths.add(path);
        if (paths.size() == BATCH_SIZE) {
            flush();
        }
    }

    /** Complete the plan before deleting any metadata. */
    public void seal() {
        try {
            flush();
            publish(fileIO, new Path(plan, SEALED), Integer.toString(batchCount));
        } catch (IOException e) {
            discard();
            throw new UncheckedIOException("Cannot seal file cleanup plan " + plan, e);
        }
    }

    void discard() {
        fileIO.deleteDirectoryQuietly(plan);
    }

    /**
     * Preserve the table candidates before DROP removes their table directory. Returns an archived
     * plan that can be confirmed after the catalog accepts the drop.
     */
    public TemplateFileDeletionPlan archiveForTableDrop() {
        TemplateFileDeletionPlan archived =
                new TemplateFileDeletionPlan(
                        fileIO, tablePath, archivePath(tablePath), TABLE_PREFIX);
        try {
            seal();
            copyBatches(fileIO, plan, archived);
            archived.seal();
            return archived;
        } catch (IOException | RuntimeException e) {
            archived.discard();
            throw new IllegalStateException("Cannot archive file cleanup plan " + plan, e);
        } finally {
            discard();
        }
    }

    /** Call only after the catalog has accepted the table drop. */
    public void confirmTableDrop(int threads) {
        try {
            publish(fileIO, new Path(plan, TABLE_DROPPED), tablePath.toString());
            reclaimTablePlan(fileIO, plan, threads);
        } catch (IOException | RuntimeException e) {
            LOG.warn("Cannot finish file cleanup for {}", plan, e);
        }
    }

    /** Retry confirmed table drops, including after the original table directory was removed. */
    public static void retryTableDrops(FileIO fileIO, Path tablePath, int threads) {
        try {
            Path root = archivePath(tablePath);
            if (fileIO.exists(root)) {
                for (FileStatus status : fileIO.listStatus(root)) {
                    Path plan = status.getPath();
                    Path marker = new Path(plan, TABLE_DROPPED);
                    if (status.isDir()
                            && fileIO.exists(marker)
                            && tablePath.toString().equals(fileIO.readFileUtf8(marker))) {
                        reclaimTablePlan(fileIO, plan, threads);
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn("Cannot retry file cleanup for {}", tablePath, e);
        }
    }

    private static void copyBatches(FileIO fileIO, Path source, TemplateFileDeletionPlan target)
            throws IOException {
        int count = batchCount(fileIO, source);
        for (int i = 0; i < count; i++) {
            Path batch = batchPath(source, i);
            if (fileIO.exists(batch)) {
                for (Path path : readBatch(fileIO, batch)) {
                    target.add(path);
                }
            }
        }
    }

    private static void reclaimTablePlan(FileIO fileIO, Path plan, int threads) throws IOException {
        ThreadPoolExecutor executor = createCachedThreadPool(threads, "TEMPLATE_DROP_FILES");
        try {
            cleanPlan(
                    fileIO,
                    plan,
                    false,
                    candidates -> {
                        AtomicBoolean deleted = new AtomicBoolean(true);
                        randomlyOnlyExecute(
                                executor,
                                path -> {
                                    if (!deleteFile(fileIO, path)) {
                                        deleted.set(false);
                                    }
                                },
                                candidates);
                        return deleted.get();
                    });
        } finally {
            executor.shutdownNow();
        }
    }

    private static void cleanPlan(FileIO fileIO, Path plan, boolean dryRun, BatchCleaner cleaner)
            throws IOException {
        boolean complete = true;
        int count = batchCount(fileIO, plan);
        for (int i = 0; i < count; i++) {
            Path batch = batchPath(plan, i);
            if (fileIO.exists(batch)) {
                if (cleaner.clean(readBatch(fileIO, batch))) {
                    if (!dryRun) {
                        fileIO.delete(batch, false);
                    }
                } else {
                    complete = false;
                }
            }
        }
        if (!dryRun && complete) {
            fileIO.deleteDirectoryQuietly(plan);
        }
    }

    static boolean deleteFile(FileIO fileIO, Path path) {
        try {
            if (fileIO.delete(path, false) || !fileIO.exists(path)) {
                return true;
            }
            LOG.warn("Cannot remove template file {}", path);
        } catch (IOException e) {
            LOG.warn("Cannot remove template file {}", path, e);
        }
        return false;
    }

    private void flush() throws IOException {
        if (!paths.isEmpty()) {
            publish(
                    fileIO,
                    batchPath(plan, batchCount),
                    JsonSerdeUtil.toJson(
                            paths.stream().map(Path::toString).toArray(String[]::new)));
            batchCount++;
            paths.clear();
        }
    }

    private static Set<Path> readBatch(FileIO fileIO, Path batch) throws IOException {
        Set<Path> candidates = new LinkedHashSet<>();
        for (String file : JsonSerdeUtil.fromJson(fileIO.readFileUtf8(batch), String[].class)) {
            candidates.add(new Path(file));
        }
        return candidates;
    }

    private static int batchCount(FileIO fileIO, Path plan) throws IOException {
        return Integer.parseInt(fileIO.readFileUtf8(new Path(plan, SEALED)));
    }

    private static Path tableCleanupPath(Path tablePath) {
        return new Path(tablePath, CLEANUP_DIRECTORY);
    }

    private static Path archivePath(Path tablePath) {
        Path database = tablePath.getParent();
        Path warehouse = database.getParent();
        return new Path(warehouse == null ? database : warehouse, CLEANUP_DIRECTORY);
    }

    private static Path batchPath(Path plan, int index) {
        return new Path(plan, "batch-" + index);
    }

    private static void publish(FileIO fileIO, Path file, String value) throws IOException {
        if (!fileIO.tryToWriteAtomic(file, value) && !fileIO.exists(file)) {
            throw new IOException("Cannot publish " + file);
        }
    }

    @FunctionalInterface
    interface BatchCleaner {
        boolean clean(Set<Path> candidates) throws IOException;
    }
}
