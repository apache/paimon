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

package org.apache.paimon.append;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link AppendOnlyFileStore}. */
public class BucketedAppendCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(BucketedAppendCompactManager.class);

    private static final int FULL_COMPACT_MIN_FILE = 3;

    private final ExecutorService executor;
    private final AppendDeletionFileMaintainer dvIndexFileMaintainer;
    private final TreeSet<DataFileMeta> toCompact;
    private final int minFileNum;
    private final int maxFileNum;
    private final long targetFileSize;
    private final CompactRewriter rewriter;

    private List<DataFileMeta> compacting;

    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    public BucketedAppendCompactManager(
            ExecutorService executor,
            List<DataFileMeta> restored,
            @Nullable AppendDeletionFileMaintainer dvIndexFileMaintainer,
            int minFileNum,
            int maxFileNum,
            long targetFileSize,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        this.executor = executor;
        this.dvIndexFileMaintainer = dvIndexFileMaintainer;
        this.toCompact = new TreeSet<>(fileComparator(false));
        this.toCompact.addAll(restored);
        this.minFileNum = minFileNum;
        this.maxFileNum = maxFileNum;
        this.targetFileSize = targetFileSize;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (fullCompaction) {
            triggerFullCompaction();
        } else {
            triggerCompactionWithBestEffort();
        }
    }

    private void triggerFullCompaction() {
        Preconditions.checkState(
                taskFuture == null,
                "A compaction task is still running while the user "
                        + "forces a new compaction. This is unexpected.");
        // if deletion vector enables, always trigger compaction.
        if (toCompact.isEmpty()
                || (dvIndexFileMaintainer == null && toCompact.size() < FULL_COMPACT_MIN_FILE)) {
            return;
        }

        taskFuture =
                executor.submit(
                        new FullCompactTask(
                                dvIndexFileMaintainer,
                                toCompact,
                                targetFileSize,
                                rewriter,
                                metricsReporter));
        compacting = new ArrayList<>(toCompact);
        toCompact.clear();
    }

    private void triggerCompactionWithBestEffort() {
        if (taskFuture != null) {
            return;
        }
        Optional<List<DataFileMeta>> picked = pickCompactBefore();
        if (picked.isPresent()) {
            compacting = picked.get();
            taskFuture =
                    executor.submit(
                            new AutoCompactTask(
                                    dvIndexFileMaintainer, compacting, rewriter, metricsReporter));
        }
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        toCompact.add(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> allFiles = new ArrayList<>();
        if (compacting != null) {
            allFiles.addAll(compacting);
        }
        allFiles.addAll(toCompact);
        return allFiles;
    }

    /** Finish current task, and update result files to {@link #toCompact}. */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        if (result.isPresent()) {
            CompactResult compactResult = result.get();
            if (!compactResult.after().isEmpty()) {
                // if the last compacted file is still small,
                // add it back to the head
                DataFileMeta lastFile = compactResult.after().get(compactResult.after().size() - 1);
                if (lastFile.fileSize() < targetFileSize) {
                    toCompact.add(lastFile);
                }
            }
            compacting = null;
        }
        return result;
    }

    @VisibleForTesting
    Optional<List<DataFileMeta>> pickCompactBefore() {
        if (toCompact.isEmpty()) {
            return Optional.empty();
        }

        long totalFileSize = 0L;
        int fileNum = 0;
        LinkedList<DataFileMeta> candidates = new LinkedList<>();

        while (!toCompact.isEmpty()) {
            DataFileMeta file = toCompact.pollFirst();
            candidates.add(file);
            totalFileSize += file.fileSize();
            fileNum++;
            if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                    || fileNum >= maxFileNum) {
                return Optional.of(candidates);
            } else if (totalFileSize >= targetFileSize) {
                // let pointer shift one pos to right
                DataFileMeta removed = candidates.pollFirst();
                assert removed != null;
                totalFileSize -= removed.fileSize();
                fileNum--;
            }
        }
        toCompact.addAll(candidates);
        return Optional.empty();
    }

    @VisibleForTesting
    TreeSet<DataFileMeta> getToCompact() {
        return toCompact;
    }

    @Override
    public void close() throws IOException {
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }
    }

    /** A {@link CompactTask} impl for full compaction of append-only table. */
    public static class FullCompactTask extends CompactTask {

        private final AppendDeletionFileMaintainer dvIndexFileMaintainer;
        private final LinkedList<DataFileMeta> toCompact;
        private final long targetFileSize;
        private final CompactRewriter rewriter;

        public FullCompactTask(
                AppendDeletionFileMaintainer dvIndexFileMaintainer,
                Collection<DataFileMeta> inputs,
                long targetFileSize,
                CompactRewriter rewriter,
                @Nullable CompactionMetrics.Reporter metricsReporter) {
            super(metricsReporter);
            this.dvIndexFileMaintainer = dvIndexFileMaintainer;
            this.toCompact = new LinkedList<>(inputs);
            this.targetFileSize = targetFileSize;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            // remove large files
            while (!toCompact.isEmpty()) {
                DataFileMeta file = toCompact.peekFirst();
                // the data file with deletion file always need to be compacted.
                if (file.fileSize() >= targetFileSize && !hasDeletionFile(file)) {
                    toCompact.poll();
                    continue;
                }
                break;
            }

            // do compaction
            if (dvIndexFileMaintainer != null) {
                // if deletion vector enables, always trigger compaction.
                return compact(dvIndexFileMaintainer, toCompact, rewriter);
            } else {
                // compute small files
                int big = 0;
                int small = 0;
                for (DataFileMeta file : toCompact) {
                    if (file.fileSize() >= targetFileSize) {
                        big++;
                    } else {
                        small++;
                    }
                }
                if (small > big && toCompact.size() >= FULL_COMPACT_MIN_FILE) {
                    return compact(dvIndexFileMaintainer, toCompact, rewriter);
                } else {
                    return result(Collections.emptyList(), Collections.emptyList());
                }
            }
        }

        private boolean hasDeletionFile(DataFileMeta file) {
            return dvIndexFileMaintainer != null
                    && dvIndexFileMaintainer.getDeletionFile(file.fileName()) == null;
        }
    }

    /**
     * A {@link CompactTask} impl for append-only table auto-compaction.
     *
     * <p>This task accepts an already-picked candidate to perform one-time rewrite. And for the
     * rest of input files, it is the duty of {@link AppendOnlyWriter} to invoke the next time
     * compaction.
     */
    public static class AutoCompactTask extends CompactTask {

        private final AppendDeletionFileMaintainer dvIndexFileMaintainer;
        private final List<DataFileMeta> toCompact;
        private final CompactRewriter rewriter;

        public AutoCompactTask(
                AppendDeletionFileMaintainer dvIndexFileMaintainer,
                List<DataFileMeta> toCompact,
                CompactRewriter rewriter,
                @Nullable CompactionMetrics.Reporter metricsReporter) {
            super(metricsReporter);
            this.dvIndexFileMaintainer = dvIndexFileMaintainer;
            this.toCompact = toCompact;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            return compact(dvIndexFileMaintainer, toCompact, rewriter);
        }
    }

    private static CompactResult compact(
            AppendDeletionFileMaintainer dvIndexFileMaintainer,
            List<DataFileMeta> toCompact,
            CompactRewriter rewriter)
            throws Exception {
        if (dvIndexFileMaintainer == null) {
            return result(toCompact, rewriter.rewrite(toCompact));
        } else {
            List<DeletionFile> deletionFiles = new ArrayList<>();
            for (DataFileMeta dataFile : toCompact) {
                deletionFiles.add(dvIndexFileMaintainer.getDeletionFile(dataFile.fileName()));
            }
            List<DataFileMeta> compactAfter = rewriter.rewrite(toCompact);
            toCompact.forEach(f -> dvIndexFileMaintainer.notifyRemovedDeletionVector(f.fileName()));

            List<IndexManifestEntry> indexManifestEntries = dvIndexFileMaintainer.persist();
            if (indexManifestEntries.isEmpty()) {
                return result(toCompact, compactAfter);
            } else {
                List<IndexFileMeta> indexFilesBefore = new ArrayList<>();
                List<IndexFileMeta> indexFilesAfter = new ArrayList<>();
                for (IndexManifestEntry entry : indexManifestEntries) {
                    if (entry.kind() == FileKind.ADD) {
                        indexFilesAfter.add(entry.indexFile());
                    } else {
                        indexFilesBefore.add(entry.indexFile());
                    }
                }
                return result(toCompact, indexFilesBefore, compactAfter, indexFilesAfter);
            }
        }
    }

    private static CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CompactResult(before, after);
    }

    private static CompactResult result(
            List<DataFileMeta> before,
            @Nullable List<IndexFileMeta> indexFilesBefore,
            List<DataFileMeta> after,
            @Nullable List<IndexFileMeta> indexFilesAfter) {
        CompactResult result = new CompactResult(before, after);
        if (indexFilesBefore != null || indexFilesAfter != null) {
            IndexIncrement indexIncrement = new IndexIncrement(indexFilesAfter, indexFilesBefore);
            result.setIndexIncrement(indexIncrement);
        }
        return result;
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }

    /**
     * New files may be created during the compaction process, then the results of the compaction
     * may be put after the new files, and this order will be disrupted. We need to ensure this
     * order, so we force the order by sequence.
     */
    public static Comparator<DataFileMeta> fileComparator(boolean ignoreOverlap) {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }

            if (!ignoreOverlap && isOverlap(o1, o2)) {
                LOG.warn(
                        String.format(
                                "There should no overlap in append files, but Range1(%s, %s), Range2(%s, %s),"
                                        + " check if you have multiple write jobs.",
                                o1.minSequenceNumber(),
                                o1.maxSequenceNumber(),
                                o2.minSequenceNumber(),
                                o2.maxSequenceNumber()));
            }

            return Long.compare(o1.minSequenceNumber(), o2.minSequenceNumber());
        };
    }

    private static boolean isOverlap(DataFileMeta o1, DataFileMeta o2) {
        return o2.minSequenceNumber() <= o1.maxSequenceNumber()
                && o2.maxSequenceNumber() >= o1.minSequenceNumber();
    }
}
