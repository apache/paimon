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

package org.apache.paimon.compact;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/** Compact task. */
public abstract class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    @Nullable private final CompactionMetrics.Reporter metricsReporter;
    private final String bucketInfo;

    /**
     * Output of the steps of this task which have already finished. Subclasses producing files in
     * more than one step must accumulate into this result, otherwise files written by the finished
     * steps are leaked when a later step fails.
     */
    private final CompactResult produced = new CompactResult();

    /**
     * Makes publishing a result and declaring it cancelled mutually exclusive, so that a result is
     * never both dropped by the canceller and kept by the task. Only ever held for field
     * assignments, never across compaction or file deletion.
     */
    private final Object publishLock = new Object();

    private boolean cancelled = false;
    @Nullable private CompactResult completedResult = null;

    public CompactTask(@Nullable CompactionMetrics.Reporter metricsReporter, String bucketInfo) {
        this.metricsReporter = metricsReporter;
        this.bucketInfo = bucketInfo;
    }

    protected CompactResult produced() {
        return produced;
    }

    /**
     * Declare that the result of this task will be discarded, so that the task deletes the files it
     * produced instead of leaving them behind. This must be invoked before interrupting the task,
     * because a task doing pure CPU work may run to completion without ever observing the
     * interruption.
     *
     * <p>Invariant: the caller must not let a cancelled/discarded result be consumed by the same
     * writer's {@code prepareCommit}. Compaction may already have mutated shared in-memory state
     * (for example {@code BucketedDvMaintainer} or clustering key index) before the result is
     * published; {@link #discard} only deletes produced files and does not roll that state back.
     * Today {@code cancelCompaction} is only invoked from writer {@code close()}, so the maintainer
     * is thrown away with the writer; do not call this from a path that continues writing with the
     * same maintainer.
     */
    public void cancel() {
        synchronized (publishLock) {
            cancelled = true;
        }
    }

    /**
     * Result of a task which ran to completion, {@code null} if this task has not produced a
     * complete result. {@link java.util.concurrent.FutureTask} silently drops the value returned by
     * {@link #call} when cancellation wins the race against its own completion, this field keeps
     * the result reachable so that its files can still be accounted for.
     */
    @Nullable
    public CompactResult completedResult() {
        synchronized (publishLock) {
            return completedResult;
        }
    }

    /**
     * Make the finished result available to {@link #completedResult}, unless this task has already
     * been cancelled. Returns {@code false} when the result must be discarded by the task itself.
     *
     * <p>This is the single point where the ownership of the produced files is decided: either the
     * task publishes first and {@link #cancel} arrives too late to drop the files silently, or
     * {@link #cancel} wins and the task is the one which deletes them. There is no state in which
     * both sides believe the other one takes care of the files.
     */
    @VisibleForTesting
    protected boolean publish(CompactResult result) {
        synchronized (publishLock) {
            if (cancelled) {
                return false;
            }
            completedResult = result;
            return true;
        }
    }

    @Override
    public CompactResult call() throws Exception {
        MetricUtils.safeCall(this::startTimer, LOG);
        LOG.info(
                "Paimon compact task started: {}, taskType={}",
                bucketInfo,
                getClass().getSimpleName());
        try {
            long startMillis = System.currentTimeMillis();
            CompactResult result = doCompact();
            long durationMs = System.currentTimeMillis() - startMillis;

            // Publish before doing anything else, so that no work done afterwards (metrics,
            // logging) can widen the window in which a concurrent cancellation goes unnoticed.
            if (!publish(result)) {
                LOG.info(
                        "Paimon compact task was cancelled after it finished: {}, taskType={}. "
                                + "Deleting its output because nobody will consume the result.",
                        bucketInfo,
                        getClass().getSimpleName());
                discard();
                return new CompactResult();
            }

            MetricUtils.safeCall(
                    () -> {
                        if (metricsReporter != null) {
                            metricsReporter.reportCompactionTime(durationMs);
                            metricsReporter.increaseCompactionsCompletedCount();
                            metricsReporter.reportCompactionInputSize(
                                    result.before().stream()
                                            .map(DataFileMeta::fileSize)
                                            .reduce(Long::sum)
                                            .orElse(0L));
                            metricsReporter.reportCompactionOutputSize(
                                    result.after().stream()
                                            .map(DataFileMeta::fileSize)
                                            .reduce(Long::sum)
                                            .orElse(0L));
                        }
                    },
                    LOG);

            LOG.info(
                    "Paimon compact task finished: {}, taskType={}, "
                            + "inputFiles={}, inputBytes={}, outputFiles={}, outputBytes={}, durationMs={}",
                    bucketInfo,
                    getClass().getSimpleName(),
                    result.before().size(),
                    result.before().stream().mapToLong(DataFileMeta::fileSize).sum(),
                    result.after().size(),
                    result.after().stream().mapToLong(DataFileMeta::fileSize).sum(),
                    durationMs);

            if (LOG.isDebugEnabled()) {
                LOG.debug(logMetric(startMillis, result.before(), result.after()));
            }
            return result;
        } catch (Exception e) {
            LOG.warn(
                    "Paimon compact task failed: {}, taskType={}",
                    bucketInfo,
                    getClass().getSimpleName(),
                    e);
            discard();
            throw e;
        } finally {
            MetricUtils.safeCall(this::stopTimer, LOG);
            MetricUtils.safeCall(this::decreaseCompactionsQueuedCount, LOG);
        }
    }

    /**
     * Delete the files of a result which will never be committed. Note that an output file can be
     * the very same physical file as one of the inputs when it is only upgraded to another level,
     * such a file is still required by previous snapshots and must be kept.
     *
     * <p>Only file-side cleanup is performed here. In-memory side effects applied during {@link
     * #doCompact} (deletion-vector removals, clustering key-index updates, and similar) are not
     * restored; see {@link #cancel()} for the caller invariant that makes this safe.
     */
    private void discard() {
        Set<String> inputs = new HashSet<>();
        for (DataFileMeta file : produced.before()) {
            inputs.add(file.fileName());
        }
        List<DataFileMeta> files = new ArrayList<>(produced.changelog());
        for (DataFileMeta file : produced.after()) {
            if (!inputs.contains(file.fileName())) {
                files.add(file);
            }
        }

        // Cancellation interrupts this thread, and file systems backed by RPC (HDFS, object
        // stores) fail their calls immediately while the interrupt flag is set. Clear it for the
        // duration of the deletions, otherwise the cleanup silently does nothing and leaves
        // exactly the orphan files it is supposed to remove.
        boolean wasInterrupted = Thread.interrupted();
        try {
            deleteProduced(files);
            if (produced.deletionFile() != null) {
                produced.deletionFile().clean();
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to delete the output of a discarded compact task: {}, taskType={}",
                    bucketInfo,
                    getClass().getSimpleName(),
                    e);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Delete files produced by this task, invoked when its result is discarded. */
    protected abstract void deleteProduced(List<DataFileMeta> files);

    private void decreaseCompactionsQueuedCount() {
        if (metricsReporter != null) {
            metricsReporter.decreaseCompactionsQueuedCount();
        }
    }

    private void startTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().start();
        }
    }

    private void stopTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().finish();
        }
    }

    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "Done compacting %d files to %d files in %dms. "
                        + "Rewrite input file size = %d, output file size = %d",
                compactBefore.size(),
                compactAfter.size(),
                System.currentTimeMillis() - startMillis,
                collectRewriteSize(compactBefore),
                collectRewriteSize(compactAfter));
    }

    /**
     * Perform compaction. Implementations must accumulate their output into {@link #produced()} and
     * return it, so that a partial result of a failed task is still known and can be cleaned up.
     *
     * @return {@link CompactResult} of compact before and compact after files.
     */
    protected abstract CompactResult doCompact() throws Exception;

    private long collectRewriteSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
