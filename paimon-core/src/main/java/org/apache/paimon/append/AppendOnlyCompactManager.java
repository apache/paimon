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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link AppendOnlyFileStore}. */
public class AppendOnlyCompactManager extends CompactFutureManager {

    private final FileIO fileIO;
    private final ExecutorService executor;
    private final TreeSet<DataFileMeta> toCompact;
    private final int minFileNum;
    private final int maxFileNum;
    private final long targetFileSize;
    private final CompactRewriter rewriter;
    private final DataFilePathFactory pathFactory;
    private final boolean assertDisorder;

    public AppendOnlyCompactManager(
            FileIO fileIO,
            ExecutorService executor,
            List<DataFileMeta> restored,
            int minFileNum,
            int maxFileNum,
            long targetFileSize,
            CompactRewriter rewriter,
            DataFilePathFactory pathFactory,
            boolean assertDisorder) {
        this.fileIO = fileIO;
        this.executor = executor;
        this.assertDisorder = assertDisorder;
        this.toCompact = new TreeSet<>(fileComparator(assertDisorder));
        this.toCompact.addAll(restored);
        this.minFileNum = minFileNum;
        this.maxFileNum = maxFileNum;
        this.targetFileSize = targetFileSize;
        this.rewriter = rewriter;
        this.pathFactory = pathFactory;
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
        taskFuture =
                executor.submit(
                        new AppendOnlyCompactManager.IterativeCompactTask(
                                fileIO,
                                toCompact,
                                targetFileSize,
                                minFileNum,
                                maxFileNum,
                                rewriter,
                                pathFactory,
                                assertDisorder));
    }

    private void triggerCompactionWithBestEffort() {
        if (taskFuture != null) {
            return;
        }
        pickCompactBefore()
                .ifPresent(
                        (inputs) ->
                                taskFuture =
                                        executor.submit(new AutoCompactTask(inputs, rewriter)));
    }

    @Override
    public boolean shouldWaitCompaction() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        toCompact.add(file);
    }

    @Override
    public Collection<DataFileMeta> allFiles() {
        return toCompact;
    }

    /** Finish current task, and update result files to {@link #toCompact}. */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (!r.after().isEmpty()) {
                        // if the last compacted file is still small,
                        // add it back to the head
                        DataFileMeta lastFile = r.after().get(r.after().size() - 1);
                        if (lastFile.fileSize() < targetFileSize) {
                            toCompact.add(lastFile);
                        }
                    }
                });
        return result;
    }

    @VisibleForTesting
    Optional<List<DataFileMeta>> pickCompactBefore() {
        return pick(toCompact, targetFileSize, minFileNum, maxFileNum);
    }

    private static Optional<List<DataFileMeta>> pick(
            TreeSet<DataFileMeta> toCompact, long targetFileSize, int minFileNum, int maxFileNum) {
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
    public void close() throws IOException {}

    /**
     * A {@link CompactTask} impl for full compaction of append-only table.
     *
     * <p>This task accepts a pre-scanned file list as input and pick the candidate files to compact
     * iteratively until reach the end of the input. There might be multiple times of rewrite
     * happens during one task.
     */
    public static class IterativeCompactTask extends CompactTask {

        private final FileIO fileIO;
        private final Collection<DataFileMeta> inputs;
        private final long targetFileSize;
        private final int minFileNum;
        private final int maxFileNum;
        private final CompactRewriter rewriter;
        private final DataFilePathFactory factory;
        private final boolean assertDisorder;

        public IterativeCompactTask(
                FileIO fileIO,
                Collection<DataFileMeta> inputs,
                long targetFileSize,
                int minFileNum,
                int maxFileNum,
                CompactRewriter rewriter,
                DataFilePathFactory factory,
                boolean assertDisorder) {
            this.fileIO = fileIO;
            this.inputs = inputs;
            this.targetFileSize = targetFileSize;
            this.minFileNum = minFileNum;
            this.maxFileNum = maxFileNum;
            this.rewriter = rewriter;
            this.factory = factory;
            this.assertDisorder = assertDisorder;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            TreeSet<DataFileMeta> toCompact = new TreeSet<>(fileComparator(assertDisorder));
            toCompact.addAll(inputs);
            Set<DataFileMeta> compactBefore = new LinkedHashSet<>();
            List<DataFileMeta> compactAfter = new ArrayList<>();
            while (!toCompact.isEmpty()) {
                Optional<List<DataFileMeta>> candidates =
                        AppendOnlyCompactManager.pick(
                                toCompact, targetFileSize, minFileNum, maxFileNum);
                if (candidates.isPresent()) {
                    List<DataFileMeta> before = candidates.get();
                    compactBefore.addAll(before);
                    List<DataFileMeta> after = rewriter.rewrite(before);
                    compactAfter.addAll(after);
                    DataFileMeta lastFile = after.get(after.size() - 1);
                    if (lastFile.fileSize() < targetFileSize) {
                        toCompact.add(lastFile);
                    }
                } else {
                    break;
                }
            }
            // remove and delete intermediate files
            Iterator<DataFileMeta> afterIterator = compactAfter.iterator();
            while (afterIterator.hasNext()) {
                DataFileMeta file = afterIterator.next();
                if (compactBefore.contains(file)) {
                    compactBefore.remove(file);
                    afterIterator.remove();
                    delete(file);
                }
            }
            return result(new ArrayList<>(compactBefore), compactAfter);
        }

        @VisibleForTesting
        void delete(DataFileMeta tmpFile) {
            fileIO.deleteQuietly(factory.toPath(tmpFile.fileName()));
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

        private final List<DataFileMeta> toCompact;
        private final CompactRewriter rewriter;

        public AutoCompactTask(List<DataFileMeta> toCompact, CompactRewriter rewriter) {
            this.toCompact = toCompact;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            return result(toCompact, rewriter.rewrite(toCompact));
        }
    }

    private static CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CompactResult() {
            @Override
            public List<DataFileMeta> before() {
                return before;
            }

            @Override
            public List<DataFileMeta> after() {
                return after;
            }
        };
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
    public static Comparator<DataFileMeta> fileComparator(boolean assertDisorder) {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }

            if (assertDisorder && isOverlap(o1, o2)) {
                throw new RuntimeException(
                        String.format(
                                "There should no overlap in append files, there is a bug!"
                                        + " Range1(%s, %s), Range2(%s, %s)",
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
