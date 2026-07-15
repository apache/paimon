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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexLevels;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maintains bucket-local full-text archives with source-backed LSM consolidation. */
public class BucketedFullTextIndexMaintainer {

    private static final int DEFAULT_LEVEL_FANOUT = 5;
    private static final double DEFAULT_STALE_RATIO_THRESHOLD = 0.2;

    private final int textFieldId;
    private final PkFullTextIndexFile indexFile;
    private final PkFullTextIndexBuilder indexBuilder;
    private final PrimaryKeyIndexLevels<IndexFileMeta> levels;
    private final List<IndexFileMeta> currentPayloads = new ArrayList<>();
    private final List<IndexFileMeta> retiredPayloads = new ArrayList<>();
    private final Map<String, DataFileMeta> activeSourceFiles = new LinkedHashMap<>();
    private ExecutorService executor;
    @Nullable private PendingBuild pendingBuild;

    public BucketedFullTextIndexMaintainer(
            int textFieldId,
            PkFullTextIndexFile indexFile,
            PkFullTextIndexBuilder indexBuilder,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor) {
        this(
                textFieldId,
                indexFile,
                indexBuilder,
                DEFAULT_LEVEL_FANOUT,
                DEFAULT_STALE_RATIO_THRESHOLD,
                restoredDataFiles,
                restoredPayloads,
                executor);
    }

    public BucketedFullTextIndexMaintainer(
            int textFieldId,
            PkFullTextIndexFile indexFile,
            PkFullTextIndexBuilder indexBuilder,
            int levelFanout,
            double staleRatioThreshold,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor) {
        this.textFieldId = textFieldId;
        this.indexFile = indexFile;
        this.indexBuilder = indexBuilder;
        this.levels =
                new PrimaryKeyIndexLevels<>(
                        levelFanout,
                        staleRatioThreshold,
                        IndexFileMeta::fileName,
                        payload -> sourceMeta(payload).sourceFiles());
        this.executor = executor;
        for (DataFileMeta file : restoredDataFiles) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                activeSourceFiles.put(file.fileName(), file);
            }
        }

        PkFullTextBucketIndexState restoredState =
                PkFullTextBucketIndexState.fromActivePayloads(textFieldId, restoredPayloads);
        currentPayloads.addAll(restoredState.currentPayloads());
        retiredPayloads.addAll(restoredState.stalePayloads());
        validateActiveSourceRows();
    }

    public synchronized FullTextIndexCommit prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        checkArgument(
                eligibleFiles(appendIncrement.newFiles()).isEmpty(),
                "Append files must not be primary-key full-text index sources.");

        List<IndexFileMeta> originalPayloads = new ArrayList<>(currentPayloads);
        List<IndexFileMeta> originalRetired = new ArrayList<>(retiredPayloads);
        Map<String, DataFileMeta> originalSources = new LinkedHashMap<>(activeSourceFiles);
        List<IndexFileMeta> generated = new ArrayList<>();
        try {
            applyDataTransition(compactIncrement);
            List<IndexFileMeta> created = new ArrayList<>();
            List<IndexFileMeta> removed = new ArrayList<>(retiredPayloads);
            retiredPayloads.clear();

            while (true) {
                Optional<CompletedBuild> completed = finishPendingBuild(waitCompaction);
                if (completed.isPresent()) {
                    CompletedBuild build = completed.get();
                    generated.add(build.payload);
                    if (canAccept(build)) {
                        replacePayloads(
                                build.inputPayloads, build.payload, created, removed, generated);
                    } else {
                        deleteGenerated(build.payload, generated);
                    }
                }

                if (pendingBuild == null) {
                    List<DataFileMeta> uncovered = uncoveredFiles();
                    if (!uncovered.isEmpty()) {
                        startBuild(uncovered, Collections.emptyList());
                    } else {
                        Optional<PrimaryKeyIndexLevels.Plan<IndexFileMeta>> plan =
                                levels.pick(currentPayloads, activeSourceFiles);
                        if (plan.isPresent()) {
                            if (plan.get().sourceFiles().isEmpty()) {
                                removePayloads(
                                        plan.get().inputUnits(), created, removed, generated);
                                continue;
                            }
                            startBuild(plan.get().sourceFiles(), plan.get().inputUnits());
                        }
                    }
                }
                if (!waitCompaction || pendingBuild == null) {
                    break;
                }
            }

            boolean hasCompactDataTransition =
                    !compactIncrement.compactBefore().isEmpty()
                            || !compactIncrement.compactAfter().isEmpty();
            boolean changed = !created.isEmpty() || !removed.isEmpty();
            Optional<FullTextIndexIncrement> appendChange =
                    changed && !hasCompactDataTransition
                            ? Optional.of(new FullTextIndexIncrement(created, removed))
                            : Optional.empty();
            Optional<FullTextIndexIncrement> compactChange =
                    changed && hasCompactDataTransition
                            ? Optional.of(new FullTextIndexIncrement(created, removed))
                            : Optional.empty();
            return new FullTextIndexCommit(
                    appendChange,
                    compactChange,
                    failure ->
                            rollback(
                                    originalPayloads,
                                    originalRetired,
                                    originalSources,
                                    generated,
                                    failure));
        } catch (Throwable failure) {
            rollback(originalPayloads, originalRetired, originalSources, generated, failure);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private void applyDataTransition(CompactIncrement compactIncrement) {
        for (DataFileMeta file : compactIncrement.compactBefore()) {
            if (!containsFile(compactIncrement.compactAfter(), file.fileName())) {
                activeSourceFiles.remove(file.fileName());
            }
        }
        for (DataFileMeta file : compactIncrement.compactAfter()) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                activeSourceFiles.put(file.fileName(), file);
            }
        }
    }

    private List<DataFileMeta> uncoveredFiles() {
        Set<String> covered = coveredSources(currentPayloads);
        List<DataFileMeta> uncovered = new ArrayList<>();
        for (DataFileMeta source : activeSourceFiles.values()) {
            if (!covered.contains(source.fileName())) {
                uncovered.add(source);
            }
        }
        uncovered.sort(Comparator.comparing(DataFileMeta::fileName));
        return uncovered;
    }

    private void startBuild(List<DataFileMeta> sourceFiles, List<IndexFileMeta> inputPayloads) {
        PendingBuild build = new PendingBuild(sourceFiles, inputPayloads);
        build.start();
        pendingBuild = build;
    }

    private Optional<CompletedBuild> finishPendingBuild(boolean blocking) throws Exception {
        if (pendingBuild == null || (!blocking && !pendingBuild.isDone())) {
            return Optional.empty();
        }
        PendingBuild completed = pendingBuild;
        try {
            IndexFileMeta payload = completed.get();
            pendingBuild = null;
            return Optional.of(
                    new CompletedBuild(completed.sourceFiles, completed.inputPayloads, payload));
        } catch (CancellationException e) {
            pendingBuild = null;
            return Optional.empty();
        } catch (ExecutionException e) {
            pendingBuild = null;
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private boolean canAccept(CompletedBuild build) {
        if (!currentPayloads.containsAll(build.inputPayloads)) {
            return false;
        }
        PkFullTextBucketIndexState outputState =
                PkFullTextBucketIndexState.fromActivePayloads(
                        textFieldId, Collections.singletonList(build.payload));
        if (outputState.currentPayloads().size() != 1) {
            return false;
        }
        List<PrimaryKeyIndexSourceFile> actualSources = sourceMeta(build.payload).sourceFiles();
        if (actualSources.size() != build.sourceFiles.size()) {
            return false;
        }

        List<IndexFileMeta> retained = new ArrayList<>(currentPayloads);
        retained.removeAll(build.inputPayloads);
        Set<String> retainedSources = coveredSources(retained);
        for (int i = 0; i < actualSources.size(); i++) {
            PrimaryKeyIndexSourceFile actual = actualSources.get(i);
            DataFileMeta expected = build.sourceFiles.get(i);
            DataFileMeta active = activeSourceFiles.get(actual.fileName());
            if (!actual.fileName().equals(expected.fileName())
                    || actual.rowCount() != expected.rowCount()
                    || active == null
                    || active.rowCount() != actual.rowCount()
                    || retainedSources.contains(actual.fileName())) {
                return false;
            }
        }
        return true;
    }

    private void replacePayloads(
            List<IndexFileMeta> inputs,
            IndexFileMeta output,
            List<IndexFileMeta> created,
            List<IndexFileMeta> removed,
            List<IndexFileMeta> generated) {
        removePayloads(inputs, created, removed, generated);
        currentPayloads.add(output);
        created.add(output);
    }

    private void removePayloads(
            List<IndexFileMeta> inputs,
            List<IndexFileMeta> created,
            List<IndexFileMeta> removed,
            List<IndexFileMeta> generated) {
        for (IndexFileMeta input : inputs) {
            checkArgument(
                    currentPayloads.remove(input), "Full-text rebuild input is no longer active.");
            if (created.remove(input)) {
                deleteGenerated(input, generated);
            } else {
                removed.add(input);
            }
        }
    }

    private void deleteGenerated(IndexFileMeta payload, List<IndexFileMeta> generated) {
        indexFile.delete(payload);
        generated.remove(payload);
    }

    private synchronized void rollback(
            List<IndexFileMeta> originalPayloads,
            List<IndexFileMeta> originalRetired,
            Map<String, DataFileMeta> originalSources,
            List<IndexFileMeta> generated,
            Throwable failure) {
        currentPayloads.clear();
        currentPayloads.addAll(originalPayloads);
        retiredPayloads.clear();
        retiredPayloads.addAll(originalRetired);
        activeSourceFiles.clear();
        activeSourceFiles.putAll(originalSources);

        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            try {
                build.cancel();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        for (IndexFileMeta payload : generated) {
            try {
                indexFile.delete(payload);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    public synchronized boolean buildNotCompleted() {
        return pendingBuild != null;
    }

    public synchronized void withExecutor(ExecutorService executor) {
        checkArgument(pendingBuild == null, "Cannot replace executor during a full-text build.");
        this.executor = executor;
    }

    public synchronized void close() {
        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            build.cancel();
        }
    }

    public synchronized PkFullTextBucketIndexState state() {
        return PkFullTextBucketIndexState.fromActivePayloads(textFieldId, currentPayloads);
    }

    public synchronized List<IndexFileMeta> payloads() {
        return Collections.unmodifiableList(new ArrayList<>(currentPayloads));
    }

    private void validateActiveSourceRows() {
        for (IndexFileMeta payload : currentPayloads) {
            for (PrimaryKeyIndexSourceFile source : sourceMeta(payload).sourceFiles()) {
                DataFileMeta active = activeSourceFiles.get(source.fileName());
                if (active != null) {
                    checkArgument(
                            active.rowCount() == source.rowCount(),
                            "Full-text source %s row count does not match its active data file.",
                            source.fileName());
                }
            }
        }
    }

    private class PendingBuild {

        private final List<DataFileMeta> sourceFiles;
        private final List<IndexFileMeta> inputPayloads;
        @Nullable private IndexFileMeta result;
        @Nullable private Future<IndexFileMeta> future;
        private boolean cancelled;

        private PendingBuild(List<DataFileMeta> sourceFiles, List<IndexFileMeta> inputPayloads) {
            this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
            this.inputPayloads = Collections.unmodifiableList(new ArrayList<>(inputPayloads));
        }

        private void start() {
            future =
                    executor.submit(
                            () -> {
                                IndexFileMeta payload =
                                        sourceFiles.size() == 1
                                                ? indexBuilder.build(sourceFiles.get(0))
                                                : indexBuilder.build(sourceFiles);
                                synchronized (PendingBuild.this) {
                                    if (!cancelled) {
                                        result = payload;
                                        return payload;
                                    }
                                }
                                indexFile.delete(payload);
                                throw new CancellationException();
                            });
        }

        private boolean isDone() {
            return future.isDone();
        }

        private IndexFileMeta get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        private void cancel() {
            Future<IndexFileMeta> buildFuture;
            IndexFileMeta builtPayload;
            synchronized (this) {
                cancelled = true;
                buildFuture = future;
                builtPayload = result;
                result = null;
            }
            if (buildFuture != null) {
                buildFuture.cancel(true);
            }
            if (builtPayload != null) {
                indexFile.delete(builtPayload);
            }
        }
    }

    private static final class CompletedBuild {

        private final List<DataFileMeta> sourceFiles;
        private final List<IndexFileMeta> inputPayloads;
        private final IndexFileMeta payload;

        private CompletedBuild(
                List<DataFileMeta> sourceFiles,
                List<IndexFileMeta> inputPayloads,
                IndexFileMeta payload) {
            this.sourceFiles = sourceFiles;
            this.inputPayloads = inputPayloads;
            this.payload = payload;
        }
    }

    private static Set<String> coveredSources(List<IndexFileMeta> payloads) {
        Set<String> covered = new HashSet<>();
        for (IndexFileMeta payload : payloads) {
            for (PrimaryKeyIndexSourceFile source : sourceMeta(payload).sourceFiles()) {
                covered.add(source.fileName());
            }
        }
        return covered;
    }

    private static List<DataFileMeta> eligibleFiles(List<DataFileMeta> files) {
        List<DataFileMeta> eligible = new ArrayList<>();
        for (DataFileMeta file : files) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                eligible.add(file);
            }
        }
        return eligible;
    }

    private static boolean containsFile(List<DataFileMeta> files, String fileName) {
        for (DataFileMeta file : files) {
            if (file.fileName().equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    private static PrimaryKeyIndexSourceMeta sourceMeta(IndexFileMeta payload) {
        return PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
    }

    /** Full-text changes prepared for Paimon's append snapshot or compact snapshot. */
    public static class FullTextIndexCommit {

        private final Optional<FullTextIndexIncrement> appendIncrement;
        private final Optional<FullTextIndexIncrement> compactIncrement;
        private final AbortAction abortAction;

        private FullTextIndexCommit(
                Optional<FullTextIndexIncrement> appendIncrement,
                Optional<FullTextIndexIncrement> compactIncrement,
                AbortAction abortAction) {
            this.appendIncrement = appendIncrement;
            this.compactIncrement = compactIncrement;
            this.abortAction = abortAction;
        }

        public Optional<FullTextIndexIncrement> appendIncrement() {
            return appendIncrement;
        }

        public Optional<FullTextIndexIncrement> compactIncrement() {
            return compactIncrement;
        }

        public void abort(Throwable failure) {
            abortAction.abort(failure);
        }
    }

    @FunctionalInterface
    private interface AbortAction {

        void abort(Throwable failure);
    }

    /** Index-file additions and deletions emitted by one bucket state update. */
    public static class FullTextIndexIncrement {

        private final List<IndexFileMeta> newIndexFiles;
        private final List<IndexFileMeta> deletedIndexFiles;

        private FullTextIndexIncrement(
                List<IndexFileMeta> newIndexFiles, List<IndexFileMeta> deletedIndexFiles) {
            this.newIndexFiles = Collections.unmodifiableList(new ArrayList<>(newIndexFiles));
            this.deletedIndexFiles =
                    Collections.unmodifiableList(new ArrayList<>(deletedIndexFiles));
        }

        public List<IndexFileMeta> newIndexFiles() {
            return newIndexFiles;
        }

        public List<IndexFileMeta> deletedIndexFiles() {
            return deletedIndexFiles;
        }
    }
}
