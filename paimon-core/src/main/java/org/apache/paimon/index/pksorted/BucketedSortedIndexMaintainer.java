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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexLevels;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maintains one bucket-local source-backed BTree or Bitmap definition. */
public class BucketedSortedIndexMaintainer {

    private static final Logger LOG = LoggerFactory.getLogger(BucketedSortedIndexMaintainer.class);
    private static final int MAX_BUILD_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_BACKOFF_MILLIS = 10L;

    private final int fieldId;
    private final String indexType;
    private final PkSortedIndexFile indexFile;
    private final BuildFunction buildFunction;
    private final PrimaryKeyIndexLevels<PkSortedIndexGroup> levels;
    private final Map<String, DataFileMeta> activeSourceFiles = new LinkedHashMap<>();
    private final List<PkSortedIndexGroup> groups = new ArrayList<>();
    private final List<IndexFileMeta> pendingRestoredDeletions = new ArrayList<>();
    private ExecutorService executor;
    @Nullable private PendingBuild pendingBuild;

    public BucketedSortedIndexMaintainer(
            int fieldId,
            String indexType,
            PkSortedIndexFile indexFile,
            BuildFunction buildFunction,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor) {
        this.fieldId = fieldId;
        this.indexType = indexType;
        this.indexFile = indexFile;
        this.buildFunction = buildFunction;
        this.levels =
                new PrimaryKeyIndexLevels<>(
                        PkSortedIndexGroup::dataLevel, PkSortedIndexGroup::sourceFiles);
        this.executor = executor;
        for (DataFileMeta dataFile : restoredDataFiles) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(dataFile)) {
                activeSourceFiles.put(dataFile.fileName(), dataFile);
            }
        }

        List<IndexFileMeta> definitionPayloads = new ArrayList<>();
        for (IndexFileMeta payload : restoredPayloads) {
            if (indexType.equals(payload.indexType())
                    && payload.globalIndexMeta() != null
                    && payload.globalIndexMeta().indexFieldId() == fieldId) {
                definitionPayloads.add(payload);
            }
        }
        PkSortedBucketIndexState restoredState =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        fieldId,
                        indexType,
                        new ArrayList<>(activeSourceFiles.values()),
                        definitionPayloads);
        groups.addAll(restoredState.groups());
        pendingRestoredDeletions.addAll(restoredState.rejectedPayloads());
    }

    public synchronized SortedIndexCommit prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        return prepareCommit(appendIncrement, compactIncrement, waitCompaction, true);
    }

    public synchronized SortedIndexCommit prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction,
            boolean allowBuildStart)
            throws Exception {
        checkArgument(
                eligibleFiles(appendIncrement.newFiles()).isEmpty(),
                "Append files must not be primary-key sorted index sources.");

        Map<String, DataFileMeta> originalSourceFiles = new LinkedHashMap<>(activeSourceFiles);
        List<PkSortedIndexGroup> originalGroups = new ArrayList<>(groups);
        List<IndexFileMeta> originalRestoredDeletions = new ArrayList<>(pendingRestoredDeletions);
        List<IndexFileMeta> created = new ArrayList<>();
        try {
            boolean hasCompactDataTransition =
                    !compactIncrement.compactBefore().isEmpty()
                            || !compactIncrement.compactAfter().isEmpty();
            applySourceTransition(compactIncrement);

            List<IndexFileMeta> removed = new ArrayList<>(pendingRestoredDeletions);
            pendingRestoredDeletions.clear();
            while (true) {
                Optional<CompletedBuild> completed = finishPendingBuild(waitCompaction);
                if (completed.isPresent()) {
                    acceptOrDelete(completed.get(), created, removed);
                }

                if (pendingBuild == null && allowBuildStart) {
                    Optional<PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup>> plan =
                            levels.pick(groups, activeSourceFiles);
                    if (plan.isPresent()) {
                        if (plan.get().sourceFiles().isEmpty()) {
                            replaceInputGroups(
                                    plan.get().inputUnits(), Optional.empty(), created, removed);
                            continue;
                        }
                        startBuild(plan.get());
                    }
                }
                if (!waitCompaction || pendingBuild == null) {
                    break;
                }
            }

            boolean changed = !created.isEmpty() || !removed.isEmpty();
            Optional<SortedIndexIncrement> appendChange =
                    changed && !hasCompactDataTransition
                            ? Optional.of(new SortedIndexIncrement(created, removed))
                            : Optional.empty();
            Optional<SortedIndexIncrement> compactChange =
                    changed && hasCompactDataTransition
                            ? Optional.of(new SortedIndexIncrement(created, removed))
                            : Optional.empty();
            return new SortedIndexCommit(
                    appendChange,
                    compactChange,
                    failure ->
                            rollbackPrepareCommit(
                                    originalSourceFiles,
                                    originalGroups,
                                    originalRestoredDeletions,
                                    created,
                                    failure));
        } catch (Throwable failure) {
            rollbackPrepareCommit(
                    originalSourceFiles,
                    originalGroups,
                    originalRestoredDeletions,
                    created,
                    failure);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private synchronized void rollbackPrepareCommit(
            Map<String, DataFileMeta> originalSourceFiles,
            List<PkSortedIndexGroup> originalGroups,
            List<IndexFileMeta> originalRestoredDeletions,
            List<IndexFileMeta> created,
            Throwable failure) {
        activeSourceFiles.clear();
        activeSourceFiles.putAll(originalSourceFiles);
        groups.clear();
        groups.addAll(originalGroups);
        pendingRestoredDeletions.clear();
        pendingRestoredDeletions.addAll(originalRestoredDeletions);

        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            try {
                build.cancel();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        for (IndexFileMeta payload : created) {
            try {
                indexFile.delete(payload);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void applySourceTransition(CompactIncrement compactIncrement) {
        for (DataFileMeta before : compactIncrement.compactBefore()) {
            if (!containsFile(compactIncrement.compactAfter(), before.fileName())) {
                activeSourceFiles.remove(before.fileName());
            }
        }
        for (DataFileMeta after : compactIncrement.compactAfter()) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(after)) {
                activeSourceFiles.put(after.fileName(), after);
            }
        }
    }

    private boolean isCovered(DataFileMeta candidate, List<PkSortedIndexGroup> excludedGroups) {
        for (PkSortedIndexGroup group : groups) {
            if (excludedGroups.contains(group)) {
                continue;
            }
            for (PrimaryKeyIndexSourceFile source : group.sourceFiles()) {
                if (source.fileName().equals(candidate.fileName())
                        && source.rowCount() == candidate.rowCount()) {
                    return true;
                }
            }
        }
        return false;
    }

    private void startBuild(PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup> plan) {
        PendingBuild next = new PendingBuild(plan);
        next.start();
        pendingBuild = next;
    }

    private Optional<CompletedBuild> finishPendingBuild(boolean blocking) throws Exception {
        if (pendingBuild == null || (!blocking && !pendingBuild.isDone())) {
            return Optional.empty();
        }
        PendingBuild completed = pendingBuild;
        try {
            IndexFileMeta payload = completed.get();
            pendingBuild = null;
            return Optional.of(new CompletedBuild(completed.plan, payload));
        } catch (CancellationException e) {
            pendingBuild = null;
            throw e;
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

    private void acceptOrDelete(
            CompletedBuild completed, List<IndexFileMeta> created, List<IndexFileMeta> removed) {
        if (!levels.isCurrent(completed.plan, activeSourceFiles)) {
            deleteGenerated(completed.payload);
            return;
        }
        List<PrimaryKeyIndexSourceFile> sources = new ArrayList<>();
        boolean sourcesStillActive = true;
        for (DataFileMeta sourceFile : completed.sourceFiles) {
            sources.add(
                    new PrimaryKeyIndexSourceFile(sourceFile.fileName(), sourceFile.rowCount()));
            DataFileMeta active = activeSourceFiles.get(sourceFile.fileName());
            if (active == null || active.rowCount() != sourceFile.rowCount()) {
                sourcesStillActive = false;
            }
        }
        boolean inputsStillPresent = groups.containsAll(completed.inputGroups);
        boolean outputOverlapsRetainedGroup = false;
        for (DataFileMeta sourceFile : completed.sourceFiles) {
            if (isCovered(sourceFile, completed.inputGroups)) {
                outputOverlapsRetainedGroup = true;
                break;
            }
        }
        PrimaryKeyIndexSourceMeta outputSourceMeta;
        try {
            outputSourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(completed.payload);
        } catch (RuntimeException e) {
            deleteGenerated(completed.payload);
            return;
        }
        if (!sourcesStillActive
                || !inputsStillPresent
                || outputOverlapsRetainedGroup
                || outputSourceMeta.dataLevel() != completed.plan.dataLevel()
                || !outputSourceMeta.sourceFiles().equals(sources)) {
            deleteGenerated(completed.payload);
            return;
        }
        Optional<PkSortedIndexGroup> group;
        try {
            group =
                    PkSortedIndexGroup.create(
                            fieldId,
                            indexType,
                            sources,
                            Collections.singletonList(completed.payload));
        } catch (RuntimeException e) {
            deleteGenerated(completed.payload);
            throw new IllegalStateException(
                    "Primary-key " + indexType + " index build produced invalid metadata.", e);
        }
        if (!group.isPresent()) {
            deleteGenerated(completed.payload);
            throw new IllegalStateException(
                    "Primary-key " + indexType + " index build produced an incomplete group.");
        }
        replaceInputGroups(completed.inputGroups, group, created, removed);
    }

    private void replaceInputGroups(
            List<PkSortedIndexGroup> inputGroups,
            Optional<PkSortedIndexGroup> outputGroup,
            List<IndexFileMeta> created,
            List<IndexFileMeta> removed) {
        for (PkSortedIndexGroup inputGroup : inputGroups) {
            groups.remove(inputGroup);
            for (IndexFileMeta payload : inputGroup.payloads()) {
                if (created.remove(payload)) {
                    indexFile.delete(payload);
                } else {
                    removed.add(payload);
                }
            }
        }
        if (outputGroup.isPresent()) {
            groups.add(outputGroup.get());
            created.addAll(outputGroup.get().payloads());
        }
    }

    private void deleteGenerated(IndexFileMeta payload) {
        try {
            indexFile.delete(payload);
        } catch (RuntimeException e) {
            LOG.warn("Failed to delete unpublished primary-key sorted index payload.", e);
        }
    }

    public synchronized boolean buildNotCompleted() {
        return pendingBuild != null;
    }

    public synchronized boolean hasPendingMaintenance() {
        return pendingBuild != null
                || !pendingRestoredDeletions.isEmpty()
                || levels.pick(groups, activeSourceFiles).isPresent();
    }

    public int fieldId() {
        return fieldId;
    }

    public synchronized void withExecutor(ExecutorService executor) {
        checkArgument(pendingBuild == null, "Cannot replace executor during a sorted index build.");
        this.executor = executor;
    }

    public synchronized void close() {
        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            build.cancel();
        }
    }

    public synchronized PkSortedBucketIndexState state() {
        return PkSortedBucketIndexState.fromActiveDataFiles(
                fieldId, indexType, new ArrayList<>(activeSourceFiles.values()), activePayloads());
    }

    private List<IndexFileMeta> activePayloads() {
        List<IndexFileMeta> payloads = new ArrayList<>();
        for (PkSortedIndexGroup group : groups) {
            payloads.addAll(group.payloads());
        }
        return payloads;
    }

    private final class PendingBuild {

        private final PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup> plan;
        private final List<DataFileMeta> sourceFiles;
        private final List<PkSortedIndexGroup> inputGroups;
        @Nullable private IndexFileMeta result;
        @Nullable private Future<IndexFileMeta> future;
        private boolean cancelled;

        private PendingBuild(PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup> plan) {
            this.plan = plan;
            this.sourceFiles = plan.sourceFiles();
            this.inputGroups = plan.inputUnits();
        }

        private void start() {
            future =
                    executor.submit(
                            () -> {
                                IndexFileMeta payload = buildWithRetries();
                                synchronized (PendingBuild.this) {
                                    if (!cancelled) {
                                        result = payload;
                                        return payload;
                                    }
                                }
                                deleteGenerated(payload);
                                throw new CancellationException();
                            });
        }

        private IndexFileMeta buildWithRetries() throws Exception {
            for (int attempt = 1; ; attempt++) {
                try {
                    return buildFunction.build(sourceFiles);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException();
                } catch (Exception e) {
                    if (attempt >= MAX_BUILD_ATTEMPTS) {
                        throw e;
                    }
                    try {
                        Thread.sleep(INITIAL_RETRY_BACKOFF_MILLIS << (attempt - 1));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new CancellationException();
                    }
                }
            }
        }

        private boolean isDone() {
            return future.isDone();
        }

        private IndexFileMeta get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        private void cancel() {
            Future<IndexFileMeta> buildFuture;
            IndexFileMeta payload;
            synchronized (this) {
                cancelled = true;
                buildFuture = future;
                payload = result;
                result = null;
            }
            if (buildFuture != null) {
                buildFuture.cancel(true);
            }
            if (payload != null) {
                deleteGenerated(payload);
            }
        }
    }

    private static final class CompletedBuild {

        private final PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup> plan;
        private final List<DataFileMeta> sourceFiles;
        private final List<PkSortedIndexGroup> inputGroups;
        private final IndexFileMeta payload;

        private CompletedBuild(
                PrimaryKeyIndexLevels.Plan<PkSortedIndexGroup> plan, IndexFileMeta payload) {
            this.plan = plan;
            this.sourceFiles = plan.sourceFiles();
            this.inputGroups = plan.inputUnits();
            this.payload = payload;
        }
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

    /** Builds one payload for ordered physical source files. */
    @FunctionalInterface
    public interface BuildFunction {

        IndexFileMeta build(List<DataFileMeta> sourceFiles) throws Exception;
    }

    /** Sorted-index changes for append and compact snapshot routing. */
    public static final class SortedIndexCommit {

        private final Optional<SortedIndexIncrement> appendIncrement;
        private final Optional<SortedIndexIncrement> compactIncrement;
        private final AbortAction abortAction;

        private SortedIndexCommit(
                Optional<SortedIndexIncrement> appendIncrement,
                Optional<SortedIndexIncrement> compactIncrement,
                AbortAction abortAction) {
            this.appendIncrement = appendIncrement;
            this.compactIncrement = compactIncrement;
            this.abortAction = abortAction;
        }

        public Optional<SortedIndexIncrement> appendIncrement() {
            return appendIncrement;
        }

        public Optional<SortedIndexIncrement> compactIncrement() {
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

    /** Index-file additions and deletions emitted by one sorted definition. */
    public static final class SortedIndexIncrement {

        private final List<IndexFileMeta> newIndexFiles;
        private final List<IndexFileMeta> deletedIndexFiles;

        private SortedIndexIncrement(
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
