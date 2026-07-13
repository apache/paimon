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
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

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
                PkSortedBucketIndexState.fromActivePayloads(
                        fieldId, indexType, sourceFiles(), definitionPayloads);
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
            removed.addAll(removeInactiveGroups());
            Set<String> failedSources = new HashSet<>();
            while (true) {
                Optional<CompletedBuild> completed =
                        finishPendingBuild(waitCompaction, failedSources);
                if (completed.isPresent()) {
                    acceptOrDelete(completed.get(), created, failedSources);
                }

                if (pendingBuild == null && allowBuildStart) {
                    DataFileMeta uncovered = firstUncoveredSource(failedSources);
                    if (uncovered != null) {
                        PendingBuild next = new PendingBuild(uncovered);
                        try {
                            next.start();
                            pendingBuild = next;
                        } catch (RejectedExecutionException e) {
                            failedSources.add(sourceIdentity(uncovered));
                            LOG.warn(
                                    "Primary-key {} index build for source file {} was rejected.",
                                    indexType,
                                    uncovered.fileName(),
                                    e);
                        }
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
            return new SortedIndexCommit(appendChange, compactChange);
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

    private void rollbackPrepareCommit(
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

    private List<IndexFileMeta> removeInactiveGroups() {
        List<IndexFileMeta> removed = new ArrayList<>();
        Iterator<PkSortedIndexGroup> iterator = groups.iterator();
        while (iterator.hasNext()) {
            PkSortedIndexGroup group = iterator.next();
            DataFileMeta active = activeSourceFiles.get(group.sourceFile().fileName());
            if (active == null || active.rowCount() != group.sourceFile().rowCount()) {
                iterator.remove();
                removed.addAll(group.payloads());
            }
        }
        return removed;
    }

    @Nullable
    private DataFileMeta firstUncoveredSource(Set<String> failedSources) {
        List<DataFileMeta> candidates = new ArrayList<>(activeSourceFiles.values());
        candidates.sort(Comparator.comparing(DataFileMeta::fileName));
        for (DataFileMeta candidate : candidates) {
            if (!isCovered(candidate) && !failedSources.contains(sourceIdentity(candidate))) {
                return candidate;
            }
        }
        return null;
    }

    private boolean isCovered(DataFileMeta candidate) {
        for (PkSortedIndexGroup group : groups) {
            if (group.sourceFile().fileName().equals(candidate.fileName())
                    && group.sourceFile().rowCount() == candidate.rowCount()) {
                return true;
            }
        }
        return false;
    }

    private Optional<CompletedBuild> finishPendingBuild(boolean blocking, Set<String> failedSources)
            throws InterruptedException {
        if (pendingBuild == null || (!blocking && !pendingBuild.isDone())) {
            return Optional.empty();
        }
        PendingBuild completed = pendingBuild;
        try {
            List<IndexFileMeta> payloads = completed.get();
            pendingBuild = null;
            return Optional.of(new CompletedBuild(completed.sourceFile, payloads));
        } catch (CancellationException e) {
            pendingBuild = null;
            failedSources.add(sourceIdentity(completed.sourceFile));
            return Optional.empty();
        } catch (ExecutionException e) {
            pendingBuild = null;
            failedSources.add(sourceIdentity(completed.sourceFile));
            LOG.warn(
                    "Primary-key {} index build for source file {} failed after {} attempts; "
                            + "the source remains uncovered.",
                    indexType,
                    completed.sourceFile.fileName(),
                    MAX_BUILD_ATTEMPTS,
                    e.getCause());
            return Optional.empty();
        }
    }

    private void acceptOrDelete(
            CompletedBuild completed, List<IndexFileMeta> created, Set<String> failedSources) {
        DataFileMeta active = activeSourceFiles.get(completed.sourceFile.fileName());
        PrimaryKeyIndexSourceFile source =
                new PrimaryKeyIndexSourceFile(
                        completed.sourceFile.fileName(), completed.sourceFile.rowCount());
        Optional<PkSortedIndexGroup> group;
        try {
            group = PkSortedIndexGroup.create(fieldId, indexType, source, completed.payloads);
        } catch (RuntimeException e) {
            failedSources.add(sourceIdentity(completed.sourceFile));
            deleteGenerated(completed.payloads);
            LOG.warn(
                    "Primary-key {} index build for source file {} produced invalid metadata.",
                    indexType,
                    completed.sourceFile.fileName(),
                    e);
            return;
        }
        if (active == null
                || active.rowCount() != completed.sourceFile.rowCount()
                || isCovered(active)
                || !group.isPresent()) {
            deleteGenerated(completed.payloads);
            failedSources.add(sourceIdentity(completed.sourceFile));
            return;
        }
        groups.add(group.get());
        created.addAll(completed.payloads);
    }

    private void deleteGenerated(List<IndexFileMeta> payloads) {
        for (IndexFileMeta payload : payloads) {
            try {
                indexFile.delete(payload);
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete unpublished primary-key sorted index payload.", e);
            }
        }
    }

    public synchronized boolean buildNotCompleted() {
        return pendingBuild != null;
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
        return PkSortedBucketIndexState.fromActivePayloads(
                fieldId, indexType, sourceFiles(), activePayloads());
    }

    private List<PrimaryKeyIndexSourceFile> sourceFiles() {
        List<PrimaryKeyIndexSourceFile> sources = new ArrayList<>();
        for (DataFileMeta dataFile : activeSourceFiles.values()) {
            sources.add(new PrimaryKeyIndexSourceFile(dataFile.fileName(), dataFile.rowCount()));
        }
        return sources;
    }

    private List<IndexFileMeta> activePayloads() {
        List<IndexFileMeta> payloads = new ArrayList<>();
        for (PkSortedIndexGroup group : groups) {
            payloads.addAll(group.payloads());
        }
        return payloads;
    }

    private final class PendingBuild {

        private final DataFileMeta sourceFile;
        @Nullable private List<IndexFileMeta> result;
        @Nullable private Future<List<IndexFileMeta>> future;
        private boolean cancelled;

        private PendingBuild(DataFileMeta sourceFile) {
            this.sourceFile = sourceFile;
        }

        private void start() {
            future =
                    executor.submit(
                            () -> {
                                List<IndexFileMeta> payloads = buildWithRetries();
                                synchronized (PendingBuild.this) {
                                    if (!cancelled) {
                                        result = payloads;
                                        return payloads;
                                    }
                                }
                                deleteGenerated(payloads);
                                throw new CancellationException();
                            });
        }

        private List<IndexFileMeta> buildWithRetries() throws Exception {
            for (int attempt = 1; ; attempt++) {
                try {
                    return buildFunction.build(sourceFile);
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

        private List<IndexFileMeta> get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        private void cancel() {
            Future<List<IndexFileMeta>> buildFuture;
            List<IndexFileMeta> payloads;
            synchronized (this) {
                cancelled = true;
                buildFuture = future;
                payloads = result;
                result = null;
            }
            if (buildFuture != null) {
                buildFuture.cancel(true);
            }
            if (payloads != null) {
                deleteGenerated(payloads);
            }
        }
    }

    private static final class CompletedBuild {

        private final DataFileMeta sourceFile;
        private final List<IndexFileMeta> payloads;

        private CompletedBuild(DataFileMeta sourceFile, List<IndexFileMeta> payloads) {
            this.sourceFile = sourceFile;
            this.payloads = payloads;
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

    private static String sourceIdentity(DataFileMeta file) {
        return file.fileName() + '\0' + file.rowCount();
    }

    /** Builds all rotated payloads for one physical source file. */
    @FunctionalInterface
    public interface BuildFunction {

        List<IndexFileMeta> build(DataFileMeta sourceFile) throws Exception;
    }

    /** Sorted-index changes for append and compact snapshot routing. */
    public static final class SortedIndexCommit {

        private final Optional<SortedIndexIncrement> appendIncrement;
        private final Optional<SortedIndexIncrement> compactIncrement;

        private SortedIndexCommit(
                Optional<SortedIndexIncrement> appendIncrement,
                Optional<SortedIndexIncrement> compactIncrement) {
            this.appendIncrement = appendIncrement;
            this.compactIncrement = compactIncrement;
        }

        public Optional<SortedIndexIncrement> appendIncrement() {
            return appendIncrement;
        }

        public Optional<SortedIndexIncrement> compactIncrement() {
            return compactIncrement;
        }
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
