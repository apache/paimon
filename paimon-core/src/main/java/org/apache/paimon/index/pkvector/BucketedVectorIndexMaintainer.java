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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexLevels;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maintains bucket-local ANN payloads for eligible primary-key data files. */
public class BucketedVectorIndexMaintainer {

    private final int vectorFieldId;
    private final PkVectorAnnSegmentFile annSegmentFile;
    private final DataField vectorField;
    private final Options indexOptions;
    private final String metric;
    private final String algorithm;
    private final PkVectorDataFileReader.Factory vectorReaderFactory;
    private final DeletionVector.Factory deletionVectorFactory;
    private final PrimaryKeyIndexLevels<IndexFileMeta> annLevels;
    private ExecutorService executor;
    private final List<IndexFileMeta> annSegments;
    private final List<IndexFileMeta> retiredSegments;
    @Nullable private final CoreOptions coreOptions;
    private final Map<String, DataFileMeta> activeDataFiles = new LinkedHashMap<>();
    private final Map<String, DataFileMeta> activeSourceFiles = new LinkedHashMap<>();
    @Nullable private PendingBuild pendingBuild;

    BucketedVectorIndexMaintainer(
            int vectorFieldId,
            PkVectorAnnSegmentFile annSegmentFile,
            DataField vectorField,
            Options indexOptions,
            String metric,
            String algorithm,
            PkVectorDataFileReader.Factory vectorReaderFactory,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor) {
        this(
                vectorFieldId,
                annSegmentFile,
                vectorField,
                indexOptions,
                metric,
                algorithm,
                vectorReaderFactory,
                DeletionVector.emptyFactory(),
                restoredDataFiles,
                restoredPayloads,
                executor);
    }

    BucketedVectorIndexMaintainer(
            int vectorFieldId,
            PkVectorAnnSegmentFile annSegmentFile,
            DataField vectorField,
            Options indexOptions,
            String metric,
            String algorithm,
            PkVectorDataFileReader.Factory vectorReaderFactory,
            DeletionVector.Factory deletionVectorFactory,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor) {
        this(
                vectorFieldId,
                annSegmentFile,
                vectorField,
                indexOptions,
                metric,
                algorithm,
                vectorReaderFactory,
                deletionVectorFactory,
                restoredDataFiles,
                restoredPayloads,
                executor,
                null);
    }

    private BucketedVectorIndexMaintainer(
            int vectorFieldId,
            PkVectorAnnSegmentFile annSegmentFile,
            DataField vectorField,
            Options indexOptions,
            String metric,
            String algorithm,
            PkVectorDataFileReader.Factory vectorReaderFactory,
            DeletionVector.Factory deletionVectorFactory,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads,
            ExecutorService executor,
            @Nullable CoreOptions coreOptions) {
        this.vectorFieldId = vectorFieldId;
        this.annSegmentFile = annSegmentFile;
        this.vectorField = vectorField;
        this.indexOptions = indexOptions;
        this.metric = metric;
        this.algorithm = algorithm;
        this.vectorReaderFactory = vectorReaderFactory;
        this.deletionVectorFactory = deletionVectorFactory;
        this.annLevels =
                new PrimaryKeyIndexLevels<>(
                        segment -> sourceMeta(segment).dataLevel(),
                        segment -> sourceMeta(segment).sourceFiles());
        this.executor = executor;

        this.coreOptions = coreOptions;
        restoredDataFiles.forEach(file -> activeDataFiles.put(file.fileName(), file));
        refreshSourceFiles();

        List<IndexFileMeta> definitionPayloads = new ArrayList<>();
        for (IndexFileMeta payload : restoredPayloads) {
            if (algorithm.equals(payload.indexType())
                    && payload.globalIndexMeta() != null
                    && payload.globalIndexMeta().indexFieldId() == vectorFieldId) {
                definitionPayloads.add(payload);
            }
        }
        PkVectorBucketIndexState restoredState =
                PkVectorBucketIndexState.fromEligibleDataFiles(
                        vectorFieldId,
                        algorithm,
                        new ArrayList<>(activeSourceFiles.values()),
                        definitionPayloads);
        this.annSegments = new ArrayList<>(restoredState.annSegments());
        this.retiredSegments = new ArrayList<>(restoredState.staleSegments());
        validateCoverage(annSegments, activeSourceFiles);
    }

    public synchronized void notifyNewFiles(List<DataFileMeta> files) {
        files.forEach(file -> activeDataFiles.put(file.fileName(), file));
    }

    /** Produces ANN changes while optionally waiting for asynchronous ANN construction. */
    public synchronized VectorIndexCommit prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        checkArgument(
                appendIncrement.newFiles().stream()
                        .noneMatch(PrimaryKeyIndexSourcePolicy::shouldRead),
                "Append files must not be primary-key vector index sources.");

        List<IndexFileMeta> originalSegments = new ArrayList<>(annSegments);
        List<IndexFileMeta> originalRetired = new ArrayList<>(retiredSegments);
        Map<String, DataFileMeta> originalDataFiles = new LinkedHashMap<>(activeDataFiles);
        List<IndexFileMeta> generated = new ArrayList<>();
        try {
            appendIncrement.newFiles().forEach(file -> activeDataFiles.put(file.fileName(), file));
            appendIncrement.deletedFiles().forEach(file -> activeDataFiles.remove(file.fileName()));
            compactIncrement
                    .compactBefore()
                    .forEach(file -> activeDataFiles.remove(file.fileName()));
            compactIncrement
                    .compactAfter()
                    .forEach(file -> activeDataFiles.put(file.fileName(), file));
            refreshSourceFiles();

            List<IndexFileMeta> removed = new ArrayList<>(retiredSegments);
            retiredSegments.clear();

            List<IndexFileMeta> created = new ArrayList<>();
            // A source transition retires its old coverage before starting replacement builds.
            // Non-blocking preparation can then publish the data transition without stale indexes.
            removeSegments(
                    PkVectorBucketIndexState.fromEligibleDataFiles(
                                    vectorFieldId,
                                    algorithm,
                                    new ArrayList<>(activeSourceFiles.values()),
                                    annSegments)
                            .staleSegments(),
                    created,
                    removed);

            while (true) {
                Optional<CompletedBuild> completed = finishPendingBuild(waitCompaction);
                if (completed.isPresent()) {
                    CompletedBuild build = completed.get();
                    generated.add(build.segment);
                    if (canAccept(build)) {
                        replaceSegments(build, created, removed);
                    } else {
                        annSegmentFile.delete(build.segment);
                        generated.remove(build.segment);
                    }
                }

                if (pendingBuild == null) {
                    Optional<PrimaryKeyIndexLevels.Plan<IndexFileMeta>> plan =
                            annLevels.pick(annSegments, activeSourceFiles);
                    if (plan.isPresent()) {
                        if (plan.get().sourceFiles().isEmpty()) {
                            removeSegments(plan.get().inputUnits(), created, removed);
                            continue;
                        }
                        startPendingBuild(plan.get());
                    }
                }
                if (!waitCompaction || pendingBuild == null) {
                    break;
                }
            }

            validateCoverage(annSegments, activeSourceFiles);
            boolean hasCompactDataTransition =
                    !compactIncrement.compactBefore().isEmpty()
                            || !compactIncrement.compactAfter().isEmpty();
            boolean indexChanged = !created.isEmpty() || !removed.isEmpty();
            // Index-only backfills over promoted files need the compaction baseline check.
            boolean compactCommit = hasCompactDataTransition || usesDirectReadSources(created);
            Optional<VectorIndexIncrement> appendChange =
                    !indexChanged || compactCommit
                            ? Optional.empty()
                            : Optional.of(new VectorIndexIncrement(created, removed));
            Optional<VectorIndexIncrement> compactChange =
                    !indexChanged || !compactCommit
                            ? Optional.empty()
                            : Optional.of(new VectorIndexIncrement(created, removed));
            return new VectorIndexCommit(
                    appendChange,
                    compactChange,
                    failure ->
                            rollbackPrepareCommit(
                                    originalSegments,
                                    originalRetired,
                                    originalDataFiles,
                                    generated,
                                    failure));
        } catch (Throwable failure) {
            rollbackPrepareCommit(
                    originalSegments, originalRetired, originalDataFiles, generated, failure);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private void startPendingBuild(PrimaryKeyIndexLevels.Plan<IndexFileMeta> plan)
            throws IOException {
        PendingBuild build = new PendingBuild(plan);
        build.start();
        pendingBuild = build;
    }

    private synchronized void rollbackPrepareCommit(
            List<IndexFileMeta> originalSegments,
            List<IndexFileMeta> originalRetired,
            Map<String, DataFileMeta> originalDataFiles,
            List<IndexFileMeta> generated,
            Throwable failure) {
        annSegments.clear();
        annSegments.addAll(originalSegments);
        retiredSegments.clear();
        retiredSegments.addAll(originalRetired);
        activeDataFiles.clear();
        activeDataFiles.putAll(originalDataFiles);
        refreshSourceFiles();

        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            try {
                build.cancel();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        for (IndexFileMeta segment : generated) {
            try {
                annSegmentFile.delete(segment);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void replaceSegments(
            CompletedBuild build, List<IndexFileMeta> created, List<IndexFileMeta> removed) {
        removeSegments(build.inputSegments, created, removed);
        annSegments.add(build.segment);
        created.add(build.segment);
    }

    private void removeSegments(
            List<IndexFileMeta> segments,
            List<IndexFileMeta> created,
            List<IndexFileMeta> removed) {
        for (IndexFileMeta segment : segments) {
            checkArgument(annSegments.remove(segment), "ANN rebuild input is no longer active.");
            if (created.remove(segment)) {
                annSegmentFile.delete(segment);
            } else {
                removed.add(segment);
            }
        }
    }

    private Optional<CompletedBuild> finishPendingBuild(boolean blocking) throws Exception {
        if (pendingBuild == null || (!blocking && !pendingBuild.isDone())) {
            return Optional.empty();
        }

        PendingBuild completed = pendingBuild;
        try {
            IndexFileMeta segment = completed.get();
            pendingBuild = null;
            return Optional.of(new CompletedBuild(completed.plan, segment));
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
        if (!annLevels.isCurrent(build.plan, activeSourceFiles)
                || !annSegments.containsAll(build.inputSegments)) {
            return false;
        }
        PrimaryKeyIndexSourceMeta outputSourceMeta;
        try {
            outputSourceMeta = sourceMeta(build.segment);
        } catch (RuntimeException e) {
            return false;
        }
        if (outputSourceMeta.dataLevel() != build.plan.dataLevel()
                || outputSourceMeta.sourceFiles().size() != build.plan.sourceFiles().size()) {
            return false;
        }
        List<IndexFileMeta> retained = new ArrayList<>(annSegments);
        retained.removeAll(build.inputSegments);
        Set<String> covered = coveredSources(retained);
        for (int i = 0; i < outputSourceMeta.sourceFiles().size(); i++) {
            PrimaryKeyIndexSourceFile source = outputSourceMeta.sourceFiles().get(i);
            DataFileMeta expected = build.plan.sourceFiles().get(i);
            DataFileMeta activeFile = activeSourceFiles.get(source.fileName());
            if (!source.fileName().equals(expected.fileName())
                    || source.rowCount() != expected.rowCount()
                    || activeFile == null
                    || activeFile.rowCount() != source.rowCount()
                    || covered.contains(source.fileName())) {
                return false;
            }
        }
        return true;
    }

    public synchronized boolean buildNotCompleted() {
        return pendingBuild != null;
    }

    public synchronized void withExecutor(ExecutorService executor) {
        checkArgument(pendingBuild == null, "Cannot replace executor during an ANN build.");
        this.executor = executor;
    }

    public synchronized void close() {
        PendingBuild build = pendingBuild;
        pendingBuild = null;
        if (build != null) {
            build.cancel();
        }
    }

    private IndexFileMeta buildAnnSegment(
            List<DataFileMeta> files, Map<String, DeletionVector> deletionVectors) {
        try {
            List<PkVectorAnnSegmentFile.Source> sources = new ArrayList<>(files.size());
            for (DataFileMeta file : files) {
                PrimaryKeyIndexSourceFile sourceFile =
                        new PrimaryKeyIndexSourceFile(file.fileName(), file.rowCount());
                DeletionVector deletionVector = deletionVectors.get(file.fileName());
                LongPredicate excludedPosition =
                        deletionVector != null ? deletionVector::isDeleted : position -> false;
                sources.add(
                        PkVectorAnnSegmentFile.Source.lazy(
                                file.level(),
                                sourceFile,
                                () -> vectorReaderFactory.create(file),
                                excludedPosition));
            }
            return annSegmentFile.build(sources, vectorField, indexOptions, metric, algorithm);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build an ANN vector segment.", e);
        }
    }

    private Map<String, DeletionVector> snapshotDeletionVectors(List<DataFileMeta> files)
            throws IOException {
        Map<String, DeletionVector> snapshots = new LinkedHashMap<>();
        for (DataFileMeta file : files) {
            Optional<DeletionVector> deletionVector = deletionVectorFactory.create(file.fileName());
            if (deletionVector.isPresent() && !deletionVector.get().isEmpty()) {
                snapshots.put(
                        file.fileName(),
                        DeletionVector.deserializeFromBytes(
                                DeletionVector.serializeToBytes(deletionVector.get())));
            }
        }
        return snapshots;
    }

    private void validateCoverage(
            List<IndexFileMeta> candidateAnn, Map<String, DataFileMeta> sourceFiles) {
        PkVectorBucketIndexState state =
                PkVectorBucketIndexState.fromEligibleDataFiles(
                        vectorFieldId,
                        algorithm,
                        new ArrayList<>(sourceFiles.values()),
                        candidateAnn);
        checkArgument(
                state.staleSegments().isEmpty()
                        && state.annSegments().size() == candidateAnn.size(),
                "ANN segments must each exactly cover one current data level.");
        for (Map.Entry<String, IndexFileMeta> entry : state.sourceFileToAnnSegment().entrySet()) {
            DataFileMeta file = sourceFiles.get(entry.getKey());
            if (file == null) {
                continue;
            }
            PrimaryKeyIndexSourceMeta sourceMeta = sourceMeta(entry.getValue());
            for (PrimaryKeyIndexSourceFile source : sourceMeta.sourceFiles()) {
                if (source.fileName().equals(entry.getKey())) {
                    checkArgument(
                            source.rowCount() == file.rowCount(),
                            "ANN source %s row count does not match its active data file.",
                            source.fileName());
                }
            }
        }
    }

    public synchronized List<IndexFileMeta> segments() {
        return Collections.unmodifiableList(new ArrayList<>(annSegments));
    }

    public synchronized PkVectorBucketIndexState state() {
        return PkVectorBucketIndexState.fromEligibleDataFiles(
                vectorFieldId, algorithm, new ArrayList<>(activeSourceFiles.values()), annSegments);
    }

    private class PendingBuild {

        private final PrimaryKeyIndexLevels.Plan<IndexFileMeta> plan;
        private final List<DataFileMeta> sourceFiles;
        private final List<IndexFileMeta> inputSegments;
        private final Map<String, DeletionVector> deletionVectors;
        @Nullable private IndexFileMeta result;
        @Nullable private Future<IndexFileMeta> future;
        private boolean cancelled;

        private PendingBuild(PrimaryKeyIndexLevels.Plan<IndexFileMeta> plan) throws IOException {
            this.plan = plan;
            this.sourceFiles = plan.sourceFiles();
            this.inputSegments = plan.inputUnits();
            this.deletionVectors = snapshotDeletionVectors(sourceFiles);
        }

        private void start() {
            future =
                    executor.submit(
                            () -> {
                                IndexFileMeta segment =
                                        buildAnnSegment(sourceFiles, deletionVectors);
                                synchronized (PendingBuild.this) {
                                    if (!cancelled) {
                                        result = segment;
                                        return segment;
                                    }
                                }
                                annSegmentFile.delete(segment);
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
            IndexFileMeta segment;
            synchronized (this) {
                cancelled = true;
                buildFuture = future;
                segment = result;
                result = null;
            }
            if (buildFuture != null) {
                buildFuture.cancel(true);
            }
            if (segment != null) {
                annSegmentFile.delete(segment);
            }
        }
    }

    private static class CompletedBuild {

        private final PrimaryKeyIndexLevels.Plan<IndexFileMeta> plan;
        private final IndexFileMeta segment;
        private final List<IndexFileMeta> inputSegments;

        private CompletedBuild(
                PrimaryKeyIndexLevels.Plan<IndexFileMeta> plan, IndexFileMeta segment) {
            this.plan = plan;
            this.segment = segment;
            this.inputSegments = plan.inputUnits();
        }
    }

    private void refreshSourceFiles() {
        activeSourceFiles.clear();
        for (DataFileMeta file :
                PkVectorSourceFiles.eligible(
                        new ArrayList<>(activeDataFiles.values()), coreOptions)) {
            activeSourceFiles.put(file.fileName(), file);
        }
    }

    private boolean usesDirectReadSources(List<IndexFileMeta> segments) {
        return segments.stream()
                .flatMap(segment -> sourceMeta(segment).sourceFiles().stream())
                .anyMatch(
                        source ->
                                !PrimaryKeyIndexSourcePolicy.shouldRead(
                                        activeSourceFiles.get(source.fileName())));
    }

    private static Set<String> coveredSources(List<IndexFileMeta> segments) {
        Set<String> sources = new HashSet<>();
        for (IndexFileMeta ann : segments) {
            for (PrimaryKeyIndexSourceFile source : sourceMeta(ann).sourceFiles()) {
                sources.add(source.fileName());
            }
        }
        return sources;
    }

    private static PrimaryKeyIndexSourceMeta sourceMeta(IndexFileMeta ann) {
        return PrimaryKeyIndexSourceMeta.fromIndexFile(ann);
    }

    /** Vector index changes for Paimon's append snapshot followed by its compact snapshot. */
    public static class VectorIndexCommit {

        private final Optional<VectorIndexIncrement> appendIncrement;
        private final Optional<VectorIndexIncrement> compactIncrement;
        private final AbortAction abortAction;

        private VectorIndexCommit(
                Optional<VectorIndexIncrement> appendIncrement,
                Optional<VectorIndexIncrement> compactIncrement,
                AbortAction abortAction) {
            this.appendIncrement = appendIncrement;
            this.compactIncrement = compactIncrement;
            this.abortAction = abortAction;
        }

        public Optional<VectorIndexIncrement> appendIncrement() {
            return appendIncrement;
        }

        public Optional<VectorIndexIncrement> compactIncrement() {
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
    public static class VectorIndexIncrement {

        private final List<IndexFileMeta> newIndexFiles;
        private final List<IndexFileMeta> deletedIndexFiles;

        private VectorIndexIncrement(
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

    /** Factory to restore a bucket maintainer from data files and active ANN metadata. */
    public static class Factory {

        private final IndexFileHandler handler;
        private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
        private final DataField vectorField;
        private final int vectorDimension;
        private final String metric;
        private final String algorithm;
        private Options indexOptions;
        @Nullable private CoreOptions coreOptions;

        public Factory(
                IndexFileHandler handler,
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                DataField vectorField,
                String metric,
                String algorithm) {
            this.handler = handler;
            this.readerFactoryBuilder = readerFactoryBuilder;
            this.vectorField = vectorField;
            checkArgument(
                    vectorField.type() instanceof VectorType,
                    "Primary-key vector field must have VECTOR type.");
            this.vectorDimension = ((VectorType) vectorField.type()).getLength();
            this.metric = metric;
            this.algorithm = algorithm;
        }

        public Factory withIndexOptions(Options indexOptions) {
            this.indexOptions = new Options(indexOptions.toMap());
            return this;
        }

        public Factory withCoreOptions(CoreOptions coreOptions) {
            this.coreOptions = coreOptions;
            return this;
        }

        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        public BucketedVectorIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor) {
            return create(
                    partition,
                    bucket,
                    restoredDataFiles,
                    restoredPayloads,
                    executor,
                    DeletionVector.emptyFactory());
        }

        public BucketedVectorIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor,
                DeletionVector.Factory deletionVectorFactory) {
            checkArgument(indexOptions != null, "ANN index options are not configured.");
            List<DataFileMeta> dataFiles =
                    restoredDataFiles == null ? Collections.emptyList() : restoredDataFiles;
            List<IndexFileMeta> payloads =
                    restoredPayloads == null ? Collections.emptyList() : restoredPayloads;
            return new BucketedVectorIndexMaintainer(
                    vectorField.id(),
                    handler.pkVectorAnnSegment(partition, bucket),
                    vectorField,
                    indexOptions,
                    metric,
                    algorithm,
                    new PkVectorDataFileReader.Factory(
                            readerFactoryBuilder, partition, bucket, vectorField, vectorDimension),
                    deletionVectorFactory,
                    dataFiles,
                    payloads,
                    executor,
                    coreOptions);
        }
    }
}
