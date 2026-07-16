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
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maintains bucket-local ANN payloads from complete compact-output data files. */
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
    private final Map<String, DataFileMeta> activeSourceFiles;
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
        this.vectorFieldId = vectorFieldId;
        this.annSegmentFile = annSegmentFile;
        this.vectorField = vectorField;
        this.indexOptions = indexOptions;
        this.metric = metric;
        this.algorithm = algorithm;
        this.vectorReaderFactory = vectorReaderFactory;
        this.deletionVectorFactory = deletionVectorFactory;
        CoreOptions coreOptions = new CoreOptions(indexOptions);
        this.annLevels =
                new PrimaryKeyIndexLevels<>(
                        coreOptions.primaryKeyIndexCompactionLevelFanout(vectorField.name()),
                        coreOptions.primaryKeyIndexCompactionStaleRatioThreshold(
                                vectorField.name()),
                        IndexFileMeta::fileName,
                        segment -> sourceMeta(segment).sourceFiles());
        this.executor = executor;

        List<IndexFileMeta> definitionPayloads = new ArrayList<>();
        for (IndexFileMeta payload : restoredPayloads) {
            if (algorithm.equals(payload.indexType())
                    && payload.globalIndexMeta() != null
                    && payload.globalIndexMeta().indexFieldId() == vectorFieldId) {
                definitionPayloads.add(payload);
            }
        }
        PkVectorBucketIndexState restoredState =
                new PkVectorBucketIndexState(vectorFieldId, algorithm, definitionPayloads);
        this.annSegments = new ArrayList<>(restoredState.annSegments());
        this.activeSourceFiles = new LinkedHashMap<>();
        for (DataFileMeta file : restoredDataFiles) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                activeSourceFiles.put(file.fileName(), file);
            }
        }
        validateCoverage(annSegments, activeSourceFiles);
    }

    /** Produces ANN changes while optionally waiting for asynchronous ANN construction. */
    public synchronized VectorIndexCommit prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        checkArgument(
                eligibleFiles(appendIncrement.newFiles()).isEmpty(),
                "Append files must not be primary-key vector index sources.");

        List<IndexFileMeta> originalSegments = new ArrayList<>(annSegments);
        Map<String, DataFileMeta> originalSourceFiles = new LinkedHashMap<>(activeSourceFiles);
        List<IndexFileMeta> generated = new ArrayList<>();
        try {
            Map<String, DataFileMeta> nextSources = new LinkedHashMap<>(activeSourceFiles);
            for (DataFileMeta file : compactIncrement.compactBefore()) {
                if (!containsFile(compactIncrement.compactAfter(), file.fileName())) {
                    nextSources.remove(file.fileName());
                }
            }
            for (DataFileMeta file : compactIncrement.compactAfter()) {
                if (PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                    nextSources.put(file.fileName(), file);
                }
            }

            List<IndexFileMeta> removed = new ArrayList<>();

            List<IndexFileMeta> created = new ArrayList<>();
            activeSourceFiles.clear();
            activeSourceFiles.putAll(nextSources);

            while (true) {
                Optional<CompletedBuild> completed = finishPendingBuild(waitCompaction);
                if (completed.isPresent()) {
                    CompletedBuild build = completed.get();
                    generated.add(build.segment);
                    if (canAccept(build)) {
                        replaceSegments(build, created, removed);
                    } else {
                        annSegmentFile.delete(build.segment);
                    }
                }

                if (pendingBuild == null) {
                    List<DataFileMeta> uncovered = uncoveredFiles();
                    if (!uncovered.isEmpty()) {
                        startPendingBuild(uncovered, Collections.<IndexFileMeta>emptyList());
                    } else {
                        Optional<PrimaryKeyIndexLevels.Plan<IndexFileMeta>> plan =
                                annLevels.pick(annSegments, activeSourceFiles);
                        if (plan.isPresent()) {
                            if (plan.get().sourceFiles().isEmpty()) {
                                removeSegments(plan.get().inputUnits(), created, removed);
                                continue;
                            }
                            startPendingBuild(plan.get().sourceFiles(), plan.get().inputUnits());
                        }
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
            Optional<VectorIndexIncrement> appendChange =
                    !indexChanged || hasCompactDataTransition
                            ? Optional.empty()
                            : Optional.of(new VectorIndexIncrement(created, removed));
            Optional<VectorIndexIncrement> compactChange =
                    !indexChanged || !hasCompactDataTransition
                            ? Optional.empty()
                            : Optional.of(new VectorIndexIncrement(created, removed));
            return new VectorIndexCommit(
                    appendChange,
                    compactChange,
                    failure ->
                            rollbackPrepareCommit(
                                    originalSegments, originalSourceFiles, generated, failure));
        } catch (Throwable failure) {
            rollbackPrepareCommit(originalSegments, originalSourceFiles, generated, failure);
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private void startPendingBuild(
            List<DataFileMeta> sourceFiles, List<IndexFileMeta> inputSegments) throws IOException {
        PendingBuild build = new PendingBuild(sourceFiles, inputSegments);
        build.start();
        pendingBuild = build;
    }

    private synchronized void rollbackPrepareCommit(
            List<IndexFileMeta> originalSegments,
            Map<String, DataFileMeta> originalSourceFiles,
            List<IndexFileMeta> generated,
            Throwable failure) {
        annSegments.clear();
        annSegments.addAll(originalSegments);
        activeSourceFiles.clear();
        activeSourceFiles.putAll(originalSourceFiles);

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

    private List<DataFileMeta> uncoveredFiles() {
        Set<String> covered = coveredSources(annSegments);
        List<DataFileMeta> uncovered = new ArrayList<>();
        for (DataFileMeta file : activeSourceFiles.values()) {
            if (!covered.contains(file.fileName())) {
                uncovered.add(file);
            }
        }
        uncovered.sort(Comparator.comparing(DataFileMeta::fileName));
        return uncovered;
    }

    private Optional<CompletedBuild> finishPendingBuild(boolean blocking) throws Exception {
        if (pendingBuild == null || (!blocking && !pendingBuild.isDone())) {
            return Optional.empty();
        }

        PendingBuild completed = pendingBuild;
        try {
            IndexFileMeta segment = completed.get();
            pendingBuild = null;
            return Optional.of(new CompletedBuild(segment, completed.inputSegments));
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
        if (!annSegments.containsAll(build.inputSegments)) {
            return false;
        }
        List<IndexFileMeta> retained = new ArrayList<>(annSegments);
        retained.removeAll(build.inputSegments);
        Set<String> covered = coveredSources(retained);
        for (PrimaryKeyIndexSourceFile source : sourceMeta(build.segment).sourceFiles()) {
            DataFileMeta activeFile = activeSourceFiles.get(source.fileName());
            if (activeFile == null
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
                new PkVectorBucketIndexState(vectorFieldId, algorithm, candidateAnn);
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
        return new PkVectorBucketIndexState(vectorFieldId, algorithm, annSegments);
    }

    private class PendingBuild {

        private final List<DataFileMeta> sourceFiles;
        private final List<IndexFileMeta> inputSegments;
        private final Map<String, DeletionVector> deletionVectors;
        @Nullable private IndexFileMeta result;
        @Nullable private Future<IndexFileMeta> future;
        private boolean cancelled;

        private PendingBuild(List<DataFileMeta> sourceFiles, List<IndexFileMeta> inputSegments)
                throws IOException {
            this.sourceFiles = new ArrayList<>(sourceFiles);
            this.inputSegments = new ArrayList<>(inputSegments);
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

        private final IndexFileMeta segment;
        private final List<IndexFileMeta> inputSegments;

        private CompletedBuild(IndexFileMeta segment, List<IndexFileMeta> inputSegments) {
            this.segment = segment;
            this.inputSegments = new ArrayList<>(inputSegments);
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
                    executor);
        }
    }
}
