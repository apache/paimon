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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
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
    private final List<IndexFileMeta> annSegments;
    private final Map<String, DataFileMeta> activeSourceFiles;

    BucketedVectorIndexMaintainer(
            int vectorFieldId,
            PkVectorAnnSegmentFile annSegmentFile,
            DataField vectorField,
            Options indexOptions,
            String metric,
            String algorithm,
            PkVectorDataFileReader.Factory vectorReaderFactory,
            List<DataFileMeta> restoredDataFiles,
            List<IndexFileMeta> restoredPayloads) {
        this.vectorFieldId = vectorFieldId;
        this.annSegmentFile = annSegmentFile;
        this.vectorField = vectorField;
        this.indexOptions = indexOptions;
        this.metric = metric;
        this.algorithm = algorithm;
        this.vectorReaderFactory = vectorReaderFactory;
        PkVectorBucketIndexState restoredState =
                new PkVectorBucketIndexState(vectorFieldId, algorithm, restoredPayloads);
        this.annSegments = new ArrayList<>(restoredState.annSegments());
        this.activeSourceFiles = new LinkedHashMap<>();
        for (DataFileMeta file : restoredDataFiles) {
            if (PkVectorSourcePolicy.shouldRead(file)) {
                activeSourceFiles.put(file.fileName(), file);
            }
        }
        validateCoverage(annSegments, activeSourceFiles);
    }

    /** Produces ANN changes in the same compact increment as their source data-file transition. */
    public synchronized VectorIndexCommit prepareCommit(
            DataIncrement appendIncrement, CompactIncrement compactIncrement) {
        checkArgument(
                eligibleFiles(appendIncrement.newFiles()).isEmpty(),
                "Append files must not be primary-key vector index sources.");

        Map<String, DataFileMeta> nextSources = new LinkedHashMap<>(activeSourceFiles);
        Set<String> deletedFiles = new HashSet<>();
        for (DataFileMeta file : compactIncrement.compactBefore()) {
            if (!containsFile(compactIncrement.compactAfter(), file.fileName())) {
                deletedFiles.add(file.fileName());
                nextSources.remove(file.fileName());
            }
        }
        for (DataFileMeta file : compactIncrement.compactAfter()) {
            if (PkVectorSourcePolicy.shouldRead(file)) {
                nextSources.put(file.fileName(), file);
            }
        }

        List<IndexFileMeta> retained = new ArrayList<>();
        List<IndexFileMeta> removed = new ArrayList<>();
        for (IndexFileMeta ann : annSegments) {
            if (referencesAny(ann, deletedFiles)) {
                removed.add(ann);
            } else {
                retained.add(ann);
            }
        }

        Set<String> covered = coveredSources(retained);
        List<DataFileMeta> uncovered = new ArrayList<>();
        for (DataFileMeta file : nextSources.values()) {
            if (!covered.contains(file.fileName())) {
                uncovered.add(file);
            }
        }
        uncovered.sort(Comparator.comparing(DataFileMeta::fileName));

        List<IndexFileMeta> created = new ArrayList<>();
        try {
            if (!uncovered.isEmpty()) {
                created.add(buildAnnSegment(uncovered));
            }
            List<IndexFileMeta> nextAnn = new ArrayList<>(retained);
            nextAnn.addAll(created);
            validateCoverage(nextAnn, nextSources);

            activeSourceFiles.clear();
            activeSourceFiles.putAll(nextSources);
            annSegments.clear();
            annSegments.addAll(nextAnn);
            Optional<VectorIndexIncrement> compactChange =
                    created.isEmpty() && removed.isEmpty()
                            ? Optional.empty()
                            : Optional.of(new VectorIndexIncrement(created, removed));
            return new VectorIndexCommit(Optional.empty(), compactChange);
        } catch (RuntimeException e) {
            deleteAnnSegments(created);
            throw e;
        }
    }

    private IndexFileMeta buildAnnSegment(List<DataFileMeta> files) {
        try {
            List<PkVectorAnnSegmentFile.Source> sources = new ArrayList<>(files.size());
            for (DataFileMeta file : files) {
                PkVectorSourceFile sourceFile =
                        new PkVectorSourceFile(file.fileName(), file.rowCount());
                sources.add(
                        PkVectorAnnSegmentFile.Source.lazy(
                                sourceFile, () -> vectorReaderFactory.create(file)));
            }
            return annSegmentFile.build(sources, vectorField, indexOptions, metric, algorithm);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build an ANN vector segment.", e);
        }
    }

    private void validateCoverage(
            List<IndexFileMeta> candidateAnn, Map<String, DataFileMeta> sourceFiles) {
        PkVectorBucketIndexState state =
                new PkVectorBucketIndexState(vectorFieldId, algorithm, candidateAnn);
        for (Map.Entry<String, IndexFileMeta> entry : state.sourceFileToAnnSegment().entrySet()) {
            DataFileMeta file = sourceFiles.get(entry.getKey());
            checkArgument(
                    file != null,
                    "ANN segment %s references inactive data file %s.",
                    entry.getValue().fileName(),
                    entry.getKey());
            PkVectorSourceMeta sourceMeta = sourceMeta(entry.getValue());
            for (PkVectorSourceFile source : sourceMeta.sourceFiles()) {
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

    private void deleteAnnSegments(List<IndexFileMeta> segments) {
        for (IndexFileMeta segment : segments) {
            annSegmentFile.delete(segment);
        }
    }

    private static List<DataFileMeta> eligibleFiles(List<DataFileMeta> files) {
        List<DataFileMeta> eligible = new ArrayList<>();
        for (DataFileMeta file : files) {
            if (PkVectorSourcePolicy.shouldRead(file)) {
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

    private static boolean referencesAny(IndexFileMeta ann, Set<String> dataFiles) {
        for (PkVectorSourceFile source : sourceMeta(ann).sourceFiles()) {
            if (dataFiles.contains(source.fileName())) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> coveredSources(List<IndexFileMeta> segments) {
        Set<String> sources = new HashSet<>();
        for (IndexFileMeta ann : segments) {
            for (PkVectorSourceFile source : sourceMeta(ann).sourceFiles()) {
                sources.add(source.fileName());
            }
        }
        return sources;
    }

    private static PkVectorSourceMeta sourceMeta(IndexFileMeta ann) {
        return PkVectorSourceMeta.fromIndexFile(ann);
    }

    /** Vector index changes for Paimon's append snapshot followed by its compact snapshot. */
    public static class VectorIndexCommit {

        private final Optional<VectorIndexIncrement> appendIncrement;
        private final Optional<VectorIndexIncrement> compactIncrement;

        private VectorIndexCommit(
                Optional<VectorIndexIncrement> appendIncrement,
                Optional<VectorIndexIncrement> compactIncrement) {
            this.appendIncrement = appendIncrement;
            this.compactIncrement = compactIncrement;
        }

        public Optional<VectorIndexIncrement> appendIncrement() {
            return appendIncrement;
        }

        public Optional<VectorIndexIncrement> compactIncrement() {
            return compactIncrement;
        }
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
                @Nullable List<IndexFileMeta> restoredPayloads) {
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
                    dataFiles,
                    payloads);
        }
    }
}
