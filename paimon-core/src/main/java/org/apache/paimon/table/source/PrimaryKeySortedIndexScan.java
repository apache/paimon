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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pksorted.PkSortedBucketIndexState;
import org.apache.paimon.index.pksorted.PkSortedIndexGroup;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Plans source-backed scalar index groups in file-local row-position space. */
public final class PrimaryKeySortedIndexScan {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeySortedIndexScan.class);

    private PrimaryKeySortedIndexScan() {}

    @FunctionalInterface
    interface ReaderFactory {

        GlobalIndexReader create(
                FilePlan file, PrimaryKeyIndexDefinition definition, List<IndexFileMeta> payloads);
    }

    static ReaderFactory readerFactory(
            FileIO fileIO, FileStorePathFactory pathFactory, RowType rowType, Options options) {
        IndexFilePathFactories pathFactories = new IndexFilePathFactories(pathFactory);
        ExecutorService executor =
                GlobalIndexReadThreadPool.getExecutorService(options.get(GLOBAL_INDEX_THREAD_NUM));
        GlobalIndexFileReader fileReader = meta -> fileIO.newInputStream(meta.filePath());
        return (file, definition, payloads) -> {
            IndexPathFactory indexPathFactory =
                    pathFactories.get(file.sourceSplit().partition(), file.sourceSplit().bucket());
            List<GlobalIndexIOMeta> ioMetas = new ArrayList<>(payloads.size());
            for (IndexFileMeta payload : payloads) {
                GlobalIndexMeta meta = checkNotNull(payload.globalIndexMeta());
                ioMetas.add(
                        new GlobalIndexIOMeta(
                                indexPathFactory.toPath(payload),
                                payload.fileSize(),
                                meta.indexMeta()));
            }
            GlobalIndexer indexer =
                    GlobalIndexer.create(
                            definition.indexType(),
                            rowType.getField(definition.fieldId()),
                            definition.options());
            return indexer.createReader(fileReader, ioMetas, executor);
        };
    }

    static Plan plan(
            long snapshotId,
            List<DataSplit> dataSplits,
            List<PrimaryKeyIndexDefinition> definitions,
            List<IndexManifestEntry> indexEntries) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> payloadsByBucket = new LinkedHashMap<>();
        for (IndexManifestEntry entry : indexEntries) {
            IndexFileMeta payload = entry.indexFile();
            GlobalIndexMeta meta = payload.globalIndexMeta();
            if (entry.kind() != FileKind.ADD || meta == null || meta.sourceMeta() == null) {
                continue;
            }
            Pair<BinaryRow, Integer> bucket = Pair.of(entry.partition(), entry.bucket());
            payloadsByBucket.computeIfAbsent(bucket, ignored -> new ArrayList<>()).add(payload);
        }

        List<PrimaryKeyIndexDefinition> scalarDefinitions = new ArrayList<>();
        for (PrimaryKeyIndexDefinition definition : definitions) {
            if (definition.family() == PrimaryKeyIndexDefinition.Family.BTREE
                    || definition.family() == PrimaryKeyIndexDefinition.Family.BITMAP) {
                scalarDefinitions.add(definition);
            }
        }

        Map<Pair<BinaryRow, Integer>, List<PrimaryKeyIndexSourceFile>> sourcesByBucket =
                new LinkedHashMap<>();
        for (DataSplit split : dataSplits) {
            checkArgument(
                    split.snapshotId() == snapshotId,
                    "Data split snapshot %s does not match sorted-index scan snapshot %s.",
                    split.snapshotId(),
                    snapshotId);
            checkArgument(
                    !split.isStreaming(), "Primary-key sorted-index scan requires batch splits.");
            List<DeletionFile> deletions = split.deletionFiles().orElse(null);
            checkArgument(
                    deletions == null || deletions.size() == split.dataFiles().size(),
                    "Deletion files must align with data files in a sorted-index split.");
            List<PrimaryKeyIndexSourceFile> sources =
                    sourcesByBucket.computeIfAbsent(
                            Pair.of(split.partition(), split.bucket()),
                            ignored -> new ArrayList<>());
            for (DataFileMeta dataFile : split.dataFiles()) {
                sources.add(
                        new PrimaryKeyIndexSourceFile(dataFile.fileName(), dataFile.rowCount()));
            }
        }

        Map<Pair<BinaryRow, Integer>, Map<String, Map<Integer, PkSortedIndexGroup>>>
                groupsByBucket = new LinkedHashMap<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<PrimaryKeyIndexSourceFile>> bucketEntry :
                sourcesByBucket.entrySet()) {
            Pair<BinaryRow, Integer> bucket = bucketEntry.getKey();
            List<IndexFileMeta> bucketPayloads =
                    payloadsByBucket.getOrDefault(bucket, Collections.emptyList());
            Map<String, Map<Integer, PkSortedIndexGroup>> groupsBySource = new LinkedHashMap<>();
            for (PrimaryKeyIndexDefinition definition : scalarDefinitions) {
                List<IndexFileMeta> definitionPayloads = new ArrayList<>();
                for (IndexFileMeta payload : bucketPayloads) {
                    GlobalIndexMeta meta = payload.globalIndexMeta();
                    if (meta != null
                            && definition.indexType().equals(payload.indexType())
                            && definition.fieldId() == meta.indexFieldId()) {
                        definitionPayloads.add(payload);
                    }
                }
                try {
                    PkSortedBucketIndexState state =
                            PkSortedBucketIndexState.fromActivePayloads(
                                    definition.fieldId(),
                                    definition.indexType(),
                                    bucketEntry.getValue(),
                                    definitionPayloads);
                    for (PkSortedIndexGroup group : state.groups()) {
                        groupsBySource
                                .computeIfAbsent(
                                        group.sourceFile().fileName(),
                                        ignored -> new LinkedHashMap<>())
                                .put(definition.fieldId(), group);
                    }
                } catch (RuntimeException e) {
                    rethrowIfInterrupted(e);
                    LOG.warn(
                            "Failed to plan primary-key sorted index for partition {}, bucket {} "
                                    + "and field {}; falling back to a raw scan for this field.",
                            bucket.getKey(),
                            bucket.getValue(),
                            definition.fieldId(),
                            e);
                }
            }
            groupsByBucket.put(bucket, groupsBySource);
        }

        List<FilePlan> files = new ArrayList<>();
        for (DataSplit split : dataSplits) {
            Map<String, Map<Integer, PkSortedIndexGroup>> groupsBySource =
                    groupsByBucket.getOrDefault(
                            Pair.of(split.partition(), split.bucket()), Collections.emptyMap());
            for (int fileIndex = 0; fileIndex < split.dataFiles().size(); fileIndex++) {
                DataFileMeta dataFile = split.dataFiles().get(fileIndex);
                Map<Integer, PkSortedIndexGroup> groups =
                        groupsBySource.getOrDefault(dataFile.fileName(), Collections.emptyMap());
                files.add(new FilePlan(split, fileIndex, groups));
            }
        }
        return new Plan(snapshotId, files);
    }

    static EvaluatedPlan evaluate(
            Plan plan,
            RowType rowType,
            Predicate predicate,
            List<PrimaryKeyIndexDefinition> definitions,
            ReaderFactory readerFactory) {
        Map<Integer, PrimaryKeyIndexDefinition> definitionsByField = new LinkedHashMap<>();
        for (PrimaryKeyIndexDefinition definition : definitions) {
            if (definition.family() == PrimaryKeyIndexDefinition.Family.BTREE
                    || definition.family() == PrimaryKeyIndexDefinition.Family.BITMAP) {
                definitionsByField.put(definition.fieldId(), definition);
            }
        }

        List<EvaluatedFile> files = new ArrayList<>();
        for (FilePlan file : plan.files()) {
            GlobalIndexEvaluator evaluator =
                    new GlobalIndexEvaluator(
                            rowType,
                            fieldId -> {
                                PrimaryKeyIndexDefinition definition =
                                        definitionsByField.get(fieldId);
                                Optional<PkSortedIndexGroup> group = file.group(fieldId);
                                if (definition == null || !group.isPresent()) {
                                    return Collections.emptyList();
                                }
                                return Collections.singletonList(
                                        readerFactory.create(
                                                file, definition, group.get().payloads()));
                            });
            Optional<GlobalIndexResult> result;
            try {
                result = evaluator.evaluate(predicate);
            } catch (RuntimeException e) {
                rethrowIfInterrupted(e);
                LOG.warn(
                        "Failed to evaluate primary-key sorted index for data file {}; "
                                + "falling back to a raw scan for this file.",
                        file.dataFile().fileName(),
                        e);
                result = Optional.empty();
            } finally {
                evaluator.close();
            }
            files.add(new EvaluatedFile(file, result));
        }
        return new EvaluatedPlan(plan.snapshotId(), files);
    }

    private static void rethrowIfInterrupted(RuntimeException exception) {
        if (Thread.currentThread().isInterrupted()) {
            throw exception;
        }
    }

    /** Immutable groups for all source files in one captured snapshot. */
    public static final class Plan {

        private final long snapshotId;
        private final List<FilePlan> files;

        private Plan(long snapshotId, List<FilePlan> files) {
            this.snapshotId = snapshotId;
            this.files = Collections.unmodifiableList(new ArrayList<>(files));
        }

        public long snapshotId() {
            return snapshotId;
        }

        public List<FilePlan> files() {
            return files;
        }
    }

    /** One active data file and its complete field-local payload groups. */
    public static final class FilePlan {

        private final DataSplit sourceSplit;
        private final int fileIndex;
        private final Map<Integer, PkSortedIndexGroup> groups;

        private FilePlan(
                DataSplit sourceSplit, int fileIndex, Map<Integer, PkSortedIndexGroup> groups) {
            this.sourceSplit = sourceSplit;
            this.fileIndex = fileIndex;
            this.groups = Collections.unmodifiableMap(new LinkedHashMap<>(groups));
        }

        public DataFileMeta dataFile() {
            return sourceSplit.dataFiles().get(fileIndex);
        }

        public Optional<PkSortedIndexGroup> group(int fieldId) {
            return Optional.ofNullable(groups.get(fieldId));
        }

        DataSplit sourceSplit() {
            return sourceSplit;
        }

        int fileIndex() {
            return fileIndex;
        }
    }

    /** Predicate results for all source files in one captured snapshot. */
    public static final class EvaluatedPlan {

        private final long snapshotId;
        private final List<EvaluatedFile> files;

        private EvaluatedPlan(long snapshotId, List<EvaluatedFile> files) {
            this.snapshotId = snapshotId;
            this.files = Collections.unmodifiableList(new ArrayList<>(files));
        }

        public long snapshotId() {
            return snapshotId;
        }

        public List<EvaluatedFile> files() {
            return files;
        }
    }

    /** Optional file-local index result; empty means that the file requires a raw scan. */
    public static final class EvaluatedFile {

        private final FilePlan file;
        private final Optional<GlobalIndexResult> result;

        private EvaluatedFile(FilePlan file, Optional<GlobalIndexResult> result) {
            this.file = file;
            this.result = result;
        }

        public FilePlan file() {
            return file;
        }

        public Optional<GlobalIndexResult> result() {
            return result;
        }
    }
}
