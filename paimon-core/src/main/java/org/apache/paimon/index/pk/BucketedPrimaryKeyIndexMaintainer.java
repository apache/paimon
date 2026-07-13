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

package org.apache.paimon.index.pk;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pksorted.BucketedSortedIndexMaintainer;
import org.apache.paimon.index.pksorted.PkSortedDataFileReader;
import org.apache.paimon.index.pksorted.PkSortedIndexBuilder;
import org.apache.paimon.index.pksorted.PkSortedIndexFile;
import org.apache.paimon.index.pkvector.BucketedVectorIndexMaintainer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Coordinates all source-backed primary-key indexes for one bucket. */
public final class BucketedPrimaryKeyIndexMaintainer {

    @Nullable private final BucketedVectorIndexMaintainer vectorMaintainer;
    private final List<BucketedSortedIndexMaintainer> sortedMaintainers;

    private BucketedPrimaryKeyIndexMaintainer(
            @Nullable BucketedVectorIndexMaintainer vectorMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        this.vectorMaintainer = vectorMaintainer;
        List<BucketedSortedIndexMaintainer> sorted = new ArrayList<>(sortedMaintainers);
        sorted.sort(Comparator.comparingInt(BucketedSortedIndexMaintainer::fieldId));
        this.sortedMaintainers = Collections.unmodifiableList(sorted);
    }

    public static BucketedPrimaryKeyIndexMaintainer ofVector(
            BucketedVectorIndexMaintainer vectorMaintainer) {
        return new BucketedPrimaryKeyIndexMaintainer(vectorMaintainer, Collections.emptyList());
    }

    public static BucketedPrimaryKeyIndexMaintainer of(
            BucketedVectorIndexMaintainer vectorMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return new BucketedPrimaryKeyIndexMaintainer(vectorMaintainer, sortedMaintainers);
    }

    public static BucketedPrimaryKeyIndexMaintainer ofSorted(
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return new BucketedPrimaryKeyIndexMaintainer(null, sortedMaintainers);
    }

    public synchronized void prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit = null;
        List<BucketedSortedIndexMaintainer.SortedIndexCommit> sortedCommits = new ArrayList<>();
        try {
            if (vectorMaintainer != null) {
                vectorCommit =
                        vectorMaintainer.prepareCommit(
                                appendIncrement, compactIncrement, waitCompaction);
            }

            if (waitCompaction) {
                prepareSortedBlocking(appendIncrement, compactIncrement, sortedCommits);
            } else {
                prepareSortedNonBlocking(appendIncrement, compactIncrement, sortedCommits);
            }

            if (vectorCommit != null) {
                mergeVectorCommit(appendIncrement, compactIncrement, vectorCommit);
            }
            for (BucketedSortedIndexMaintainer.SortedIndexCommit sortedCommit : sortedCommits) {
                mergeSortedCommit(appendIncrement, compactIncrement, sortedCommit);
            }
        } catch (Throwable failure) {
            for (int i = sortedCommits.size() - 1; i >= 0; i--) {
                sortedCommits.get(i).abort(failure);
            }
            if (vectorCommit != null) {
                vectorCommit.abort(failure);
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private void prepareSortedBlocking(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            List<BucketedSortedIndexMaintainer.SortedIndexCommit> commits)
            throws Exception {
        BucketedSortedIndexMaintainer active = activeSortedMaintainer();
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            commits.add(
                    maintainer.prepareCommit(
                            appendIncrement, compactIncrement, true, maintainer == active));
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            if (maintainer != active) {
                commits.add(
                        maintainer.prepareCommit(appendIncrement, compactIncrement, true, true));
            }
        }
    }

    private void prepareSortedNonBlocking(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            List<BucketedSortedIndexMaintainer.SortedIndexCommit> commits)
            throws Exception {
        BucketedSortedIndexMaintainer active = activeSortedMaintainer();
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            commits.add(
                    maintainer.prepareCommit(
                            appendIncrement, compactIncrement, false, maintainer == active));
        }
        if (activeSortedMaintainer() != null) {
            return;
        }

        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            if (maintainer == active && !maintainer.state().uncoveredSourceFiles().isEmpty()) {
                continue;
            }
            if (!maintainer.state().uncoveredSourceFiles().isEmpty()) {
                commits.add(
                        maintainer.prepareCommit(appendIncrement, compactIncrement, false, true));
                break;
            }
        }
    }

    @Nullable
    private BucketedSortedIndexMaintainer activeSortedMaintainer() {
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            if (maintainer.buildNotCompleted()) {
                return maintainer;
            }
        }
        return null;
    }

    private static void mergeSortedCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            BucketedSortedIndexMaintainer.SortedIndexCommit commit) {
        commit.appendIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        appendIncrement.newIndexFiles(),
                                        appendIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
        commit.compactIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        compactIncrement.newIndexFiles(),
                                        compactIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
    }

    private static void mergeVectorCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            BucketedVectorIndexMaintainer.VectorIndexCommit commit) {
        commit.appendIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        appendIncrement.newIndexFiles(),
                                        appendIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
        commit.compactIncrement()
                .ifPresent(
                        increment ->
                                applyIndexIncrement(
                                        compactIncrement.newIndexFiles(),
                                        compactIncrement.deletedIndexFiles(),
                                        increment.newIndexFiles(),
                                        increment.deletedIndexFiles()));
    }

    private static void applyIndexIncrement(
            List<IndexFileMeta> targetNew,
            List<IndexFileMeta> targetDeleted,
            List<IndexFileMeta> sourceNew,
            List<IndexFileMeta> sourceDeleted) {
        targetNew.addAll(sourceNew);
        targetDeleted.addAll(sourceDeleted);
    }

    public boolean buildNotCompleted() {
        if (vectorMaintainer != null && vectorMaintainer.buildNotCompleted()) {
            return true;
        }
        return activeSortedMaintainer() != null;
    }

    public void withExecutor(ExecutorService executor) {
        if (vectorMaintainer != null) {
            vectorMaintainer.withExecutor(executor);
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            maintainer.withExecutor(executor);
        }
    }

    public void close() {
        if (vectorMaintainer != null) {
            vectorMaintainer.close();
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            maintainer.close();
        }
    }

    /** Factory to restore all configured source-backed indexes for a bucket. */
    public static final class Factory {

        private final IndexFileHandler handler;
        @Nullable private final BucketedVectorIndexMaintainer.Factory vectorFactory;
        private final List<SortedDefinitionFactory> sortedFactories;

        private Factory(
                IndexFileHandler handler,
                @Nullable BucketedVectorIndexMaintainer.Factory vectorFactory,
                List<SortedDefinitionFactory> sortedFactories) {
            this.handler = handler;
            this.vectorFactory = vectorFactory;
            this.sortedFactories = Collections.unmodifiableList(new ArrayList<>(sortedFactories));
        }

        public static Factory ofVector(BucketedVectorIndexMaintainer.Factory vectorFactory) {
            return new Factory(
                    vectorFactory.indexFileHandler(), vectorFactory, Collections.emptyList());
        }

        public static Factory create(
                IndexFileHandler handler,
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                TableSchema schema) {
            CoreOptions coreOptions = new CoreOptions(schema.options());
            Map<String, DataField> fields = schema.nameToFieldMap();
            BucketedVectorIndexMaintainer.Factory vectorFactory = null;
            List<SortedDefinitionFactory> sortedFactories = new ArrayList<>();
            for (PrimaryKeyIndexDefinition definition :
                    PrimaryKeyIndexDefinitions.create(schema).definitions()) {
                DataField field = fields.get(definition.column());
                checkArgument(
                        field != null,
                        "Primary-key index column '%s' does not exist.",
                        definition.column());
                switch (definition.family()) {
                    case VECTOR:
                        checkArgument(
                                vectorFactory == null,
                                "Only one primary-key vector index is supported.");
                        vectorFactory =
                                new BucketedVectorIndexMaintainer.Factory(
                                                handler,
                                                readerFactoryBuilder,
                                                field,
                                                coreOptions.primaryKeyVectorDistanceMetric(
                                                        definition.column()),
                                                definition.indexType())
                                        .withIndexOptions(definition.options());
                        break;
                    case BTREE:
                    case BITMAP:
                        sortedFactories.add(
                                new SortedDefinitionFactory(
                                        readerFactoryBuilder,
                                        field,
                                        definition.indexType(),
                                        definition.options()));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported primary-key index family " + definition.family());
                }
            }
            checkArgument(
                    vectorFactory != null || !sortedFactories.isEmpty(),
                    "No primary-key index definition is configured.");
            return new Factory(handler, vectorFactory, sortedFactories);
        }

        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        public BucketedPrimaryKeyIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor) {
            return create(partition, bucket, restoredDataFiles, restoredPayloads, executor, null);
        }

        public BucketedPrimaryKeyIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor,
                @Nullable IOManager ioManager) {
            List<DataFileMeta> dataFiles =
                    restoredDataFiles == null ? Collections.emptyList() : restoredDataFiles;
            List<IndexFileMeta> payloads =
                    restoredPayloads == null ? Collections.emptyList() : restoredPayloads;
            BucketedVectorIndexMaintainer vector =
                    vectorFactory == null
                            ? null
                            : vectorFactory.create(
                                    partition, bucket, dataFiles, payloads, executor);
            List<BucketedSortedIndexMaintainer> sorted = new ArrayList<>();
            for (SortedDefinitionFactory factory : sortedFactories) {
                sorted.add(
                        factory.create(
                                handler, partition, bucket, dataFiles, payloads, executor,
                                ioManager));
            }
            return new BucketedPrimaryKeyIndexMaintainer(vector, sorted);
        }

        private static final class SortedDefinitionFactory {

            private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
            private final DataField field;
            private final String indexType;
            private final org.apache.paimon.options.Options options;

            private SortedDefinitionFactory(
                    KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                    DataField field,
                    String indexType,
                    org.apache.paimon.options.Options options) {
                this.readerFactoryBuilder = readerFactoryBuilder;
                this.field = field;
                this.indexType = indexType;
                this.options = options;
            }

            private BucketedSortedIndexMaintainer create(
                    IndexFileHandler handler,
                    BinaryRow partition,
                    int bucket,
                    List<DataFileMeta> restoredDataFiles,
                    List<IndexFileMeta> restoredPayloads,
                    ExecutorService executor,
                    @Nullable IOManager ioManager) {
                PkSortedIndexFile indexFile = handler.pkSortedIndex(partition, bucket);
                PkSortedIndexBuilder builder =
                        new PkSortedIndexBuilder(
                                new PkSortedDataFileReader.Factory(
                                        readerFactoryBuilder, partition, bucket, field),
                                indexFile,
                                field,
                                indexType,
                                options,
                                ioManager);
                return new BucketedSortedIndexMaintainer(
                        field.id(),
                        indexType,
                        indexFile,
                        builder::build,
                        restoredDataFiles,
                        restoredPayloads,
                        executor);
            }
        }
    }
}
