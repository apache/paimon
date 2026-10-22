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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pkfulltext.BucketedFullTextIndexMaintainer;
import org.apache.paimon.index.pkfulltext.PkFullTextDataFileReader;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexBuilder;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexFile;
import org.apache.paimon.index.pksorted.BucketedSortedIndexMaintainer;
import org.apache.paimon.index.pksorted.PkSequentialIndexBuilder;
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
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Coordinates all source-backed primary-key indexes for one bucket. */
public final class BucketedPrimaryKeyIndexMaintainer {

    private final List<BucketedVectorIndexMaintainer> vectorMaintainers;
    @Nullable private final BucketedFullTextIndexMaintainer fullTextMaintainer;
    private final List<BucketedSortedIndexMaintainer> sortedMaintainers;

    private BucketedPrimaryKeyIndexMaintainer(
            List<BucketedVectorIndexMaintainer> vectorMaintainers,
            @Nullable BucketedFullTextIndexMaintainer fullTextMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        this.vectorMaintainers = Collections.unmodifiableList(new ArrayList<>(vectorMaintainers));
        this.fullTextMaintainer = fullTextMaintainer;
        List<BucketedSortedIndexMaintainer> sorted = new ArrayList<>(sortedMaintainers);
        sorted.sort(Comparator.comparingInt(BucketedSortedIndexMaintainer::fieldId));
        this.sortedMaintainers = Collections.unmodifiableList(sorted);
    }

    public static BucketedPrimaryKeyIndexMaintainer ofVector(
            BucketedVectorIndexMaintainer vectorMaintainer) {
        return new BucketedPrimaryKeyIndexMaintainer(
                Collections.singletonList(vectorMaintainer), null, Collections.emptyList());
    }

    public static BucketedPrimaryKeyIndexMaintainer ofFullText(
            BucketedFullTextIndexMaintainer fullTextMaintainer) {
        return new BucketedPrimaryKeyIndexMaintainer(
                Collections.emptyList(), fullTextMaintainer, Collections.emptyList());
    }

    public static BucketedPrimaryKeyIndexMaintainer of(
            BucketedVectorIndexMaintainer vectorMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return of(vectorMaintainer, null, sortedMaintainers);
    }

    public static BucketedPrimaryKeyIndexMaintainer of(
            @Nullable BucketedVectorIndexMaintainer vectorMaintainer,
            @Nullable BucketedFullTextIndexMaintainer fullTextMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return new BucketedPrimaryKeyIndexMaintainer(
                vectorMaintainer == null
                        ? Collections.emptyList()
                        : Collections.singletonList(vectorMaintainer),
                fullTextMaintainer,
                sortedMaintainers);
    }

    public static BucketedPrimaryKeyIndexMaintainer ofVectors(
            List<BucketedVectorIndexMaintainer> vectorMaintainers,
            @Nullable BucketedFullTextIndexMaintainer fullTextMaintainer,
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return new BucketedPrimaryKeyIndexMaintainer(
                vectorMaintainers, fullTextMaintainer, sortedMaintainers);
    }

    public static BucketedPrimaryKeyIndexMaintainer ofSorted(
            List<BucketedSortedIndexMaintainer> sortedMaintainers) {
        return new BucketedPrimaryKeyIndexMaintainer(
                Collections.emptyList(), null, sortedMaintainers);
    }

    public synchronized void prepareCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction)
            throws Exception {
        List<Runnable> commits = new ArrayList<>();
        List<Consumer<Throwable>> aborts = new ArrayList<>();
        try {
            // Start all indexes before waiting. Retain both rounds for reverse-order rollback,
            // and merge changes only after every index has prepared successfully.
            int indexCount =
                    vectorMaintainers.size()
                            + sortedMaintainers.size()
                            + (fullTextMaintainer == null ? 0 : 1);
            if (waitCompaction && indexCount > 1) {
                prepareIndexes(appendIncrement, compactIncrement, false, commits, aborts);
            }
            prepareIndexes(appendIncrement, compactIncrement, waitCompaction, commits, aborts);
            for (Runnable commit : commits) {
                commit.run();
            }
        } catch (Throwable failure) {
            for (int i = aborts.size() - 1; i >= 0; i--) {
                aborts.get(i).accept(failure);
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

    private void prepareIndexes(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            boolean waitCompaction,
            List<Runnable> commits,
            List<Consumer<Throwable>> aborts)
            throws Exception {
        for (BucketedVectorIndexMaintainer maintainer : vectorMaintainers) {
            BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                    maintainer.prepareCommit(appendIncrement, compactIncrement, waitCompaction);
            aborts.add(commit::abort);
            commits.add(() -> mergeVectorCommit(appendIncrement, compactIncrement, commit));
        }
        if (fullTextMaintainer != null) {
            BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                    fullTextMaintainer.prepareCommit(
                            appendIncrement, compactIncrement, waitCompaction);
            aborts.add(commit::abort);
            commits.add(() -> mergeFullTextCommit(appendIncrement, compactIncrement, commit));
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                    maintainer.prepareCommit(appendIncrement, compactIncrement, waitCompaction);
            aborts.add(commit::abort);
            commits.add(() -> mergeSortedCommit(appendIncrement, compactIncrement, commit));
        }
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

    private static void mergeFullTextCommit(
            DataIncrement appendIncrement,
            CompactIncrement compactIncrement,
            BucketedFullTextIndexMaintainer.FullTextIndexCommit commit) {
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
        for (BucketedVectorIndexMaintainer maintainer : vectorMaintainers) {
            if (maintainer.buildNotCompleted()) {
                return true;
            }
        }
        if (fullTextMaintainer != null && fullTextMaintainer.buildNotCompleted()) {
            return true;
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            if (maintainer.buildNotCompleted()) {
                return true;
            }
        }
        return false;
    }

    public void withExecutor(ExecutorService executor) {
        checkArgument(
                !buildNotCompleted(),
                "Cannot replace executor during primary-key index maintenance.");
        for (BucketedVectorIndexMaintainer maintainer : vectorMaintainers) {
            maintainer.withExecutor(executor);
        }
        if (fullTextMaintainer != null) {
            fullTextMaintainer.withExecutor(executor);
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            maintainer.withExecutor(executor);
        }
    }

    public void close() {
        List<AutoCloseable> closeables = new ArrayList<>();
        for (BucketedVectorIndexMaintainer maintainer : vectorMaintainers) {
            closeables.add(maintainer::close);
        }
        if (fullTextMaintainer != null) {
            closeables.add(fullTextMaintainer::close);
        }
        for (BucketedSortedIndexMaintainer maintainer : sortedMaintainers) {
            closeables.add(maintainer::close);
        }
        try {
            IOUtils.closeAll(closeables);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** Factory to restore all configured source-backed indexes for a bucket. */
    public static final class Factory {

        private final IndexFileHandler handler;
        private final List<BucketedVectorIndexMaintainer.Factory> vectorFactories;
        @Nullable private final FullTextDefinitionFactory fullTextFactory;
        private final List<SortedDefinitionFactory> sortedFactories;

        private Factory(
                IndexFileHandler handler,
                List<BucketedVectorIndexMaintainer.Factory> vectorFactories,
                @Nullable FullTextDefinitionFactory fullTextFactory,
                List<SortedDefinitionFactory> sortedFactories) {
            this.handler = handler;
            this.vectorFactories = Collections.unmodifiableList(new ArrayList<>(vectorFactories));
            this.fullTextFactory = fullTextFactory;
            this.sortedFactories = Collections.unmodifiableList(new ArrayList<>(sortedFactories));
        }

        public static Factory ofVector(BucketedVectorIndexMaintainer.Factory vectorFactory) {
            return new Factory(
                    vectorFactory.indexFileHandler(),
                    Collections.singletonList(vectorFactory),
                    null,
                    Collections.emptyList());
        }

        public static Factory create(
                IndexFileHandler handler,
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                TableSchema schema) {
            CoreOptions coreOptions = new CoreOptions(schema.options());
            Map<String, DataField> fields = schema.nameToFieldMap();
            List<BucketedVectorIndexMaintainer.Factory> vectorFactories = new ArrayList<>();
            FullTextDefinitionFactory fullTextFactory = null;
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
                        vectorFactories.add(
                                new BucketedVectorIndexMaintainer.Factory(
                                                handler,
                                                readerFactoryBuilder,
                                                field,
                                                coreOptions.primaryKeyVectorDistanceMetric(
                                                        definition.column()),
                                                definition.indexType())
                                        .withIndexOptions(definition.options()));
                        break;
                    case BTREE:
                    case BITMAP:
                    case MULTI_VALUE:
                        sortedFactories.add(
                                new SortedDefinitionFactory(
                                        readerFactoryBuilder,
                                        field,
                                        definition.indexType(),
                                        definition.options(),
                                        false));
                        break;
                    case FM:
                        sortedFactories.add(
                                new SortedDefinitionFactory(
                                        readerFactoryBuilder,
                                        field,
                                        definition.indexType(),
                                        definition.options(),
                                        true));
                        break;
                    case FULL_TEXT:
                        checkArgument(
                                fullTextFactory == null,
                                "Only one primary-key full-text index is supported.");
                        fullTextFactory =
                                new FullTextDefinitionFactory(
                                        readerFactoryBuilder, field, definition.options());
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported primary-key index family " + definition.family());
                }
            }
            checkArgument(
                    !vectorFactories.isEmpty()
                            || fullTextFactory != null
                            || !sortedFactories.isEmpty(),
                    "No primary-key index definition is configured.");
            return new Factory(handler, vectorFactories, fullTextFactory, sortedFactories);
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
            return create(
                    partition,
                    bucket,
                    restoredDataFiles,
                    restoredPayloads,
                    executor,
                    ioManager,
                    DeletionVector.emptyFactory());
        }

        public BucketedPrimaryKeyIndexMaintainer create(
                BinaryRow partition,
                int bucket,
                @Nullable List<DataFileMeta> restoredDataFiles,
                @Nullable List<IndexFileMeta> restoredPayloads,
                ExecutorService executor,
                @Nullable IOManager ioManager,
                DeletionVector.Factory deletionVectorFactory) {
            List<DataFileMeta> dataFiles =
                    restoredDataFiles == null ? Collections.emptyList() : restoredDataFiles;
            List<IndexFileMeta> payloads =
                    restoredPayloads == null ? Collections.emptyList() : restoredPayloads;
            List<BucketedVectorIndexMaintainer> vectors = new ArrayList<>();
            for (BucketedVectorIndexMaintainer.Factory factory : vectorFactories) {
                vectors.add(
                        factory.create(
                                partition,
                                bucket,
                                dataFiles,
                                payloads,
                                executor,
                                deletionVectorFactory));
            }
            BucketedFullTextIndexMaintainer fullText =
                    fullTextFactory == null
                            ? null
                            : fullTextFactory.create(
                                    handler, partition, bucket, dataFiles, payloads, executor);
            List<BucketedSortedIndexMaintainer> sorted = new ArrayList<>();
            for (SortedDefinitionFactory factory : sortedFactories) {
                sorted.add(
                        factory.create(
                                handler, partition, bucket, dataFiles, payloads, executor,
                                ioManager));
            }
            return new BucketedPrimaryKeyIndexMaintainer(vectors, fullText, sorted);
        }

        private static final class FullTextDefinitionFactory {

            private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
            private final DataField field;
            private final org.apache.paimon.options.Options options;

            private FullTextDefinitionFactory(
                    KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                    DataField field,
                    org.apache.paimon.options.Options options) {
                this.readerFactoryBuilder = readerFactoryBuilder;
                this.field = field;
                this.options = options;
            }

            private BucketedFullTextIndexMaintainer create(
                    IndexFileHandler handler,
                    BinaryRow partition,
                    int bucket,
                    List<DataFileMeta> restoredDataFiles,
                    List<IndexFileMeta> restoredPayloads,
                    ExecutorService executor) {
                PkFullTextIndexFile indexFile = handler.pkFullTextIndex(partition, bucket);
                PkFullTextIndexBuilder builder =
                        new PkFullTextIndexBuilder(
                                indexFile,
                                new PkFullTextDataFileReader.Factory(
                                        readerFactoryBuilder, partition, bucket, field),
                                field,
                                options);
                return new BucketedFullTextIndexMaintainer(
                        field.id(),
                        indexFile,
                        builder,
                        restoredDataFiles,
                        restoredPayloads,
                        executor);
            }
        }

        private static final class SortedDefinitionFactory {

            private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
            private final DataField field;
            private final String indexType;
            private final org.apache.paimon.options.Options options;
            private final boolean sequential;

            private SortedDefinitionFactory(
                    KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                    DataField field,
                    String indexType,
                    org.apache.paimon.options.Options options,
                    boolean sequential) {
                this.readerFactoryBuilder = readerFactoryBuilder;
                this.field = field;
                this.indexType = indexType;
                this.options = options;
                this.sequential = sequential;
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
                PkSortedDataFileReader.Factory readerFactory =
                        new PkSortedDataFileReader.Factory(
                                readerFactoryBuilder, partition, bucket, field);
                if (sequential) {
                    PkSequentialIndexBuilder builder =
                            new PkSequentialIndexBuilder(
                                    readerFactory, indexFile, field, indexType, options);
                    return new BucketedSortedIndexMaintainer(
                            field.id(),
                            indexType,
                            indexFile,
                            builder::build,
                            restoredDataFiles,
                            restoredPayloads,
                            executor);
                }
                PkSortedIndexBuilder builder =
                        new PkSortedIndexBuilder(
                                readerFactory, indexFile, field, indexType, options, ioManager);
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
