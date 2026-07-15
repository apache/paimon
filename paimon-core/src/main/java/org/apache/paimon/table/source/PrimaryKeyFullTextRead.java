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

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pkfulltext.PkFullTextDataFileReader;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexBuilder;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexFile;
import org.apache.paimon.index.pkfulltext.PrimaryKeyFullTextBucketSearch;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Reads primary-key full-text payloads and optional temporary fallback indexes. */
public class PrimaryKeyFullTextRead implements FullTextRead {

    private final GlobalIndexSearchMode searchMode;
    private final int limit;
    private final BucketRankingSearch indexedSearch;
    private final BucketRankingSearch rawSearch;

    public PrimaryKeyFullTextRead(
            FileStoreTable table,
            PrimaryKeyIndexDefinition definition,
            DataField textField,
            String query,
            int limit) {
        checkArgument(limit > 0, "Full-text search limit must be positive: %s.", limit);
        checkArgument(
                definition.family() == PrimaryKeyIndexDefinition.Family.FULL_TEXT,
                "Primary-key full-text read requires a full-text definition.");
        checkArgument(
                definition.fieldId() == textField.id(),
                "Full-text definition does not match field %s.",
                textField.name());
        ProductionSearch production =
                new ProductionSearch(table, definition, textField, query, limit);
        this.searchMode = table.coreOptions().globalIndexSearchMode();
        this.limit = limit;
        this.indexedSearch = production::searchIndexed;
        this.rawSearch = production::searchRaw;
    }

    PrimaryKeyFullTextRead(
            GlobalIndexSearchMode searchMode,
            int limit,
            BucketRankingSearch indexedSearch,
            BucketRankingSearch rawSearch) {
        checkArgument(limit > 0, "Full-text search limit must be positive: %s.", limit);
        this.searchMode = searchMode;
        this.limit = limit;
        this.indexedSearch = indexedSearch;
        this.rawSearch = rawSearch;
    }

    @Override
    public PrimaryKeyScoredResult read(FullTextScan.Plan plan) {
        checkArgument(
                plan instanceof PrimaryKeyFullTextScan.Plan,
                "Primary-key full-text read requires a PrimaryKeyFullTextScan plan.");
        PrimaryKeyFullTextScan.Plan primaryKeyPlan = (PrimaryKeyFullTextScan.Plan) plan;
        return read(primaryKeyPlan.snapshotId(), primaryKeyPlan.splits());
    }

    @Override
    public PrimaryKeyScoredResult read(List<FullTextSearchSplit> splits) {
        if (splits.isEmpty()) {
            return new PrimaryKeyScoredResult(0, Collections.emptyList(), Collections.emptyList());
        }
        checkArgument(
                splits.get(0) instanceof PrimaryKeyFullTextSearchSplit,
                "Primary-key full-text read requires primary-key full-text splits.");
        long snapshotId = ((PrimaryKeyFullTextSearchSplit) splits.get(0)).dataSplit().snapshotId();
        return read(snapshotId, splits);
    }

    private PrimaryKeyScoredResult read(long snapshotId, List<FullTextSearchSplit> splits) {
        List<DataSplit> sourceSplits = new ArrayList<>(splits.size());
        List<List<PrimaryKeySearchPosition>> rankings = new ArrayList<>();
        for (FullTextSearchSplit searchSplit : splits) {
            checkArgument(
                    searchSplit instanceof PrimaryKeyFullTextSearchSplit,
                    "Primary-key full-text read received an incompatible split.");
            PrimaryKeyFullTextSearchSplit split = (PrimaryKeyFullTextSearchSplit) searchSplit;
            checkArgument(
                    split.dataSplit().snapshotId() == snapshotId,
                    "Full-text bucket split snapshot does not match its plan.");
            sourceSplits.add(split.dataSplit());
            rankings.addAll(indexedSearch.search(split));
            if (searchMode != GlobalIndexSearchMode.FAST && !split.uncoveredDataFiles().isEmpty()) {
                rankings.addAll(rawSearch.search(split));
            }
        }
        List<PrimaryKeySearchPosition> positions =
                rankings.isEmpty()
                        ? Collections.emptyList()
                        : PrimaryKeySearchRanker.rrf(rankings, limit);
        return new PrimaryKeyScoredResult(snapshotId, sourceSplits, positions);
    }

    @FunctionalInterface
    interface BucketRankingSearch {
        List<List<PrimaryKeySearchPosition>> search(PrimaryKeyFullTextSearchSplit split);
    }

    private static KeyValueFileStore keyValueStore(FileStoreTable table) {
        FileStoreTable unwrapped = table;
        while (unwrapped instanceof DelegatedFileStoreTable) {
            unwrapped = ((DelegatedFileStoreTable) unwrapped).wrapped();
        }
        checkArgument(
                unwrapped.store() instanceof KeyValueFileStore,
                "Primary-key full-text search requires a key-value file store.");
        return (KeyValueFileStore) unwrapped.store();
    }

    private static class ProductionSearch {

        private final FileStoreTable table;
        private final PrimaryKeyIndexDefinition definition;
        private final DataField textField;
        private final String query;
        private final int limit;
        private final FileIO fileIO;
        private final IndexFileHandler indexFileHandler;
        private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
        private final ExecutorService executor;
        private final GlobalIndexer indexer;
        private final GlobalIndexFileReader archiveReader;

        private ProductionSearch(
                FileStoreTable table,
                PrimaryKeyIndexDefinition definition,
                DataField textField,
                String query,
                int limit) {
            this.table = table;
            this.definition = definition;
            this.textField = textField;
            this.query = checkNotNull(query, "Full-text query must not be null.");
            this.limit = limit;
            this.fileIO = table.fileIO();
            this.indexFileHandler = table.store().newIndexFileHandler();
            this.readerFactoryBuilder = keyValueStore(table).newReaderFactoryBuilder();
            this.executor =
                    GlobalIndexReadThreadPool.getExecutorService(
                            table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
            this.indexer =
                    GlobalIndexer.create(
                            PkFullTextIndexFile.INDEX_TYPE, textField, definition.options());
            this.archiveReader = meta -> fileIO.newInputStream(meta.filePath());
        }

        private List<List<PrimaryKeySearchPosition>> searchIndexed(
                PrimaryKeyFullTextSearchSplit split) {
            if (split.payloadFiles().isEmpty()) {
                return Collections.emptyList();
            }
            return bucketSearch(split)
                    .searchRankings(
                            split,
                            deletionVectors(split.dataSplit()),
                            textField.name(),
                            query,
                            limit);
        }

        private List<List<PrimaryKeySearchPosition>> searchRaw(
                PrimaryKeyFullTextSearchSplit split) {
            DataSplit rawDataSplit = uncoveredSplit(split);
            if (rawDataSplit.dataFiles().isEmpty()) {
                return Collections.emptyList();
            }
            PkFullTextIndexFile indexFile =
                    indexFileHandler.pkFullTextIndex(
                            rawDataSplit.partition(), rawDataSplit.bucket());
            PkFullTextIndexBuilder builder =
                    new PkFullTextIndexBuilder(
                            indexFile,
                            new PkFullTextDataFileReader.Factory(
                                    readerFactoryBuilder,
                                    rawDataSplit.partition(),
                                    rawDataSplit.bucket(),
                                    textField),
                            textField,
                            definition.options(),
                            definition.definitionFingerprint());
            List<CompletableFuture<IndexFileMeta>> builds = new ArrayList<>();
            for (DataFileMeta dataFile : rawDataSplit.dataFiles()) {
                builds.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return builder.build(dataFile);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                },
                                executor));
            }

            List<IndexFileMeta> temporaryPayloads = new ArrayList<>();
            try {
                CompletableFuture.allOf(builds.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<IndexFileMeta> build : builds) {
                    temporaryPayloads.add(build.join());
                }
                PrimaryKeyFullTextSearchSplit temporarySplit =
                        new PrimaryKeyFullTextSearchSplit(
                                rawDataSplit, temporaryPayloads, Collections.emptyList());
                return bucketSearch(temporarySplit)
                        .searchRankings(
                                temporarySplit,
                                deletionVectors(rawDataSplit),
                                textField.name(),
                                query,
                                limit);
            } finally {
                for (CompletableFuture<IndexFileMeta> build : builds) {
                    if (build.isDone()
                            && !build.isCompletedExceptionally()
                            && !build.isCancelled()) {
                        IndexFileMeta payload = build.getNow(null);
                        if (payload != null) {
                            indexFile.delete(payload);
                        }
                    }
                }
            }
        }

        private PrimaryKeyFullTextBucketSearch bucketSearch(PrimaryKeyFullTextSearchSplit split) {
            PkFullTextIndexFile indexFile =
                    indexFileHandler.pkFullTextIndex(
                            split.dataSplit().partition(), split.dataSplit().bucket());
            return new PrimaryKeyFullTextBucketSearch(
                    payload -> {
                        GlobalIndexMeta meta = checkNotNull(payload.globalIndexMeta());
                        GlobalIndexIOMeta ioMeta =
                                new GlobalIndexIOMeta(
                                        indexFile.path(payload),
                                        payload.fileSize(),
                                        meta.indexMeta());
                        GlobalIndexReader reader =
                                indexer.createReader(
                                        archiveReader, Collections.singletonList(ioMeta), executor);
                        return reader;
                    });
        }

        private Map<String, DeletionVector> deletionVectors(DataSplit split) {
            try {
                DeletionVector.Factory factory =
                        DeletionVector.factory(
                                fileIO, split.dataFiles(), split.deletionFiles().orElse(null));
                Map<String, DeletionVector> result = new HashMap<>();
                for (DataFileMeta file : split.dataFiles()) {
                    Optional<DeletionVector> deletionVector = factory.create(file.fileName());
                    if (deletionVector.isPresent()) {
                        result.put(file.fileName(), deletionVector.get());
                    }
                }
                return result;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read full-text deletion vectors.", e);
            }
        }

        private static DataSplit uncoveredSplit(PrimaryKeyFullTextSearchSplit split) {
            DataSplit source = split.dataSplit();
            Set<String> uncovered = new HashSet<>(split.uncoveredDataFiles());
            List<DataFileMeta> dataFiles = new ArrayList<>();
            List<DeletionFile> deletionFiles = new ArrayList<>();
            List<DeletionFile> sourceDeletions = source.deletionFiles().orElse(null);
            for (int i = 0; i < source.dataFiles().size(); i++) {
                DataFileMeta dataFile = source.dataFiles().get(i);
                if (uncovered.remove(dataFile.fileName())) {
                    dataFiles.add(dataFile);
                    deletionFiles.add(sourceDeletions == null ? null : sourceDeletions.get(i));
                }
            }
            checkArgument(
                    uncovered.isEmpty(),
                    "Full-text split references unknown uncovered files %s.",
                    uncovered);
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(source.snapshotId())
                            .withPartition(source.partition())
                            .withBucket(source.bucket())
                            .withBucketPath(source.bucketPath())
                            .withTotalBuckets(source.totalBuckets())
                            .withDataFiles(dataFiles)
                            .isStreaming(false)
                            .rawConvertible(false);
            if (sourceDeletions != null) {
                builder.withDataDeletionFiles(deletionFiles);
            }
            return builder.build();
        }
    }
}
