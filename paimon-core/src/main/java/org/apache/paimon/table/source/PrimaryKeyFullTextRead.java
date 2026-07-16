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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexFile;
import org.apache.paimon.index.pkfulltext.PrimaryKeyFullTextBucketSearch;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Reads compaction-visible primary-key full-text payloads in fast search mode. */
public class PrimaryKeyFullTextRead implements FullTextRead {

    private final int limit;
    private final BucketRankingSearch indexedSearch;

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
        checkFastSearchMode(table.coreOptions().globalIndexSearchMode());
        ProductionSearch production =
                new ProductionSearch(table, definition, textField, query, limit);
        this.limit = limit;
        this.indexedSearch = production::searchIndexed;
    }

    PrimaryKeyFullTextRead(
            GlobalIndexSearchMode searchMode, int limit, BucketRankingSearch indexedSearch) {
        checkArgument(limit > 0, "Full-text search limit must be positive: %s.", limit);
        checkFastSearchMode(searchMode);
        this.limit = limit;
        this.indexedSearch = indexedSearch;
    }

    private static void checkFastSearchMode(GlobalIndexSearchMode searchMode) {
        if (searchMode != GlobalIndexSearchMode.FAST) {
            throw new UnsupportedOperationException(
                    "Primary-key full-text search only supports the FAST global-index search mode; "
                            + "FULL and DETAIL require merge-aware logical-row fallback.");
        }
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
        }
        List<PrimaryKeySearchPosition> positions =
                rankings.isEmpty()
                        ? Collections.emptyList()
                        : PrimaryKeySearchRanker.topKByScore(rankings, limit);
        return new PrimaryKeyScoredResult(snapshotId, sourceSplits, positions);
    }

    @FunctionalInterface
    interface BucketRankingSearch {
        List<List<PrimaryKeySearchPosition>> search(PrimaryKeyFullTextSearchSplit split);
    }

    private static class ProductionSearch {

        private final DataField textField;
        private final String query;
        private final int limit;
        private final FileIO fileIO;
        private final IndexFileHandler indexFileHandler;
        private final ExecutorService executor;
        private final GlobalIndexer indexer;
        private final GlobalIndexFileReader archiveReader;

        private ProductionSearch(
                FileStoreTable table,
                PrimaryKeyIndexDefinition definition,
                DataField textField,
                String query,
                int limit) {
            this.textField = textField;
            this.query = checkNotNull(query, "Full-text query must not be null.");
            this.limit = limit;
            this.fileIO = table.fileIO();
            this.indexFileHandler = table.store().newIndexFileHandler();
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
    }
}
