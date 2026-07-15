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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyFullTextSearchSplit;
import org.apache.paimon.table.source.PrimaryKeySearchPosition;
import org.apache.paimon.table.source.PrimaryKeySearchRanker;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Searches source-backed full-text payloads and fuses their local ranks. */
public class PrimaryKeyFullTextBucketSearch {

    private final ReaderFactory readerFactory;

    public PrimaryKeyFullTextBucketSearch(ReaderFactory readerFactory) {
        this.readerFactory = readerFactory;
    }

    public List<PrimaryKeySearchPosition> search(
            PrimaryKeyFullTextSearchSplit split,
            Map<String, DeletionVector> deletionVectors,
            String column,
            String query,
            int limit) {
        List<List<PrimaryKeySearchPosition>> localRankings =
                searchRankings(split, deletionVectors, column, query, limit);
        if (localRankings.isEmpty()) {
            return Collections.emptyList();
        }
        return PrimaryKeySearchRanker.rrf(localRankings, limit);
    }

    public List<List<PrimaryKeySearchPosition>> searchRankings(
            PrimaryKeyFullTextSearchSplit split,
            Map<String, DeletionVector> deletionVectors,
            String column,
            String query,
            int limit) {
        checkArgument(limit > 0, "Full-text search limit must be positive: %s.", limit);
        DataSplit dataSplit = split.dataSplit();
        Map<String, DataFileMeta> files = new HashMap<>();
        for (DataFileMeta file : dataSplit.dataFiles()) {
            checkArgument(
                    files.put(file.fileName(), file) == null,
                    "Duplicate full-text source file %s.",
                    file.fileName());
        }

        List<PayloadRequest> requests = new ArrayList<>();
        for (IndexFileMeta payload : split.payloadFiles()) {
            List<SourceRange> sourceRanges = new ArrayList<>();
            boolean needsInclude = false;
            long totalRowCount = 0;
            for (PrimaryKeyIndexSourceFile source :
                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles()) {
                DataFileMeta file = files.get(source.fileName());
                if (file != null) {
                    checkArgument(
                            source.rowCount() == file.rowCount(),
                            "Full-text payload %s source row count does not match data file %s.",
                            payload.fileName(),
                            source.fileName());
                }
                DeletionVector deletionVector = deletionVectors.get(source.fileName());
                needsInclude |=
                        file == null || (deletionVector != null && !deletionVector.isEmpty());
                sourceRanges.add(
                        new SourceRange(
                                source.fileName(), totalRowCount, source.rowCount(), file != null));
                totalRowCount = Math.addExact(totalRowCount, source.rowCount());
            }
            RoaringNavigableMap64 include =
                    needsInclude ? liveRows(sourceRanges, deletionVectors) : null;
            if (include != null && include.isEmpty()) {
                continue;
            }
            GlobalIndexReader reader = readerFactory.create(payload);
            CompletableFuture<Optional<ScoredGlobalIndexResult>> future;
            try {
                FullTextSearch predicate = new FullTextSearch(column, query, limit);
                if (include != null) {
                    predicate.withIncludeRowIds(include);
                }
                future =
                        reader.visitFullTextSearch(predicate)
                                .whenComplete((ignored, error) -> IOUtils.closeQuietly(reader));
            } catch (RuntimeException | Error t) {
                IOUtils.closeQuietly(reader);
                throw t;
            }
            requests.add(new PayloadRequest(sourceRanges, totalRowCount, include, future));
        }

        CompletableFuture.allOf(
                        requests.stream()
                                .map(request -> request.future)
                                .toArray(CompletableFuture[]::new))
                .join();
        List<List<PrimaryKeySearchPosition>> localRankings = new ArrayList<>(requests.size());
        for (PayloadRequest request : requests) {
            Optional<ScoredGlobalIndexResult> result = request.future.join();
            if (!result.isPresent()) {
                continue;
            }
            ScoredGlobalIndexResult scored = result.get();
            List<PrimaryKeySearchPosition> ranking = new ArrayList<>();
            for (long rowId : scored.results()) {
                checkArgument(
                        rowId >= 0 && rowId < request.totalRowCount,
                        "Full-text index returned archive row position %s outside row count %s.",
                        rowId,
                        request.totalRowCount);
                if (request.include != null && !request.include.contains(rowId)) {
                    continue;
                }
                SourceRange source = request.source(rowId);
                checkArgument(
                        source != null && source.active,
                        "Full-text index returned row position %s from an inactive source.",
                        rowId);
                ranking.add(
                        new PrimaryKeySearchPosition(
                                dataSplit.partition(),
                                dataSplit.bucket(),
                                source.fileName,
                                rowId - source.offset,
                                scored.scoreGetter().score(rowId)));
            }
            ranking.sort(
                    (left, right) -> {
                        int scoreOrder = Float.compare(right.score(), left.score());
                        if (scoreOrder != 0) {
                            return scoreOrder;
                        }
                        int fileOrder = left.dataFileName().compareTo(right.dataFileName());
                        return fileOrder != 0
                                ? fileOrder
                                : Long.compare(left.rowPosition(), right.rowPosition());
                    });
            localRankings.add(ranking);
        }
        return Collections.unmodifiableList(localRankings);
    }

    @Nullable
    private static RoaringNavigableMap64 liveRows(
            long rowCount, @Nullable DeletionVector deletionVector) {
        if (deletionVector == null || deletionVector.isEmpty()) {
            return null;
        }
        RoaringNavigableMap64 live = new RoaringNavigableMap64();
        if (rowCount > 0) {
            live.addRange(new Range(0, rowCount - 1));
        }
        RoaringNavigableMap64 deleted = new RoaringNavigableMap64();
        deletionVector.forEachDeletedPosition(
                position -> {
                    checkArgument(
                            position >= 0 && position < rowCount,
                            "Deletion vector contains invalid row position %s.",
                            position);
                    deleted.add(position);
                });
        live.andNot(deleted);
        return live;
    }

    private static RoaringNavigableMap64 liveRows(
            List<SourceRange> sourceRanges, Map<String, DeletionVector> deletionVectors) {
        RoaringNavigableMap64 include = new RoaringNavigableMap64();
        for (SourceRange source : sourceRanges) {
            if (!source.active) {
                continue;
            }
            RoaringNavigableMap64 local =
                    liveRows(source.rowCount, deletionVectors.get(source.fileName));
            if (local == null) {
                if (source.rowCount > 0) {
                    include.addRange(new Range(source.offset, source.offset + source.rowCount - 1));
                }
            } else {
                for (long rowId : local) {
                    include.add(source.offset + rowId);
                }
            }
        }
        return include;
    }

    /** Creates one independently closeable reader for an immutable payload archive. */
    @FunctionalInterface
    public interface ReaderFactory {
        GlobalIndexReader create(IndexFileMeta payload);
    }

    private static class PayloadRequest {

        private final List<SourceRange> sourceRanges;
        private final long totalRowCount;
        @Nullable private final RoaringNavigableMap64 include;
        private final CompletableFuture<Optional<ScoredGlobalIndexResult>> future;

        private PayloadRequest(
                List<SourceRange> sourceRanges,
                long totalRowCount,
                @Nullable RoaringNavigableMap64 include,
                CompletableFuture<Optional<ScoredGlobalIndexResult>> future) {
            this.sourceRanges = sourceRanges;
            this.totalRowCount = totalRowCount;
            this.include = include;
            this.future = future;
        }

        @Nullable
        private SourceRange source(long rowId) {
            for (SourceRange source : sourceRanges) {
                if (rowId >= source.offset && rowId < source.offset + source.rowCount) {
                    return source;
                }
            }
            return null;
        }
    }

    private static class SourceRange {

        private final String fileName;
        private final long offset;
        private final long rowCount;
        private final boolean active;

        private SourceRange(String fileName, long offset, long rowCount, boolean active) {
            this.fileName = fileName;
            this.offset = offset;
            this.rowCount = rowCount;
            this.active = active;
        }
    }
}
