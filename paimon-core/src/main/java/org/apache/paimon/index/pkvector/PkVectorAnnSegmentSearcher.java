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

import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.VectorSearchMetric;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Searches one ANN payload and maps its segment-local ids back to source row positions. */
public class PkVectorAnnSegmentSearcher {

    private static final Comparator<PkVectorSearchResult> BEST_FIRST =
            Comparator.comparingDouble(PkVectorSearchResult::distance)
                    .thenComparing(PkVectorSearchResult::dataFileName)
                    .thenComparingLong(PkVectorSearchResult::rowPosition);

    private final FileIO fileIO;
    private final PkVectorAnnSegmentFile annSegmentFile;
    private final DataField vectorField;
    private final Options indexOptions;
    private final String metric;
    private final ExecutorService executor;

    public PkVectorAnnSegmentSearcher(
            FileIO fileIO,
            PkVectorAnnSegmentFile annSegmentFile,
            DataField vectorField,
            Options indexOptions,
            String metric,
            ExecutorService executor) {
        this.fileIO = fileIO;
        this.annSegmentFile = annSegmentFile;
        this.vectorField = vectorField;
        this.indexOptions = indexOptions;
        this.metric = VectorSearchMetric.normalize(metric);
        this.executor = executor;
    }

    public List<PkVectorSearchResult> search(
            IndexFileMeta segment,
            PkVectorSourceMeta sourceMeta,
            float[] query,
            int limit,
            @Nullable DeletionVector deletionVector,
            Map<String, String> searchOptions) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        if (deletionVector != null) {
            checkArgument(
                    sourceMeta.sourceFiles().size() == 1,
                    "A single deletion vector can only search a single-source ANN segment.");
            deletionVectors.put(sourceMeta.sourceFiles().get(0).fileName(), deletionVector);
        }
        return search(segment, sourceMeta, query, limit, deletionVectors, searchOptions);
    }

    public List<PkVectorSearchResult> search(
            IndexFileMeta segment,
            PkVectorSourceMeta sourceMeta,
            float[] query,
            int limit,
            Map<String, DeletionVector> deletionVectors,
            Map<String, String> searchOptions) {
        Set<String> activeSourceFiles = new HashSet<>();
        for (PkVectorSourceFile sourceFile : sourceMeta.sourceFiles()) {
            activeSourceFiles.add(sourceFile.fileName());
        }
        return search(
                segment,
                sourceMeta,
                query,
                limit,
                deletionVectors,
                activeSourceFiles,
                searchOptions);
    }

    public List<PkVectorSearchResult> search(
            IndexFileMeta segment,
            PkVectorSourceMeta sourceMeta,
            float[] query,
            int limit,
            Map<String, DeletionVector> deletionVectors,
            Set<String> activeSourceFiles,
            Map<String, String> searchOptions) {
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        GlobalIndexMeta globalIndexMeta = segment.globalIndexMeta();
        checkArgument(
                globalIndexMeta != null && globalIndexMeta.sourceMeta() != null,
                "Vector segment %s has no source metadata.",
                segment.fileName());
        GlobalIndexer indexer =
                GlobalIndexer.create(segment.indexType(), vectorField, indexOptions);
        checkArgument(
                indexer instanceof VectorGlobalIndexer,
                "Index algorithm %s does not implement VectorGlobalIndexer.",
                segment.indexType());
        String readerMetric =
                VectorSearchMetric.normalize(((VectorGlobalIndexer) indexer).metric());
        checkArgument(
                metric.equals(readerMetric),
                "ANN segment metric %s does not match index reader metric %s.",
                metric,
                readerMetric);

        GlobalIndexIOMeta ioMeta =
                new GlobalIndexIOMeta(
                        annSegmentFile.path(segment),
                        segment.fileSize(),
                        globalIndexMeta.indexMeta());
        GlobalIndexReader reader =
                indexer.createReader(
                        meta -> fileIO.newInputStream(meta.filePath()),
                        Collections.singletonList(ioMeta),
                        executor);
        try {
            VectorSearch search = new VectorSearch(query, limit, vectorField.name(), searchOptions);
            RoaringNavigableMap64 liveRows =
                    liveRowPositions(sourceMeta.sourceFiles(), activeSourceFiles, deletionVectors);
            if (liveRows != null) {
                search.withIncludeRowIds(liveRows);
            }
            Optional<ScoredGlobalIndexResult> result = reader.visitVectorSearch(search).join();
            if (!result.isPresent()) {
                return Collections.emptyList();
            }

            long sourceRowCount = totalRowCount(sourceMeta.sourceFiles());
            List<PkVectorSearchResult> candidates = new ArrayList<>();
            ScoredGlobalIndexResult scored = result.get();
            for (long ordinal : scored.results()) {
                checkArgument(
                        ordinal >= 0 && ordinal < sourceRowCount,
                        "ANN segment %s returned ordinal %s outside [0, %s).",
                        segment.fileName(),
                        ordinal,
                        sourceRowCount);
                FilePosition filePosition = filePosition(sourceMeta.sourceFiles(), ordinal);
                checkArgument(
                        activeSourceFiles.contains(filePosition.dataFileName),
                        "ANN segment %s returned inactive source %s.",
                        segment.fileName(),
                        filePosition.dataFileName);
                DeletionVector deletionVector = deletionVectors.get(filePosition.dataFileName);
                checkArgument(
                        deletionVector == null
                                || !deletionVector.isDeleted(filePosition.rowPosition),
                        "ANN segment %s returned snapshot-deleted row position %s.",
                        segment.fileName(),
                        filePosition.rowPosition);
                candidates.add(
                        new PkVectorSearchResult(
                                filePosition.dataFileName,
                                filePosition.rowPosition,
                                VectorSearchMetric.scoreToDistance(
                                        scored.scoreGetter().score(ordinal), metric)));
            }
            Collections.sort(candidates, BEST_FIRST);
            return Collections.unmodifiableList(candidates);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    @Nullable
    private static RoaringNavigableMap64 liveRowPositions(
            List<PkVectorSourceFile> sourceFiles,
            Set<String> activeSourceFiles,
            Map<String, DeletionVector> deletionVectors) {
        boolean allSourcesActive = true;
        for (PkVectorSourceFile sourceFile : sourceFiles) {
            if (!activeSourceFiles.contains(sourceFile.fileName())) {
                allSourcesActive = false;
                break;
            }
        }
        if (allSourcesActive && deletionVectors.isEmpty()) {
            return null;
        }
        RoaringNavigableMap64 live = new RoaringNavigableMap64();
        RoaringNavigableMap64 deleted = new RoaringNavigableMap64();
        long fileOffset = 0;
        for (PkVectorSourceFile sourceFile : sourceFiles) {
            boolean active = activeSourceFiles.contains(sourceFile.fileName());
            if (active && sourceFile.rowCount() > 0) {
                live.addRange(new Range(fileOffset, fileOffset + sourceFile.rowCount() - 1));
            }
            DeletionVector deletionVector =
                    active ? deletionVectors.get(sourceFile.fileName()) : null;
            if (deletionVector != null) {
                final long offset = fileOffset;
                deletionVector.forEachDeletedPosition(position -> deleted.add(offset + position));
            }
            fileOffset += sourceFile.rowCount();
        }
        live.andNot(deleted);
        return live;
    }

    private static long totalRowCount(List<PkVectorSourceFile> sourceFiles) {
        long total = 0;
        for (PkVectorSourceFile sourceFile : sourceFiles) {
            total = Math.addExact(total, sourceFile.rowCount());
        }
        return total;
    }

    private static FilePosition filePosition(List<PkVectorSourceFile> sourceFiles, long ordinal) {
        long fileOffset = 0;
        for (PkVectorSourceFile sourceFile : sourceFiles) {
            long nextOffset = fileOffset + sourceFile.rowCount();
            if (ordinal < nextOffset) {
                return new FilePosition(sourceFile.fileName(), ordinal - fileOffset);
            }
            fileOffset = nextOffset;
        }
        throw new IllegalArgumentException("ANN ordinal is outside source files: " + ordinal);
    }

    private static class FilePosition {

        private final String dataFileName;
        private final long rowPosition;

        private FilePosition(String dataFileName, long rowPosition) {
            this.dataFileName = dataFileName;
            this.rowPosition = rowPosition;
        }
    }
}
