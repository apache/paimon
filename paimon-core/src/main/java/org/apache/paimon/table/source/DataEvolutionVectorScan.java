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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.CentroidVectorIndexUtils;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexCoverage;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.VectorIndexOptions;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldIds;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Data-evolution implementation for {@link VectorScan}. */
public class DataEvolutionVectorScan implements VectorScan {

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final Predicate filter;
    private final DataField vectorColumn;
    private final Map<String, String> options;
    @Nullable private final Snapshot pinnedSnapshot;

    public DataEvolutionVectorScan(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            DataField vectorColumn,
            @Nullable Map<String, String> options) {
        this(table, partitionFilter, filter, vectorColumn, options, null);
    }

    public DataEvolutionVectorScan(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            DataField vectorColumn,
            @Nullable Map<String, String> options,
            @Nullable Snapshot pinnedSnapshot) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.vectorColumn = vectorColumn;
        this.pinnedSnapshot = pinnedSnapshot;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public Plan scan() {
        Objects.requireNonNull(vectorColumn, "Vector column must be set");

        Set<Integer> filterFieldIds = collectFieldIds(table.rowType(), filter);
        @Nullable
        Snapshot snapshot =
                pinnedSnapshot != null ? pinnedSnapshot : TimeTravelUtil.tryTravelOrLatest(table);
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        Filter<IndexManifestEntry> indexFileFilter =
                entry -> {
                    if (partitionFilter != null && !partitionFilter.test(entry.partition())) {
                        return false;
                    }
                    GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
                    if (globalIndex == null) {
                        return false;
                    }
                    if (isPrimaryColumn(globalIndex, vectorColumn.id())) {
                        return true;
                    }
                    for (int filterFieldId : filterFieldIds) {
                        if (containsField(globalIndex, filterFieldId)) {
                            return true;
                        }
                    }
                    return false;
                };

        List<IndexManifestEntry> indexEntries = indexFileHandler.scan(snapshot, indexFileFilter);
        List<IndexFileMeta> allIndexFiles =
                indexEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());
        String vectorIndexType = vectorIndexType(allIndexFiles);
        if (vectorIndexType == null) {
            vectorIndexType = configuredVectorIndexType();
        }

        // Group vector index files by (rowRangeStart, rowRangeEnd). A file is treated as the vector
        // index for this search only when the vector column is its PRIMARY field (indexFieldId);
        // the canonical ES hybrid layout is vector-as-primary with text/scalar columns carried as
        // extra fields. If the vector column were instead an extra field, no IndexVectorSearchSplit
        // is produced here and the search falls back to a brute-force RawVectorSearchSplit (correct
        // results, but the ANN index is bypassed).
        Map<Range, List<IndexFileMeta>> vectorByRange = new HashMap<>();
        List<IndexFileMeta> vectorIndexFiles = new ArrayList<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            if (isPrimaryColumn(meta, vectorColumn.id())) {
                vectorIndexFiles.add(indexFile);
                if (CentroidVectorIndexUtils.centroidBased(indexFile)) {
                    continue;
                }
                Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
                vectorByRange.computeIfAbsent(range, k -> new ArrayList<>()).add(indexFile);
            }
        }

        // Build splits: for each vector range, attach matching scalar index files
        List<VectorSearchSplit> splits = new ArrayList<>();
        for (Map.Entry<Range, List<IndexFileMeta>> entry : vectorByRange.entrySet()) {
            Range range = entry.getKey();
            List<IndexFileMeta> vectorFiles = entry.getValue();
            List<IndexFileMeta> scalarFiles =
                    allIndexFiles.stream()
                            .filter(
                                    f -> {
                                        GlobalIndexMeta globalIndex =
                                                checkNotNull(f.globalIndexMeta());
                                        if (!canServeScalarFilter(globalIndex)) {
                                            return false;
                                        }
                                        return range.hasIntersection(globalIndex.rowRange());
                                    })
                            .collect(Collectors.toList());
            splits.add(new IndexVectorSearchSplit(range.from, range.to, vectorFiles, scalarFiles));
        }

        CoreOptions effectiveOptions = effectiveCoreOptions();
        Map<BinaryRow, List<IndexFileMeta>> partitionVectors = new HashMap<>();
        for (IndexManifestEntry entry : indexEntries) {
            if (isPrimaryColumn(
                    checkNotNull(entry.indexFile().globalIndexMeta()), vectorColumn.id())) {
                partitionVectors
                        .computeIfAbsent(entry.partition(), p -> new ArrayList<>())
                        .add(entry.indexFile());
            }
        }
        for (List<IndexFileMeta> files : partitionVectors.values()) {
            if (files.stream().noneMatch(CentroidVectorIndexUtils::centroidBased)) {
                continue;
            }
            IndexFileMeta routing = CentroidVectorIndexUtils.validateModel(files);
            List<Range> coverage = CentroidVectorIndexUtils.metadata(routing).sourceRowRanges();
            long from = coverage.get(0).from;
            long to = coverage.get(coverage.size() - 1).to;
            List<IndexFileMeta> scalarFiles = scalarIndexFiles(allIndexFiles, coverage);
            splits.add(new IndexVectorSearchSplit(from, to, files, scalarFiles));
            // FAST scalar fallback is restricted to actual source coverage, not the envelope.
            for (Range range : coverage) {
                vectorByRange.putIfAbsent(range, Collections.emptyList());
            }
        }
        GlobalIndexSearchMode vectorSearchMode = effectiveOptions.vectorIndexSearchMode();
        List<Range> rawRowRanges =
                new DataEvolutionGlobalIndexCoverage(
                                table,
                                snapshot,
                                partitionFilter,
                                vectorIndexFiles,
                                vectorSearchMode)
                        .unindexedRanges(vectorColumn.id());
        if (filter != null) {
            List<Range> scalarUnindexedRanges =
                    scalarUnindexedRanges(
                            snapshot,
                            scalarIndexFiles(allIndexFiles),
                            effectiveOptions.scalarIndexSearchMode(),
                            filterFieldIds);
            if (vectorSearchMode == GlobalIndexSearchMode.FAST) {
                scalarUnindexedRanges =
                        Range.and(
                                scalarUnindexedRanges,
                                Range.sortAndMergeOverlap(
                                        new ArrayList<>(vectorByRange.keySet()), true));
            }
            rawRowRanges =
                    Range.sortAndMergeOverlap(addAll(rawRowRanges, scalarUnindexedRanges), true);
        }
        if (!rawRowRanges.isEmpty()) {
            splits.add(
                    new RawVectorSearchSplit(
                            rawRowRanges,
                            scalarIndexFiles(allIndexFiles, rawRowRanges),
                            vectorIndexType));
        }

        @Nullable Snapshot planSnapshot = snapshot;
        return new Plan() {
            @Override
            public List<VectorSearchSplit> splits() {
                return splits;
            }

            @Override
            public Snapshot snapshot() {
                return planSnapshot;
            }
        };
    }

    private List<Range> scalarUnindexedRanges(
            @Nullable Snapshot snapshot,
            List<IndexFileMeta> scalarFiles,
            GlobalIndexSearchMode searchMode,
            Set<Integer> filterFieldIds) {
        if (searchMode == GlobalIndexSearchMode.FAST) {
            return Collections.emptyList();
        }
        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(
                        table, snapshot, partitionFilter, scalarFiles, searchMode);
        List<Range> dataRanges = coverage.unindexedRanges(Collections.emptySet(), null);
        boolean possiblyPartial = false;
        for (int fieldId : filterFieldIds) {
            List<Range> missing =
                    coverage.unindexedRanges(Collections.singleton(fieldId), dataRanges);
            if (!missing.isEmpty() && !missing.equals(dataRanges)) {
                possiblyPartial = true;
                break;
            }
        }
        if (!possiblyPartial) {
            return Collections.emptyList();
        }

        // Metadata alone cannot prove operator support. Probe only partial coverage, and
        // use the actual contributing fields so a declined OR arm does not generate raw work.
        Optional<DataEvolutionGlobalIndexScanner> optionalScanner =
                DataEvolutionGlobalIndexScanner.createForScalarFilters(
                        table, snapshot, partitionFilter, scalarFiles);
        if (!optionalScanner.isPresent()) {
            return Collections.emptyList();
        }
        try (DataEvolutionGlobalIndexScanner scanner = optionalScanner.get()) {
            Optional<GlobalIndexEvaluator.Evaluation> evaluation = scanner.scanWithCoverage(filter);
            if (!evaluation.isPresent()) {
                return Collections.emptyList();
            }
            GlobalIndexEvaluator.Evaluation evaluated = evaluation.get();
            return evaluated.coveredRanges() == null
                    ? coverage.unindexedRanges(evaluated.contributingFieldIds(), dataRanges)
                    : coverage.unindexedRangesFromCoverage(evaluated.coveredRanges(), dataRanges);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to evaluate scalar index coverage for vector search.", e);
        }
    }

    private CoreOptions effectiveCoreOptions() {
        Map<String, String> merged = new HashMap<>(table.options());
        merged.putAll(options);
        return CoreOptions.fromMap(merged);
    }

    private static boolean isPrimaryColumn(GlobalIndexMeta meta, int fieldId) {
        return meta.indexFieldId() == fieldId;
    }

    private List<IndexFileMeta> scalarIndexFiles(List<IndexFileMeta> allIndexFiles) {
        return allIndexFiles.stream()
                .filter(
                        f -> {
                            GlobalIndexMeta globalIndex = checkNotNull(f.globalIndexMeta());
                            return canServeScalarFilter(globalIndex);
                        })
                .collect(Collectors.toList());
    }

    private List<IndexFileMeta> scalarIndexFiles(
            List<IndexFileMeta> allIndexFiles, List<Range> rowRanges) {
        return allIndexFiles.stream()
                .filter(
                        f -> {
                            GlobalIndexMeta globalIndex = checkNotNull(f.globalIndexMeta());
                            if (!canServeScalarFilter(globalIndex)) {
                                return false;
                            }
                            return hasIntersection(rowRanges, globalIndex.rowRange());
                        })
                .collect(Collectors.toList());
    }

    private boolean canServeScalarFilter(GlobalIndexMeta meta) {
        return !isPrimaryColumn(meta, vectorColumn.id()) || hasExtraFields(meta);
    }

    private static boolean hasExtraFields(GlobalIndexMeta meta) {
        int[] extraFieldIds = meta.extraFieldIds();
        return extraFieldIds != null && extraFieldIds.length > 0;
    }

    private static boolean hasIntersection(List<Range> ranges, Range range) {
        for (Range r : ranges) {
            if (r.hasIntersection(range)) {
                return true;
            }
        }
        return false;
    }

    private static List<Range> addAll(List<Range> left, List<Range> right) {
        List<Range> result = new ArrayList<>(left.size() + right.size());
        result.addAll(left);
        result.addAll(right);
        return result;
    }

    @Nullable
    private String vectorIndexType(List<IndexFileMeta> indexFiles) {
        String indexType = null;
        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            if (!isPrimaryColumn(meta, vectorColumn.id())) {
                continue;
            }
            if (indexType == null) {
                indexType = indexFile.indexType();
            } else if (!indexType.equals(indexFile.indexType())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vector column '%s' has multiple index types: %s and %s.",
                                vectorColumn.name(), indexType, indexFile.indexType()));
            }
        }
        return indexType;
    }

    @Nullable
    private String configuredVectorIndexType() {
        return VectorIndexOptions.resolve(
                        new Options(table.options(), options), null, vectorColumn.name(), false)
                .type();
    }

    private static boolean containsField(GlobalIndexMeta meta, int fieldId) {
        if (meta.indexFieldId() == fieldId) {
            return true;
        }
        int[] extraFieldIds = meta.extraFieldIds();
        if (extraFieldIds != null) {
            for (int extraFieldId : extraFieldIds) {
                if (extraFieldId == fieldId) {
                    return true;
                }
            }
        }
        return false;
    }
}
