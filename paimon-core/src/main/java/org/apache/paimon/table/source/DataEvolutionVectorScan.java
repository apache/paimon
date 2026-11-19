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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
        List<IndexFileMeta> scalarFiles = new ArrayList<>();
        // Only the primary field serves this vector search. Each split goes to its own index type.
        Map<Pair<Range, String>, List<IndexFileMeta>> rangeGroups = new HashMap<>();
        List<IndexFileMeta> vectorIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : indexEntries) {
            IndexFileMeta file = entry.indexFile();
            GlobalIndexMeta meta = checkNotNull(file.globalIndexMeta());
            if (filter != null && canServeScalarFilter(meta)) {
                scalarFiles.add(file);
            }
            if (isPrimaryColumn(meta, vectorColumn.id())) {
                vectorIndexFiles.add(file);
                rangeGroups
                        .computeIfAbsent(
                                Pair.of(meta.rowRange(), file.indexType()), k -> new ArrayList<>())
                        .add(file);
            }
        }

        Map<Range, List<IndexFileMeta>> scalarFilesByRange =
                scalarFiles.isEmpty()
                        ? Collections.emptyMap()
                        : groupScalarFiles(
                                scalarFiles,
                                rangeGroups.keySet().stream()
                                        .map(Pair::getLeft)
                                        .distinct()
                                        .collect(Collectors.toList()));
        List<VectorSearchSplit> splits = new ArrayList<>();
        List<Range> indexedRanges = new ArrayList<>();
        String scoringIndexType = null;
        for (Map.Entry<Pair<Range, String>, List<IndexFileMeta>> entry : rangeGroups.entrySet()) {
            List<IndexFileMeta> files = entry.getValue();
            // An all-null range still supplies coverage, but has no vectors to search or score.
            if (files.stream().allMatch(file -> file.rowCount() == 0)
                    && files.stream().anyMatch(CentroidVectorIndexUtils::isRoutingModel)) {
                continue;
            }
            String type = entry.getKey().getRight();
            if (scoringIndexType == null || type.compareTo(scoringIndexType) < 0) {
                scoringIndexType = type;
            }
            Range range = entry.getKey().getLeft();
            indexedRanges.add(range);
            splits.add(
                    new IndexVectorSearchSplit(
                            range.from,
                            range.to,
                            files,
                            scalarFilesByRange.getOrDefault(range, Collections.emptyList())));
        }
        CoreOptions effectiveOptions = effectiveCoreOptions();
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
                            scalarFiles,
                            effectiveOptions.scalarIndexSearchMode(),
                            filterFieldIds);
            if (vectorSearchMode == GlobalIndexSearchMode.FAST) {
                scalarUnindexedRanges =
                        Range.and(
                                scalarUnindexedRanges,
                                Range.sortAndMergeOverlap(indexedRanges, true));
            }
            rawRowRanges =
                    Range.sortAndMergeOverlap(addAll(rawRowRanges, scalarUnindexedRanges), true);
        }
        if (!rawRowRanges.isEmpty()) {
            List<IndexFileMeta> rawScalarFiles = Collections.emptyList();
            if (!scalarFiles.isEmpty()) {
                RowRangeIndex raw = RowRangeIndex.create(rawRowRanges);
                rawScalarFiles =
                        scalarFiles.stream()
                                .filter(
                                        file -> {
                                            GlobalIndexMeta meta = file.globalIndexMeta();
                                            return raw.intersects(
                                                    meta.rowRangeStart(), meta.rowRangeEnd());
                                        })
                                .collect(Collectors.toList());
            }
            splits.add(
                    new RawVectorSearchSplit(
                            rawRowRanges,
                            rawScalarFiles,
                            scoringIndexType == null
                                    ? configuredVectorIndexType()
                                    : scoringIndexType));
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

    private static Map<Range, List<IndexFileMeta>> groupScalarFiles(
            List<IndexFileMeta> scalarFiles, List<Range> ranges) {
        ranges.sort(Comparator.comparingLong(range -> range.from));
        // Ends need not be ordered when ranges overlap. The prefix maximum lets us skip
        // every earlier range that ends before the scalar file starts.
        long[] maxEnds = new long[ranges.size()];
        long maxEnd = Long.MIN_VALUE;
        for (int i = 0; i < ranges.size(); i++) {
            maxEnd = Math.max(maxEnd, ranges.get(i).to);
            maxEnds[i] = maxEnd;
        }
        Map<Range, List<IndexFileMeta>> byRange = new HashMap<>();
        for (IndexFileMeta file : scalarFiles) {
            Range scalar = file.globalIndexMeta().rowRange();
            int lo = 0;
            int hi = ranges.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (maxEnds[mid] < scalar.from) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            for (int i = lo; i < ranges.size() && ranges.get(i).from <= scalar.to; i++) {
                Range range = ranges.get(i);
                if (range.to >= scalar.from) {
                    byRange.computeIfAbsent(range, k -> new ArrayList<>()).add(file);
                }
            }
        }
        return byRange;
    }

    private boolean canServeScalarFilter(GlobalIndexMeta meta) {
        return !isPrimaryColumn(meta, vectorColumn.id()) || hasExtraFields(meta);
    }

    private static boolean hasExtraFields(GlobalIndexMeta meta) {
        int[] extraFieldIds = meta.extraFieldIds();
        return extraFieldIds != null && extraFieldIds.length > 0;
    }

    private static List<Range> addAll(List<Range> left, List<Range> right) {
        List<Range> result = new ArrayList<>(left.size() + right.size());
        result.addAll(left);
        result.addAll(right);
        return result;
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
