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

import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.GlobalIndexCoverage;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public DataEvolutionVectorScan(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            DataField vectorColumn,
            @Nullable Map<String, String> options) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.vectorColumn = vectorColumn;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public Plan scan() {
        Objects.requireNonNull(vectorColumn, "Vector column must be set");

        Set<Integer> filterFieldIds = collectFieldIds(table.rowType(), filter);
        @Nullable Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
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

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
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
                Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
                vectorByRange.computeIfAbsent(range, k -> new ArrayList<>()).add(indexFile);
                vectorIndexFiles.add(indexFile);
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

        List<Range> rawRowRanges =
                new GlobalIndexCoverage(table, snapshot, partitionFilter, vectorIndexFiles)
                        .unindexedRanges(vectorColumn.id());
        if (filter != null) {
            rawRowRanges =
                    Range.sortAndMergeOverlap(
                            addAll(
                                    rawRowRanges,
                                    new GlobalIndexCoverage(
                                                    table,
                                                    snapshot,
                                                    partitionFilter,
                                                    scalarIndexFiles(allIndexFiles))
                                            .unindexedRanges(table.rowType(), filter)),
                            true);
        }
        if (!rawRowRanges.isEmpty()) {
            splits.add(
                    new RawVectorSearchSplit(
                            rawRowRanges,
                            scalarIndexFiles(allIndexFiles, rawRowRanges),
                            vectorIndexType));
        }

        return new Plan() {
            @Override
            public List<VectorSearchSplit> splits() {
                return splits;
            }
        };
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
        String indexType = option("index_type");
        if (indexType == null) {
            indexType = option("index-type");
        }
        if (indexType == null) {
            indexType = option("vector.index-type");
        }
        if (indexType == null) {
            indexType = option("fields." + vectorColumn.name() + ".index-type");
        }
        return indexType;
    }

    @Nullable
    private String option(String key) {
        String value = options.get(key);
        if (value == null) {
            value = table.options().get(key);
        }
        return value == null ? null : value.toLowerCase().trim();
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
