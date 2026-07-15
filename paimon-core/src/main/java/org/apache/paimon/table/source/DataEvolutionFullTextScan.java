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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.globalindex.GlobalIndexCoverage;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextScan}. */
public class DataEvolutionFullTextScan implements FullTextScan {

    private final FileStoreTable table;
    private final PartitionPredicate partitionFilter;
    private final List<DataField> textColumns;

    public DataEvolutionFullTextScan(FileStoreTable table, DataField textColumn) {
        this(table, null, textColumn);
    }

    public DataEvolutionFullTextScan(
            FileStoreTable table, PartitionPredicate partitionFilter, DataField textColumn) {
        this(table, partitionFilter, Collections.singletonList(textColumn));
    }

    public DataEvolutionFullTextScan(
            FileStoreTable table, PartitionPredicate partitionFilter, List<DataField> textColumns) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.textColumns = textColumns;
    }

    @Override
    public Plan scan() {
        Objects.requireNonNull(textColumns, "Text columns must be set");
        if (textColumns.isEmpty()) {
            return Collections::emptyList;
        }

        Set<Integer> textColumnIds = new HashSet<>();
        Map<Integer, String> idToColumn = new HashMap<>();
        for (DataField textColumn : textColumns) {
            textColumnIds.add(textColumn.id());
            idToColumn.put(textColumn.id(), textColumn.name());
        }

        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        Filter<IndexManifestEntry> indexFileFilter =
                entry -> {
                    if (partitionFilter != null && !partitionFilter.test(entry.partition())) {
                        return false;
                    }
                    GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
                    if (globalIndex == null || globalIndex.sourceMeta() != null) {
                        return false;
                    }
                    return !matchedTextColumnIds(globalIndex, textColumnIds).isEmpty()
                            && supportsFullTextSearch(entry.indexFile().indexType());
                };

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());

        List<FullTextSearchSplit> splits = new ArrayList<>();
        for (IndexRangeSelection selection :
                chooseIndexRanges(allIndexFiles, textColumnIds, idToColumn)) {
            splits.add(
                    new IndexFullTextSearchSplit(
                            selection.columnName,
                            selection.fileRange.from,
                            selection.fileRange.to,
                            selection.files,
                            selection.searchRanges));
        }

        if (!allIndexFiles.isEmpty()) {
            List<Range> rawRowRanges =
                    new GlobalIndexCoverage(table, snapshot, partitionFilter, allIndexFiles)
                            .unindexedRanges(textColumnIds);
            if (!rawRowRanges.isEmpty()) {
                splits.add(new RawFullTextSearchSplit(rawRowRanges));
            }
        }

        return () -> splits;
    }

    /**
     * Returns the searched text-column ids served by {@code meta}: its primary {@code indexFieldId}
     * plus any {@code extraFieldIds} present in {@code textColumnIds}. This lets a multi-column
     * index (e.g. vector primary + text extra) serve full-text search on its extra text column.
     */
    private static List<Integer> matchedTextColumnIds(
            GlobalIndexMeta meta, Set<Integer> textColumnIds) {
        List<Integer> matched = new ArrayList<>();
        if (textColumnIds.contains(meta.indexFieldId())) {
            matched.add(meta.indexFieldId());
        }
        int[] extraFieldIds = meta.extraFieldIds();
        if (extraFieldIds != null) {
            for (int extraFieldId : extraFieldIds) {
                if (textColumnIds.contains(extraFieldId)) {
                    matched.add(extraFieldId);
                }
            }
        }
        return matched;
    }

    /**
     * Chooses one index definition for every disjoint row interval of each searched text column. A
     * dedicated index (the text column is its primary field) wins over an index that only carries
     * the column as an extra field; remaining ties are resolved deterministically by index identity
     * and physical file range.
     *
     * <p>Index files may cover overlapping but non-identical ranges. Exact-range grouping would
     * keep all such files and search the overlap more than once. This method splits their coverage
     * at every range boundary, assigns each resulting interval once, and records the assigned
     * sub-ranges separately from the physical file range. Readers still need the physical range to
     * translate file-local row ids correctly.
     */
    private static List<IndexRangeSelection> chooseIndexRanges(
            List<IndexFileMeta> allIndexFiles,
            Set<Integer> textColumnIds,
            Map<Integer, String> idToColumn) {
        // column -> physical range -> index identity -> candidate files
        Map<String, Map<Range, Map<IndexIdentity, IndexRangeCandidate>>> grouped = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            IndexIdentity identity = IndexIdentity.of(indexFile);
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            for (int columnId : matchedTextColumnIds(meta, textColumnIds)) {
                String columnName = checkNotNull(idToColumn.get(columnId));
                boolean primary = meta.indexFieldId() == columnId;
                IndexRangeCandidate candidate =
                        grouped.computeIfAbsent(columnName, k -> new HashMap<>())
                                .computeIfAbsent(range, k -> new HashMap<>())
                                .computeIfAbsent(
                                        identity,
                                        k ->
                                                new IndexRangeCandidate(
                                                        columnName, range, identity, primary));
                candidate.files.add(indexFile);
            }
        }

        List<IndexRangeSelection> selections = new ArrayList<>();
        for (Map.Entry<String, Map<Range, Map<IndexIdentity, IndexRangeCandidate>>> columnEntry :
                grouped.entrySet()) {
            List<IndexRangeCandidate> candidates = new ArrayList<>();
            for (Map<IndexIdentity, IndexRangeCandidate> byIdentity :
                    columnEntry.getValue().values()) {
                candidates.addAll(byIdentity.values());
            }

            TreeSet<Long> boundaries = new TreeSet<>();
            Map<Long, List<IndexRangeCandidate>> startingAt = new HashMap<>();
            Map<Long, List<IndexRangeCandidate>> endingAt = new HashMap<>();
            for (IndexRangeCandidate candidate : candidates) {
                boundaries.add(candidate.fileRange.from);
                startingAt
                        .computeIfAbsent(candidate.fileRange.from, k -> new ArrayList<>())
                        .add(candidate);
                if (candidate.fileRange.to != Long.MAX_VALUE) {
                    long afterEnd = candidate.fileRange.to + 1;
                    boundaries.add(afterEnd);
                    endingAt.computeIfAbsent(afterEnd, k -> new ArrayList<>()).add(candidate);
                }
            }

            List<Long> sortedBoundaries = new ArrayList<>(boundaries);
            Map<IndexRangeCandidate, List<Range>> assigned = new LinkedHashMap<>();
            TreeSet<IndexRangeCandidate> active =
                    new TreeSet<>(DataEvolutionFullTextScan::compareCandidates);
            for (int i = 0; i < sortedBoundaries.size(); i++) {
                long from = sortedBoundaries.get(i);
                List<IndexRangeCandidate> ending = endingAt.get(from);
                if (ending != null) {
                    active.removeAll(ending);
                }
                List<IndexRangeCandidate> starting = startingAt.get(from);
                if (starting != null) {
                    active.addAll(starting);
                }
                long to =
                        i + 1 < sortedBoundaries.size()
                                ? sortedBoundaries.get(i + 1) - 1
                                : Long.MAX_VALUE;
                if (!active.isEmpty()) {
                    IndexRangeCandidate best = active.first();
                    assigned.computeIfAbsent(best, k -> new ArrayList<>()).add(new Range(from, to));
                }
            }

            for (Map.Entry<IndexRangeCandidate, List<Range>> entry : assigned.entrySet()) {
                IndexRangeCandidate candidate = entry.getKey();
                selections.add(
                        new IndexRangeSelection(
                                candidate.columnName,
                                candidate.fileRange,
                                candidate.identity,
                                candidate.files,
                                mergeAdjacent(entry.getValue())));
            }
        }

        selections.sort(
                (left, right) -> {
                    int result = left.columnName.compareTo(right.columnName);
                    if (result != 0) {
                        return result;
                    }
                    result = Long.compare(left.fileRange.from, right.fileRange.from);
                    if (result != 0) {
                        return result;
                    }
                    result = Long.compare(left.fileRange.to, right.fileRange.to);
                    return result != 0
                            ? result
                            : left.identity.key().compareTo(right.identity.key());
                });
        return selections;
    }

    private static int compareCandidates(IndexRangeCandidate left, IndexRangeCandidate right) {
        if (left.primary != right.primary) {
            return left.primary ? -1 : 1;
        }
        int result = left.identity.key().compareTo(right.identity.key());
        if (result != 0) {
            return result;
        }
        result = Long.compare(left.fileRange.from, right.fileRange.from);
        return result != 0 ? result : Long.compare(left.fileRange.to, right.fileRange.to);
    }

    private static List<Range> mergeAdjacent(List<Range> ranges) {
        List<Range> merged = new ArrayList<>();
        for (Range next : ranges) {
            if (merged.isEmpty()) {
                merged.add(next);
                continue;
            }
            Range current = merged.get(merged.size() - 1);
            if (current.to != Long.MAX_VALUE && current.to + 1 == next.from) {
                merged.set(merged.size() - 1, new Range(current.from, next.to));
            } else {
                merged.add(next);
            }
        }
        return merged;
    }

    @VisibleForTesting
    static boolean sameIndexIdentity(IndexFileMeta left, IndexFileMeta right) {
        return IndexIdentity.of(left).equals(IndexIdentity.of(right));
    }

    /**
     * Identity of a global index definition — its index type, primary {@code indexFieldId} and
     * extra field ids. Two index files with the same identity belong to the same index definition
     * (differing only by row range/shard); files with different identities must not be merged into
     * one reader input.
     */
    private static final class IndexIdentity {
        private final String key;

        private IndexIdentity(String key) {
            this.key = key;
        }

        static IndexIdentity of(IndexFileMeta indexFile) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            int[] extra = meta.extraFieldIds();
            return new IndexIdentity(
                    indexFile.indexType()
                            + '|'
                            + meta.indexFieldId()
                            + '|'
                            + Arrays.toString(extra == null ? new int[0] : extra));
        }

        String key() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof IndexIdentity && key.equals(((IndexIdentity) o).key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    private static final class IndexRangeCandidate {
        private final String columnName;
        private final Range fileRange;
        private final IndexIdentity identity;
        private final boolean primary;
        private final List<IndexFileMeta> files = new ArrayList<>();

        private IndexRangeCandidate(
                String columnName, Range fileRange, IndexIdentity identity, boolean primary) {
            this.columnName = columnName;
            this.fileRange = fileRange;
            this.identity = identity;
            this.primary = primary;
        }
    }

    private static final class IndexRangeSelection {
        private final String columnName;
        private final Range fileRange;
        private final IndexIdentity identity;
        private final List<IndexFileMeta> files;
        private final List<Range> searchRanges;

        private IndexRangeSelection(
                String columnName,
                Range fileRange,
                IndexIdentity identity,
                List<IndexFileMeta> files,
                List<Range> searchRanges) {
            this.columnName = columnName;
            this.fileRange = fileRange;
            this.identity = identity;
            this.files = files;
            this.searchRanges = searchRanges;
        }
    }

    private static boolean supportsFullTextSearch(String indexType) {
        GlobalIndexerFactory factory;
        try {
            factory = GlobalIndexerFactoryUtils.load(indexType);
        } catch (RuntimeException e) {
            return false;
        }
        return factory.supportsFullTextSearch();
    }
}
