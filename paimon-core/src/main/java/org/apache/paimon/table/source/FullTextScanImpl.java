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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextScan}. */
public class FullTextScanImpl implements FullTextScan {

    private final FileStoreTable table;
    private final PartitionPredicate partitionFilter;
    private final List<DataField> textColumns;

    public FullTextScanImpl(FileStoreTable table, DataField textColumn) {
        this(table, null, textColumn);
    }

    public FullTextScanImpl(
            FileStoreTable table, PartitionPredicate partitionFilter, DataField textColumn) {
        this(table, partitionFilter, Collections.singletonList(textColumn));
    }

    public FullTextScanImpl(
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
                    if (globalIndex == null) {
                        return false;
                    }
                    return !matchedTextColumnIds(globalIndex, textColumnIds).isEmpty()
                            && supportsFullTextSearch(entry.indexFile().indexType());
                };

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());

        // A searched text column can be covered by more than one full-text-capable global index
        // (e.g. a dedicated full-text index whose primary field is the column, and a vector index
        // that carries the column as an extra field). These are different index definitions and
        // must not be merged into one reader input. Pick exactly one index definition per column,
        // preferring the one where the column is the primary indexFieldId (dedicated full-text)
        // over an extra-field match, so every split carries files from a single index identity.
        Map<String, IndexIdentity> chosenByColumn =
                chooseIndexPerColumn(allIndexFiles, textColumnIds, idToColumn);

        // Group full-text index files by column and row range. A multi-column index serves a text
        // column through either its primary indexFieldId or its extraFieldIds, and one file may
        // cover more than one searched text column.
        Map<String, Map<Range, List<IndexFileMeta>>> byColumnAndRange = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            IndexIdentity identity = IndexIdentity.of(indexFile);
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            for (int columnId : matchedTextColumnIds(meta, textColumnIds)) {
                String columnName = checkNotNull(idToColumn.get(columnId));
                if (!identity.equals(chosenByColumn.get(columnName))) {
                    // This column is served by a different (preferred) index definition.
                    continue;
                }
                byColumnAndRange
                        .computeIfAbsent(columnName, k -> new HashMap<>())
                        .computeIfAbsent(range, k -> new ArrayList<>())
                        .add(indexFile);
            }
        }

        List<FullTextSearchSplit> splits = new ArrayList<>();
        for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> columnEntry :
                byColumnAndRange.entrySet()) {
            String columnName = columnEntry.getKey();
            for (Map.Entry<Range, List<IndexFileMeta>> rangeEntry :
                    columnEntry.getValue().entrySet()) {
                Range range = rangeEntry.getKey();
                splits.add(
                        new IndexFullTextSearchSplit(
                                columnName, range.from, range.to, rangeEntry.getValue()));
            }
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
     * Chooses exactly one index definition to serve each searched text column. A column that is the
     * primary {@code indexFieldId} of some index (a dedicated full-text index) prefers that index;
     * otherwise it falls back to an index that carries the column as an extra field. Ties are
     * broken deterministically by index identity so the choice is stable across planning runs.
     */
    private static Map<String, IndexIdentity> chooseIndexPerColumn(
            List<IndexFileMeta> allIndexFiles,
            Set<Integer> textColumnIds,
            Map<Integer, String> idToColumn) {
        // columnId -> candidate identity -> whether the column is that index's primary field
        Map<Integer, Map<IndexIdentity, Boolean>> candidates = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            IndexIdentity identity = IndexIdentity.of(indexFile);
            for (int columnId : matchedTextColumnIds(meta, textColumnIds)) {
                boolean primary = meta.indexFieldId() == columnId;
                candidates
                        .computeIfAbsent(columnId, k -> new HashMap<>())
                        .merge(identity, primary, (a, b) -> a || b);
            }
        }

        Map<String, IndexIdentity> chosen = new HashMap<>();
        for (Map.Entry<Integer, Map<IndexIdentity, Boolean>> entry : candidates.entrySet()) {
            String columnName = checkNotNull(idToColumn.get(entry.getKey()));
            IndexIdentity best = null;
            boolean bestPrimary = false;
            for (Map.Entry<IndexIdentity, Boolean> candidate : entry.getValue().entrySet()) {
                IndexIdentity identity = candidate.getKey();
                boolean primary = candidate.getValue();
                boolean better =
                        best == null
                                || (primary && !bestPrimary)
                                || (primary == bestPrimary
                                        && identity.key().compareTo(best.key()) < 0);
                if (better) {
                    best = identity;
                    bestPrimary = primary;
                }
            }
            chosen.put(columnName, best);
        }
        return chosen;
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
            int[] sortedExtra = extra == null ? new int[0] : extra.clone();
            Arrays.sort(sortedExtra);
            return new IndexIdentity(
                    indexFile.indexType()
                            + '|'
                            + meta.indexFieldId()
                            + '|'
                            + Arrays.toString(sortedExtra));
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
