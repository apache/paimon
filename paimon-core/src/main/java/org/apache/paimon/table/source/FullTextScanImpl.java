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
                    return textColumnIds.contains(globalIndex.indexFieldId());
                };

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());

        // Group full-text index files by column and row range.
        Map<String, Map<Range, List<IndexFileMeta>>> byColumnAndRange = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            String columnName = checkNotNull(idToColumn.get(meta.indexFieldId()));
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            byColumnAndRange
                    .computeIfAbsent(columnName, k -> new HashMap<>())
                    .computeIfAbsent(range, k -> new ArrayList<>())
                    .add(indexFile);
        }

        List<FullTextSearchSplit> splits = new ArrayList<>();
        for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> columnEntry :
                byColumnAndRange.entrySet()) {
            String columnName = columnEntry.getKey();
            for (Map.Entry<Range, List<IndexFileMeta>> rangeEntry :
                    columnEntry.getValue().entrySet()) {
                Range range = rangeEntry.getKey();
                splits.add(
                        new FullTextSearchSplit(
                                columnName, range.from, range.to, rangeEntry.getValue()));
            }
        }

        return () -> splits;
    }
}
