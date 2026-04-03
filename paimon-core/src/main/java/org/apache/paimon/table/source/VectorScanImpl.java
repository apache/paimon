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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link VectorScan}. */
public class VectorScanImpl implements VectorScan {

    private final FileStoreTable table;
    private final PartitionPredicate partitionFilter;
    private final Predicate filter;
    private final DataField vectorColumn;

    public VectorScanImpl(
            FileStoreTable table,
            PartitionPredicate partitionFilter,
            Predicate filter,
            DataField vectorColumn) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.vectorColumn = vectorColumn;
    }

    @Override
    public Plan scan() {
        Objects.requireNonNull(vectorColumn, "Vector column must be set");

        Set<Integer> filterFieldIds =
                collectFieldNames(filter).stream()
                        .filter(name -> table.rowType().containsField(name))
                        .map(name -> table.rowType().getField(name).id())
                        .collect(Collectors.toSet());
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
                    int fieldId = globalIndex.indexFieldId();
                    return vectorColumn.id() == fieldId || filterFieldIds.contains(fieldId);
                };

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());

        // Group vector index files by (rowRangeStart, rowRangeEnd)
        Map<Range, List<IndexFileMeta>> vectorByRange = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            if (meta.indexFieldId() == vectorColumn.id()) {
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
                                        if (globalIndex.indexFieldId() == vectorColumn.id()) {
                                            return false;
                                        }
                                        return range.hasIntersection(globalIndex.rowRange());
                                    })
                            .collect(Collectors.toList());
            splits.add(new VectorSearchSplit(range.from, range.to, vectorFiles, scalarFiles));
        }

        return () -> splits;
    }
}
