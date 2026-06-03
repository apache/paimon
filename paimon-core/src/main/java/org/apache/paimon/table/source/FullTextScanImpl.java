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
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextScan}. */
public class FullTextScanImpl implements FullTextScan {

    private final FileStoreTable table;
    private final DataField textColumn;

    public FullTextScanImpl(FileStoreTable table, DataField textColumn) {
        this.table = table;
        this.textColumn = textColumn;
    }

    @Override
    public Plan scan() {
        Objects.requireNonNull(textColumn, "Text column must be set");

        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        Filter<IndexManifestEntry> indexFileFilter =
                entry -> {
                    GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
                    if (globalIndex == null) {
                        return false;
                    }
                    return textColumn.id() == globalIndex.indexFieldId();
                };

        List<IndexFileMeta> allIndexFiles =
                indexFileHandler.scan(snapshot, indexFileFilter).stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());

        // Group full-text index files by (rowRangeStart, rowRangeEnd)
        Map<Range, List<IndexFileMeta>> byRange = new HashMap<>();
        for (IndexFileMeta indexFile : allIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            byRange.computeIfAbsent(range, k -> new ArrayList<>()).add(indexFile);
        }

        List<FullTextSearchSplit> splits = new ArrayList<>();
        for (Map.Entry<Range, List<IndexFileMeta>> entry : byRange.entrySet()) {
            Range range = entry.getKey();
            splits.add(new FullTextSearchSplit(range.from, range.to, entry.getValue()));
        }

        return () -> splits;
    }
}
