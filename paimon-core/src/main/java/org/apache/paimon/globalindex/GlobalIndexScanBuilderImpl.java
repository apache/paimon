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

package org.apache.paimon.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Implementation of {@link GlobalIndexScanBuilder}. */
public class GlobalIndexScanBuilderImpl implements GlobalIndexScanBuilder {

    private final Options options;
    private final RowType rowType;
    private final FileIO fileIO;
    private final IndexPathFactory indexPathFactory;
    private final SnapshotManager snapshotManager;
    private final IndexFileHandler indexFileHandler;

    private Snapshot snapshot;
    private PartitionPredicate partitionPredicate;
    private Range rowRange;
    private VectorSearch vectorSearch;

    public GlobalIndexScanBuilderImpl(
            Options options,
            RowType rowType,
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            SnapshotManager snapshotManager,
            IndexFileHandler indexFileHandler) {
        this.options = options;
        this.rowType = rowType;
        this.fileIO = fileIO;
        this.indexPathFactory = indexPathFactory;
        this.snapshotManager = snapshotManager;
        this.indexFileHandler = indexFileHandler;
    }

    @Override
    public GlobalIndexScanBuilder withSnapshot(long snapshotId) {
        this.snapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    @Override
    public GlobalIndexScanBuilder withSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    @Override
    public GlobalIndexScanBuilder withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    @Override
    public GlobalIndexScanBuilder withRowRange(Range rowRange) {
        this.rowRange = rowRange;
        return this;
    }

    @Override
    public RowRangeGlobalIndexScanner build() {
        Objects.requireNonNull(rowRange, "rowRange must not be null");
        List<IndexManifestEntry> entries = scan();
        return new RowRangeGlobalIndexScanner(
                options, rowType, fileIO, indexPathFactory, rowRange, entries);
    }

    @Override
    public List<Range> shardList() {
        Map<String, List<Range>> indexRanges = new HashMap<>();
        for (IndexManifestEntry entry : scan()) {
            GlobalIndexMeta globalIndexMeta = entry.indexFile().globalIndexMeta();

            if (globalIndexMeta == null) {
                continue;
            }
            long start = globalIndexMeta.rowRangeStart();
            long end = globalIndexMeta.rowRangeEnd();
            indexRanges
                    .computeIfAbsent(entry.indexFile().indexType(), k -> new ArrayList<>())
                    .add(new Range(start, end));
        }

        String checkIndexType = null;
        List<Range> checkRanges = null;
        // check all type index have same shard ranges
        // If index a has [1,10],[20,30] and index b has [1,10],[20,25], it's inconsistent, because
        // it is hard to handle the [26,30] range.
        for (Map.Entry<String, List<Range>> rangeEntry : indexRanges.entrySet()) {
            String indexType = rangeEntry.getKey();
            List<Range> ranges = rangeEntry.getValue();
            if (checkRanges == null) {
                checkIndexType = indexType;
                checkRanges = Range.sortAndMergeOverlap(ranges, true);
            } else {
                List<Range> merged = Range.sortAndMergeOverlap(ranges, true);
                if (merged.size() != checkRanges.size()) {
                    throw new IllegalStateException(
                            "Inconsistent shard ranges among index types: "
                                    + checkIndexType
                                    + " vs "
                                    + indexType);
                }
                for (int i = 0; i < merged.size(); i++) {
                    Range r1 = merged.get(i);
                    Range r2 = checkRanges.get(i);
                    if (r1.from != r2.from || r1.to != r2.to) {
                        throw new IllegalStateException(
                                "Inconsistent shard ranges among index types:"
                                        + checkIndexType
                                        + " vs "
                                        + indexType);
                    }
                }
            }
        }

        return Range.sortAndMergeOverlap(
                indexRanges.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
    }

    private List<IndexManifestEntry> scan() {
        Filter<IndexManifestEntry> filter =
                entry -> {
                    if (partitionPredicate != null) {
                        if (!partitionPredicate.test(entry.partition())) {
                            return false;
                        }
                    }
                    if (rowRange != null) {
                        GlobalIndexMeta globalIndexMeta = entry.indexFile().globalIndexMeta();
                        if (globalIndexMeta == null) {
                            return false;
                        }
                        long entryStart = globalIndexMeta.rowRangeStart();
                        long entryEnd = globalIndexMeta.rowRangeEnd();

                        if (!Range.intersect(entryStart, entryEnd, rowRange.from, rowRange.to)) {
                            return false;
                        }
                    }
                    return true;
                };

        Snapshot snapshot =
                this.snapshot == null ? snapshotManager.latestSnapshot() : this.snapshot;

        return indexFileHandler.scan(snapshot, filter);
    }
}
