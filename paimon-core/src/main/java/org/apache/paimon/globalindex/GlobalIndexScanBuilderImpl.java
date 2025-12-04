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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;
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
        return Range.sortAndMergeOverlap(
                scan().stream()
                        .map(
                                entry -> {
                                    GlobalIndexMeta globalIndexMeta =
                                            entry.indexFile().globalIndexMeta();
                                    if (globalIndexMeta == null) {
                                        return null;
                                    }
                                    long start = globalIndexMeta.rowRangeStart();
                                    long end = globalIndexMeta.rowRangeEnd();
                                    return new Range(start, end);
                                })
                        .filter(Objects::nonNull)
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
