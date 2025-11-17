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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@link GlobalIndexScanBuilder}. */
public class GlobalIndexScanBuilderImpl implements GlobalIndexScanBuilder {

    private final FileStoreTable fileStoreTable;

    private Long snapshotId;
    private BinaryRow partition;
    private Long rowRangeStart;
    private Long rowRangeEnd;

    public GlobalIndexScanBuilderImpl(FileStoreTable fileStoreTable) {
        this.fileStoreTable = fileStoreTable;
    }

    @Override
    public GlobalIndexScanBuilder withSnapshot(long snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    @Override
    public GlobalIndexScanBuilder withPartition(BinaryRow binaryRow) {
        this.partition = binaryRow;
        return this;
    }

    @Override
    public GlobalIndexScanBuilder withRowRange(Range rowRange) {
        this.rowRangeStart = rowRange.getStart();
        this.rowRangeEnd = rowRange.getEnd();
        return this;
    }

    @Override
    public ShardGlobalIndexScanner build() {
        Objects.requireNonNull(rowRangeStart, "rowRangeStart must not be null");
        Objects.requireNonNull(rowRangeEnd, "rowRangeEnd must not be null");
        List<IndexManifestEntry> entries = scan();
        return new ShardGlobalIndexScanner(
                fileStoreTable, partition, rowRangeStart, rowRangeEnd, entries);
    }

    @Override
    public Set<Range> shardList() {
        return scan().stream()
                .map(
                        entry -> {
                            Long start = entry.indexFile().rowRangeStart();
                            Long end = entry.indexFile().rowRangeEnd();
                            if (start == null || end == null) {
                                return null;
                            }
                            return Range.of(start, end);
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private List<IndexManifestEntry> scan() {
        IndexFileHandler indexFileHandler = fileStoreTable.store().newIndexFileHandler();

        Filter<IndexManifestEntry> filter =
                entry -> {
                    if (partition != null) {
                        if (!entry.partition().equals(partition)) {
                            return false;
                        }
                    }
                    if (rowRangeStart != null && rowRangeEnd != null) {
                        Long entryStart = entry.indexFile().rowRangeStart();
                        Long entryEnd = entry.indexFile().rowRangeEnd();
                        if (entryStart == null || entryEnd == null) {
                            return false;
                        }

                        if (!RangeUtils.intersect(
                                entryStart, entryEnd, rowRangeStart, rowRangeEnd)) {
                            return false;
                        }
                    }
                    return true;
                };

        return snapshotId == null
                ? indexFileHandler.scan(filter)
                : indexFileHandler.scan(snapshotId, filter);
    }
}
