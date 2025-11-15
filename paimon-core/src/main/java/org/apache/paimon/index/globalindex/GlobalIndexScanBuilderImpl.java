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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GlobalIndexScanBuilderImpl implements GlobalIndexScanBuilder {

    private final FileStoreTable fileStoreTable;

    private Long snapshotId;
    private BinaryRow partition;
    private Integer shardId;

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
    public GlobalIndexScanBuilder withShard(int shardId) {
        this.shardId = shardId;
        return this;
    }

    @Override
    public ShardGlobalIndexScanner build() {
        Objects.requireNonNull(shardId, "shardId must not be null");
        List<IndexManifestEntry> entries = scan();
        return new ShardGlobalIndexScanner(fileStoreTable, partition, shardId, entries);
    }

    @Override
    public Set<Integer> shardList() {
        return scan().stream()
                .map(entry -> entry.indexFile().getShard())
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
                    if (shardId != null) {
                        if (!Objects.equals(entry.indexFile().getShard(), shardId)) {
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
