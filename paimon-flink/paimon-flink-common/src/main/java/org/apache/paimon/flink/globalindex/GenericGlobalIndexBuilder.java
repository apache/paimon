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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builder for generic (non-btree) global index. */
public class GenericGlobalIndexBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;

    @Nullable protected PartitionPredicate partitionPredicate;
    @Nullable private Snapshot scanSnapshot;
    @Nullable private String indexType;

    public GenericGlobalIndexBuilder(FileStoreTable table) {
        this.table = table;
    }

    public GenericGlobalIndexBuilder withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public GenericGlobalIndexBuilder withIndexType(String indexType) {
        this.indexType = indexType;
        return this;
    }

    public FileStoreTable table() {
        return table;
    }

    /**
     * Scans manifest entries to determine which files need to be indexed.
     *
     * @return manifest entries to build index from
     */
    public List<ManifestEntry> scan() {
        checkArgument(
                table.coreOptions().bucket() == -1,
                "Generic global index only supports unaware-bucket tables (bucket = -1), "
                        + "but table '%s' has bucket = %d.",
                table.name(),
                table.coreOptions().bucket());
        // Allow DV only for index types that opt in (read path filters DV-deleted
        // rows via the live-row pre-filter, e.g. Lumina); reject all others.
        checkArgument(
                !table.coreOptions().deletionVectorsEnabled() || supportsDeletionVectors(),
                "Global index type '%s' does not support tables with deletion vectors enabled. "
                        + "Table '%s' has 'deletion-vectors.enabled' = true, which may cause "
                        + "deleted rows to be indexed.",
                indexType,
                table.name());

        scanSnapshot = table.snapshotManager().latestSnapshot();
        if (scanSnapshot == null) {
            return Collections.emptyList();
        }

        return table.store()
                .newScan()
                .withSnapshot(scanSnapshot)
                .withPartitionFilter(partitionPredicate)
                .plan()
                .files();
    }

    private boolean supportsDeletionVectors() {
        return indexType != null
                && GlobalIndexerFactoryUtils.load(indexType).supportsDeletionVectors();
    }

    @Nullable
    public Snapshot scanSnapshot() {
        return scanSnapshot;
    }

    /** Returns old index file entries that should be deleted after new indexes are built. */
    public List<IndexManifestEntry> deletedIndexEntries() {
        return Collections.emptyList();
    }
}
