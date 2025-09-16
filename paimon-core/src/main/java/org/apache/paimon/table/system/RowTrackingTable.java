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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for reading row id of table. */
public class RowTrackingTable implements DataTable, ReadonlyTable {

    public static final String ROW_TRACKING = "row_tracking";

    private final FileStoreTable wrapped;

    public RowTrackingTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;

        if (!coreOptions().rowTrackingEnabled()) {
            throw new IllegalArgumentException(
                    "Table "
                            + wrapped.name()
                            + " does not support row tracking, "
                            + "please enable 'row-tracking.enabled' option to use 'with_metadata' table.");
        }
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + ROW_TRACKING;
    }

    @Override
    public RowType rowType() {
        return SpecialFields.rowTypeWithRowTracking(wrapped.rowType());
    }

    @Override
    public List<String> partitionKeys() {
        return wrapped.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return wrapped.newSnapshotReader();
    }

    @Override
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new RowTrackingTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public InnerTableRead newRead() {
        return wrapped.newRead().withReadType(rowType());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new RowTrackingTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }
}
