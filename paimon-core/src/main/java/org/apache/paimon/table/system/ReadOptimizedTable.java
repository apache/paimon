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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/**
 * A {@link Table} optimized for reading by avoiding merging files.
 *
 * <ul>
 *   <li>For primary key tables, this system table only scans files on top level.
 *   <li>For append only tables, as all files can be read without merging, this system table does
 *       nothing special.
 * </ul>
 */
public class ReadOptimizedTable implements DataTable, ReadonlyTable {

    public static final String READ_OPTIMIZED = "ro";

    private final FileStoreTable dataTable;

    public ReadOptimizedTable(FileStoreTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + READ_OPTIMIZED;
    }

    @Override
    public RowType rowType() {
        return dataTable.rowType();
    }

    @Override
    public List<String> partitionKeys() {
        return dataTable.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return dataTable.options();
    }

    @Override
    public List<String> primaryKeys() {
        return dataTable.primaryKeys();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        if (dataTable.schema().primaryKeys().size() > 0) {
            return dataTable
                    .newSnapshotReader()
                    .withLevelFilter(level -> level == coreOptions().numLevels() - 1);
        } else {
            return dataTable.newSnapshotReader();
        }
    }

    @Override
    public DataTableBatchScan newScan() {
        return new DataTableBatchScan(
                dataTable.schema().primaryKeys().size() > 0,
                coreOptions(),
                newSnapshotReader(),
                DefaultValueAssigner.create(dataTable.schema()));
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new DataTableStreamScan(
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                dataTable.supportStreamingReadOverwrite(),
                DefaultValueAssigner.create(dataTable.schema()));
    }

    @Override
    public CoreOptions coreOptions() {
        return dataTable.coreOptions();
    }

    @Override
    public Path location() {
        return dataTable.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return dataTable.snapshotManager();
    }

    @Override
    public TagManager tagManager() {
        return dataTable.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return dataTable.branchManager();
    }

    @Override
    public InnerTableRead newRead() {
        return dataTable.newRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new ReadOptimizedTable(dataTable.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return dataTable.fileIO();
    }
}
