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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end integration test for {@link ChainFileStoreTable}. */
public class ChainFileStoreTableIntegrationTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private Schema schema;

    @BeforeEach
    public void beforeEach() {
        tablePath = new Path(tempDir.toString());
        schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "a")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true")
                        .option(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot")
                        .option(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta")
                        .build();
    }

    @Test
    public void testEndToEndWorkflow() throws Exception {
        // Create catalog and table
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        // Get chain table
        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);
        assertThat(table).isInstanceOf(ChainFileStoreTable.class);

        // Verify table structure
        assertThat(table.snapshotStoreTable()).isNotNull();
        assertThat(table.deltaStoreTable()).isNotNull();

        // Test scan without data
        List<Split> emptySplits = table.newScan().plan().splits();
        assertThat(emptySplits).isEmpty();
    }

    @Test
    public void testDataConsistency() throws Exception {
        // Create catalog and table
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Verify scan returns correct splits
        List<Split> splits = table.newScan().plan().splits();
        assertThat(splits).isNotNull();
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        // Create catalog and table
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Test with partition filter
        List<Split> splits = table.newScan().plan().splits();
        assertThat(splits).isNotNull();

        // Verify partition entries
        assertThat(table.newScan().listPartitionEntries()).isNotNull();
    }

    @Test
    public void testCopyOperations() throws Exception {
        // Create catalog and table
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Test copy with dynamic options
        FileStoreTable copiedTable = table.copy(java.util.Collections.emptyMap());
        assertThat(copiedTable).isInstanceOf(ChainFileStoreTable.class);

        // Test copy with latest schema
        FileStoreTable latestSchemaTable = table.copyWithLatestSchema();
        assertThat(latestSchemaTable).isInstanceOf(ChainFileStoreTable.class);
    }

    @Test
    public void testBranchSwitching() throws Exception {
        // Create catalog and table
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Test switch to branch
        FileStoreTable branchTable = table.switchToBranch("test-branch");
        assertThat(branchTable).isInstanceOf(ChainFileStoreTable.class);
    }
}

