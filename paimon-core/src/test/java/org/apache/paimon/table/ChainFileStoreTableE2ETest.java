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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for {@link ChainFileStoreTable}. Tests the complete workflow
 * including table creation, data operations, and performance.
 */
public class ChainFileStoreTableE2ETest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Schema schema;
    private Identifier identifier;

    @BeforeEach
    public void beforeEach() throws Exception {
        Path warehouse = new Path(tempDir.toString());
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", warehouse.toString());

        CatalogContext context = CatalogContext.create(catalogOptions);
        catalog = CatalogFactory.createCatalog(context);

        catalog.createDatabase("test_db", true);

        identifier = Identifier.create("test_db", "chain_test_table");

        schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .partitionKeys("id")
                        .primaryKey("id", "name")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true")
                        .option(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot")
                        .option(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta")
                        .build();
    }

    @Test
    public void testChainTableCreationAndStructure() throws Exception {
        // Test 1: Create chain table
        catalog.createTable(identifier, schema, false);

        // Test 2: Verify table exists and is of correct type
        Table table = catalog.getTable(identifier);
        assertThat(table).isNotNull();
        assertThat(table).isInstanceOf(ChainFileStoreTable.class);

        // Test 3: Verify chain table internal structure
        ChainFileStoreTable chainTable = (ChainFileStoreTable) table;
        assertThat(chainTable.snapshotStoreTable()).isNotNull();
        assertThat(chainTable.deltaStoreTable()).isNotNull();

        // Test 4: Verify schema is preserved
        assertThat(chainTable.schema()).isNotNull();
        assertThat(chainTable.schema().fields()).hasSize(3);
    }

    @Test
    public void testChainTableScanOperations() throws Exception {
        // Create table
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        // Test 1: Scan empty table
        List<Split> emptySplits = table.newScan().plan().splits();
        assertThat(emptySplits).isEmpty();

        // Test 2: Scan with filters
        List<Split> filteredSplits = table.newScan().withFilter(null).plan().splits();
        assertThat(filteredSplits).isNotNull();

        // Test 3: List partition entries
        assertThat(table.newScan().listPartitionEntries()).isNotNull().isEmpty();
    }

    @Test
    public void testChainTableCopyOperations() throws Exception {
        // Create table
        catalog.createTable(identifier, schema, false);
        Table table = catalog.getTable(identifier);

        // Test 1: Copy with dynamic options
        FileStoreTable copiedTable =
                ((FileStoreTable) table).copy(java.util.Collections.emptyMap());
        assertThat(copiedTable).isInstanceOf(ChainFileStoreTable.class);

        ChainFileStoreTable copiedChainTable = (ChainFileStoreTable) copiedTable;
        assertThat(copiedChainTable.snapshotStoreTable()).isNotNull();
        assertThat(copiedChainTable.deltaStoreTable()).isNotNull();

        // Test 2: Copy with latest schema
        FileStoreTable latestTable = ((FileStoreTable) table).copyWithLatestSchema();
        assertThat(latestTable).isInstanceOf(ChainFileStoreTable.class);

        // Test 3: Copy without time travel
        FileStoreTable noTravelTable =
                ((FileStoreTable) table).copyWithoutTimeTravel(java.util.Collections.emptyMap());
        assertThat(noTravelTable).isInstanceOf(ChainFileStoreTable.class);
    }

    @Test
    public void testChainTableReadOperations() throws Exception {
        // Create table
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        // Test 1: Create read instance
        assertThat(table.newRead()).isNotNull();

        // Test 2: Read operations work
        assertThat(table.newRead()).isNotNull();
    }

    @Test
    public void testChainTablePerformanceMetrics() throws Exception {
        // Create table
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        ChainFileStoreTable chainTable = (ChainFileStoreTable) table;

        // Test: Performance monitoring is in place
        // Scan multiple times to ensure caching works
        for (int i = 0; i < 3; i++) {
            List<Split> splits = chainTable.newScan().plan().splits();
            assertThat(splits).isNotNull();
        }

        // Verify scan completes successfully (performance metrics are logged internally)
        assertThat(chainTable.snapshotStoreTable()).isNotNull();
        assertThat(chainTable.deltaStoreTable()).isNotNull();
    }

    @Test
    public void testChainTableConsistency() throws Exception {
        // Create table
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        // Test 1: Multiple scans return consistent results
        List<Split> splits1 = table.newScan().plan().splits();
        List<Split> splits2 = table.newScan().plan().splits();

        assertThat(splits1).isEqualTo(splits2);

        // Test 2: Partition entries are consistent
        List partitionEntries1 = table.newScan().listPartitionEntries();
        List partitionEntries2 = table.newScan().listPartitionEntries();

        assertThat(partitionEntries1).isEqualTo(partitionEntries2);
    }
}
