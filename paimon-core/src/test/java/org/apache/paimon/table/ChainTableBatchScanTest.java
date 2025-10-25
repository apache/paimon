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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ChainFileStoreTable.ChainTableBatchScan}. */
public class ChainTableBatchScanTest {

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
    public void testChainTableBatchScan() throws Exception {
        // Create catalog
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);

        // Create table
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        // Get table
        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Get scan
        ChainFileStoreTable.ChainTableBatchScan scan =
                (ChainFileStoreTable.ChainTableBatchScan) table.newScan();

        // Test plan
        List<Split> splits = scan.plan().splits();
        assertThat(splits).isNotNull();
    }

    @Test
    public void testChainTableBatchScanWithFilter() throws Exception {
        // Create catalog
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), tablePath);
        catalog.createDatabase("default", true);

        // Create table
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(identifier, schema, true);

        // Get table
        ChainFileStoreTable table = (ChainFileStoreTable) catalog.getTable(identifier);

        // Get scan
        ChainFileStoreTable.ChainTableBatchScan scan =
                (ChainFileStoreTable.ChainTableBatchScan) table.newScan();

        // Test with filter
        ChainFileStoreTable.ChainTableBatchScan filteredScan =
                scan.withFilter(null); // Just test the method exists and works

        // Test plan
        List<Split> splits = filteredScan.plan().splits();
        assertThat(splits).isNotNull();
    }
}
