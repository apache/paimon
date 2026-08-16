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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.ConfigRefresher.configGroups;
import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactRefresher}. */
public class CompactRefresherTest {

    @TempDir public java.nio.file.Path tempDir;

    Catalog catalog;

    @BeforeEach
    public void before() throws Exception {
        Options options = new Options();
        options.set(WAREHOUSE, tempDir.toString());
        options.set(CACHE_ENABLED, false);
        CatalogContext context = CatalogContext.create(options);
        catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase("default", true);
    }

    @Test
    public void testRefreshFieldsAndConfigs() throws Exception {
        String detectGroups = "external-paths";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), detectGroups);
        createTable(options);
        FileStoreTable table1 = getTable();

        table1.schemaManager()
                .commitChanges(
                        SchemaChange.addColumn("c", DataTypes.INT()),
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                "round-robin"));
        FileStoreTable table2 = getTable();
        try (BatchTableWrite write = table2.newBatchWriteBuilder().newWrite();
                BatchTableCommit commit = table2.newBatchWriteBuilder().newCommit()) {
            write.write(GenericRow.of(1, 1, 1));
            commit.commit(write.prepareCommit());
        }

        List<DataField> dataFields = new ArrayList<>();
        Map<String, String> refreshedOptions = new HashMap<>();
        Set<String> groups = Arrays.stream(detectGroups.split(",")).collect(Collectors.toSet());
        CompactRefresher writerRefresher =
                CompactRefresher.create(
                        true,
                        table1,
                        new ConfigRefresherTest.TestWriteRefresher(
                                groups, refreshedOptions, dataFields));
        writerRefresher.tryRefresh(
                table2.newSnapshotReader().read().dataSplits().get(0).dataFiles());
        assertThat(dataFields).isEqualTo(table2.schema().fields());
        assertThat(refreshedOptions).isEqualTo(configGroups(groups, table2.coreOptions()));

        // should not refresh again
        writerRefresher.tryRefresh(Collections.emptyList());
    }

    @Test
    public void testRefreshOnlyFields() throws Exception {
        Map<String, String> options = new HashMap<>();
        createTable(options);
        FileStoreTable table1 = getTable();

        table1.schemaManager().commitChanges(SchemaChange.addColumn("c", DataTypes.INT()));
        FileStoreTable table2 = getTable();
        try (BatchTableWrite write = table2.newBatchWriteBuilder().newWrite();
                BatchTableCommit commit = table2.newBatchWriteBuilder().newCommit()) {
            write.write(GenericRow.of(1, 1, 1));
            commit.commit(write.prepareCommit());
        }

        List<DataField> dataFields = new ArrayList<>();
        CompactRefresher writerRefresher =
                CompactRefresher.create(
                        true,
                        table1,
                        new ConfigRefresherTest.TestWriteRefresher(
                                null, Collections.emptyMap(), dataFields));
        writerRefresher.tryRefresh(
                table2.newSnapshotReader().read().dataSplits().get(0).dataFiles());
        assertThat(dataFields).isEqualTo(table2.schema().fields());
    }

    @Test
    public void testRefreshOnlyConfigs() throws Exception {
        String detectGroups = "external-paths";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), detectGroups);
        createTable(options);
        FileStoreTable table1 = getTable();

        table1.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                "round-robin"));
        FileStoreTable table2 = getTable();

        Map<String, String> refreshedOptions = new HashMap<>();
        Set<String> groups = Arrays.stream(detectGroups.split(",")).collect(Collectors.toSet());
        CompactRefresher writerRefresher =
                CompactRefresher.create(
                        true,
                        table1,
                        new ConfigRefresherTest.TestWriteRefresher(groups, refreshedOptions, null));
        writerRefresher.tryRefresh(Collections.emptyList());
        assertThat(refreshedOptions).isEqualTo(configGroups(groups, table2.coreOptions()));
    }

    private void createTable(Map<String, String> options) throws Exception {
        catalog.createTable(
                Identifier.create("default", "T"),
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .options(options)
                        .build(),
                false);
    }

    private FileStoreTable getTable() throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("default", "T"));
    }
}
