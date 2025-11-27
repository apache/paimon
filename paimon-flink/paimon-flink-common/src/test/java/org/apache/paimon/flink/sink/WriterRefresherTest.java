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

import static org.apache.paimon.flink.sink.WriterRefresher.configGroups;
import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link WriterRefresher}. */
public class WriterRefresherTest {
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
    public void testDoRefresh() throws Exception {
        String detectGroups = "external-paths";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), detectGroups);
        createTable(options);

        FileStoreTable table1 = getTable();

        table1.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), "external-path1"),
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                "round-robin"));
        FileStoreTable table2 = getTable();

        Map<String, String> refreshedOptions = new HashMap<>();
        Set<String> groups = Arrays.stream(detectGroups.split(",")).collect(Collectors.toSet());
        WriterRefresher writerRefresher =
                WriterRefresher.create(
                        true, table1, new TestWriteRefresher(groups, refreshedOptions));
        writerRefresher.tryRefreshForConfigs();
        assertThat(refreshedOptions).isEqualTo(configGroups(groups, table2.coreOptions()));
        writerRefresher.tryRefreshForConfigs();
    }

    @Test
    public void testRefreshWithNullConfigGroups() throws Exception {
        // Create table without SINK_WRITER_REFRESH_DETECTORS option
        Map<String, String> options = new HashMap<>();
        createTable(options);
        FileStoreTable table1 = getTable();
        WriterRefresher writerRefresher =
                WriterRefresher.create(true, table1, new TestWriteRefresher(null, null));
        assertThat(writerRefresher).isNull();
    }

    @Test
    public void testRefreshWithEmptyConfigGroups() throws Exception {
        // Create table with empty SINK_WRITER_REFRESH_DETECTORS option
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), "");
        createTable(options);
        FileStoreTable table1 = getTable();
        WriterRefresher writerRefresher =
                WriterRefresher.create(true, table1, new TestWriteRefresher(null, null));
        assertThat(writerRefresher).isNull();
    }

    @Test
    public void testRefreshWithCommaOnlyConfigGroups() throws Exception {
        // Create table with comma-only SINK_WRITER_REFRESH_DETECTORS option
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), ",,,");
        createTable(options);

        FileStoreTable table1 = getTable();

        Set<String> emptyGroups =
                Arrays.stream(",,,".split(","))
                        .filter(s -> !s.trim().isEmpty())
                        .collect(Collectors.toSet());

        WriterRefresher writerRefresher =
                WriterRefresher.create(true, table1, new TestWriteRefresher(emptyGroups, null));
        assertThat(writerRefresher).isNull();
    }

    @Test
    public void testNoRefreshWhenNoSchemaChange() throws Exception {
        String detectGroups = "external-paths";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), detectGroups);
        createTable(options);

        FileStoreTable table1 = getTable();

        Map<String, String> refreshedOptions = new HashMap<>();
        refreshedOptions.put("initial", "value");

        Set<String> groups = Arrays.stream(detectGroups.split(",")).collect(Collectors.toSet());
        WriterRefresher writerRefresher =
                WriterRefresher.create(
                        true, table1, new TestWriteRefresher(groups, refreshedOptions));

        // No schema changes made, should not refresh
        writerRefresher.tryRefreshForConfigs();

        // Options should remain unchanged
        assertThat(refreshedOptions).containsEntry("initial", "value");
        assertThat(refreshedOptions).hasSize(1);
    }

    @Test
    public void testNoRefreshWhenConfigGroupsNotChanged() throws Exception {
        String detectGroups = "external-paths";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), detectGroups);
        // Set initial external paths option
        options.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), "external-path1");
        createTable(options);

        FileStoreTable table1 = getTable();

        // Make schema changes but keep the same external paths value
        table1.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), "external-path1"),
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_PREFIX.key(),
                                "data1")); // Change different option

        Map<String, String> refreshedOptions = new HashMap<>();
        refreshedOptions.put("initial", "value");

        Set<String> groups = Arrays.stream(detectGroups.split(",")).collect(Collectors.toSet());
        WriterRefresher writerRefresher =
                WriterRefresher.create(
                        true, table1, new TestWriteRefresher(groups, refreshedOptions));

        // Should not refresh when monitored config groups haven't changed
        writerRefresher.tryRefreshForConfigs();

        // Options should remain unchanged
        assertThat(refreshedOptions).containsEntry("initial", "value");
        assertThat(refreshedOptions).hasSize(1);
    }

    @Test
    public void testRefreshWithNeedCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        createTable(options);
        FileStoreTable table1 = getTable();
        WriterRefresher writerRefresher =
                WriterRefresher.create(true, false, table1, new TestWriteRefresher(null, null));
        assertThat(writerRefresher).isNull();

        writerRefresher =
                WriterRefresher.create(true, true, table1, new TestWriteRefresher(null, null));
        assertThat(writerRefresher).isNotNull();
    }

    @Test
    public void testRefreshForDataFiles() throws Exception {
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
        WriterRefresher writerRefresher =
                WriterRefresher.create(
                        true,
                        true,
                        table1,
                        new TestWriteRefresher(null, Collections.emptyMap(), dataFields));
        writerRefresher.tryRefreshForDataFiles(
                table2.newSnapshotReader().read().dataSplits().get(0).dataFiles());
        assertThat(dataFields).isEqualTo(table2.schema().fields());
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

    private static class TestWriteRefresher implements WriterRefresher.Refresher {

        private final Set<String> groups;
        private final Map<String, String> options;
        private final List<DataField> dataFields;

        TestWriteRefresher(Set<String> groups, Map<String, String> options) {
            this(groups, options, null);
        }

        TestWriteRefresher(
                Set<String> groups, Map<String, String> options, List<DataField> fields) {
            this.groups = groups;
            this.options = options;
            this.dataFields = fields;
        }

        @Override
        public void refresh(FileStoreTable table) {
            options.clear();
            if (groups != null) {
                options.putAll(configGroups(groups, table.coreOptions()));
            }
            if (dataFields != null) {
                dataFields.clear();
                dataFields.addAll(table.schema().fields());
            }
        }
    }
}
