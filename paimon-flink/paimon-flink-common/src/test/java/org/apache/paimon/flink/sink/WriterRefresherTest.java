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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

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
        String detectOptions =
                "data-file.external-paths,data-file.external-paths.strategy,data-file.external-paths.specific-fs";
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECT_OPTIONS.key(), detectOptions);
        createTable(options);

        FileStoreTable table1 = getTable();

        table1.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECT_OPTIONS.key(),
                                detectOptions),
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), "external-path1"),
                        SchemaChange.setOption(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                "round-robin"));
        FileStoreTable table2 = getTable();

        Map<String, String> refreshedOptions = new HashMap<>();
        WriterRefresher.doRefresh(
                table1, refreshedOptions, new TestWriteRefresher(detectOptions.split(",")));
        assertThat(refreshedOptions)
                .isEqualTo(table2.coreOptions().getSpecificOptions(detectOptions.split(",")));
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

    private static class TestWriteRefresher
            implements WriterRefresher.Refresher<Map<String, String>> {
        String[] specificOptions;

        TestWriteRefresher(String[] specificOptions) {
            this.specificOptions = specificOptions;
        }

        @Override
        public void refresh(FileStoreTable table, Map<String, String> options) throws Exception {
            options.clear();
            options.putAll(table.coreOptions().getSpecificOptions(specificOptions));
        }
    }
}
