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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CDCSource.TableManager}. */
public class CDCSourceTableManagerTest {

    private static final String DATABASE = "default";
    private static final String TABLE = "test_table";
    private static final Identifier IDENTIFIER = Identifier.create(DATABASE, TABLE);

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        Options options = new Options();
        options.setString("warehouse", tempDir.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, true);
        catalog.createTable(
                IDENTIFIER,
                new Schema(
                        Collections.singletonList(DataTypes.FIELD(0, "v0", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null),
                false);
        ioManager = IOManager.create(tempDir.toString());
    }

    @AfterEach
    public void afterEach() throws Exception {
        ioManager.close();
        catalog.close();
    }

    @Test
    public void testSchemaCacheIsBoundedAndReloadable() throws Exception {
        CDCSource.TableManager tableManager = createTableManager(1, 1, 1);

        TableSchema schema0 = tableManager.getTableSchema(IDENTIFIER, 0L);
        TableSchema schema1 = createNextSchema();

        assertThat(tableManager.getTableSchema(IDENTIFIER, 1L)).isEqualTo(schema1);
        assertThat(tableManager.getTableSchema(IDENTIFIER, 0L))
                .isEqualTo(schema0)
                .isNotSameAs(schema0);
    }

    @Test
    public void testTableReadCacheIsBoundedAndReloadable() throws Exception {
        CDCSource.TableManager tableManager = createTableManager(1, 1, 1);
        TableSchema schema0 = tableManager.getTableSchema(IDENTIFIER, 0L);
        TableSchema schema1 = createNextSchema();

        TableRead read0 = tableManager.getTableRead(IDENTIFIER, schema0);
        TableRead read1 = tableManager.getTableRead(IDENTIFIER, schema1);
        TableRead reloadedRead0 = tableManager.getTableRead(IDENTIFIER, schema0);

        assertThat(read1).isNotSameAs(read0);
        assertThat(reloadedRead0).isNotSameAs(read0);
    }

    @Test
    public void testTableCacheIsBoundedAndReloadable() throws Exception {
        Identifier anotherIdentifier = Identifier.create(DATABASE, "another_table");
        catalog.createTable(
                anotherIdentifier,
                new Schema(
                        Collections.singletonList(DataTypes.FIELD(0, "v0", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null),
                false);
        CountingCatalog countingCatalog = new CountingCatalog(catalog);
        CDCSource.TableManager tableManager = createTableManager(countingCatalog, 1, 1, 1);

        assertThat(tableManager.getTableSchema(IDENTIFIER, 0L).id()).isEqualTo(0);
        assertThat(tableManager.getTableSchema(anotherIdentifier, 0L).id()).isEqualTo(0);
        assertThat(tableManager.getTableSchema(IDENTIFIER, 0L).id()).isEqualTo(0);

        assertThat(countingCatalog.getTableCount(IDENTIFIER)).isEqualTo(2);
        assertThat(countingCatalog.getTableCount(anotherIdentifier)).isEqualTo(1);
    }

    @Test
    public void testEvictionDoesNotAffectActiveRecordReader() throws Exception {
        CDCSource.TableManager tableManager = createTableManager(1, 1, 1);
        TableSchema schema0 = tableManager.getTableSchema(IDENTIFIER, 0L);
        TableRead read0 = tableManager.getTableRead(IDENTIFIER, schema0);
        RecordReader<InternalRow> activeReader = read0.createReader(Collections.emptyList());

        TableSchema schema1 = createNextSchema();
        tableManager.getTableRead(IDENTIFIER, schema1);

        assertThat(activeReader.readBatch()).isNull();
        activeReader.close();
    }

    private TableSchema createNextSchema() throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(IDENTIFIER);
        table.schemaManager().commitChanges(SchemaChange.addColumn("v1", DataTypes.INT()));
        return table.schemaManager().schema(1);
    }

    private CDCSource.TableManager createTableManager(
            int tableCacheSize, int tableSchemaCacheSize, int tableReadCacheSize) {
        return createTableManager(
                catalog, tableCacheSize, tableSchemaCacheSize, tableReadCacheSize);
    }

    private CDCSource.TableManager createTableManager(
            Catalog catalog, int tableCacheSize, int tableSchemaCacheSize, int tableReadCacheSize) {
        return new CDCSource.TableManager(
                catalog, ioManager, null, tableCacheSize, tableSchemaCacheSize, tableReadCacheSize);
    }

    private static class CountingCatalog extends DelegateCatalog {

        private final Map<Identifier, Integer> getTableCounts = new HashMap<>();

        private CountingCatalog(Catalog wrapped) {
            super(wrapped);
        }

        @Override
        public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
            getTableCounts.put(identifier, getTableCount(identifier) + 1);
            return super.getTable(identifier);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return wrapped.catalogLoader();
        }

        private int getTableCount(Identifier identifier) {
            Integer count = getTableCounts.get(identifier);
            return count == null ? 0 : count;
        }
    }
}
