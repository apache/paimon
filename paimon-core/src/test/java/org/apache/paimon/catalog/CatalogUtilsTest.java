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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.FORMAT_TABLE_IMPLEMENTATION;
import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.catalog.CatalogUtils.loadTable;
import static org.apache.paimon.catalog.CatalogUtils.validateCreateTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link CatalogUtils}. */
class CatalogUtilsTest {

    @Test
    void testRejectEngineImplementationForManagedFormatTableOnCreate() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(TYPE.key(), FORMAT_TABLE.toString())
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(FORMAT_TABLE_IMPLEMENTATION.key(), "engine")
                        .build();

        assertThatThrownBy(() -> validateCreateTable(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METASTORE_PARTITIONED_TABLE.key())
                .hasMessageContaining(FORMAT_TABLE_IMPLEMENTATION.key());
    }

    @Test
    void testEngineImplementationManagedFormatTableLoadsAsUnmanaged() throws Exception {
        // The option combination is invalid for managed partitions, but an existing table must
        // stay loadable: it degrades to an unmanaged format table instead of failing.
        Table table = loadManagedFormatTable("engine", true);
        assertUnmanagedFormatTable(table);
    }

    @Test
    void testManagedFormatTableOutsideCapableCatalogLoadsAsUnmanaged() throws Exception {
        // A catalog that cannot manage partitions (e.g. Hive) must not reject existing tables that
        // carry the (previously inert) option; it loads them as unmanaged format tables.
        Table table = loadManagedFormatTable("paimon", false);
        assertUnmanagedFormatTable(table);
    }

    @Test
    void testLoadManagedFormatTableFromCapableCatalog() throws Exception {
        Table table = loadManagedFormatTable("paimon", true);
        assertThat(table).isInstanceOf(FormatTable.class);
        FormatTable formatTable = (FormatTable) table;
        assertThat(
                        new org.apache.paimon.CoreOptions(formatTable.options())
                                .partitionedTableInMetastore())
                .isTrue();
        assertThat(formatTable.catalogProvider()).isNotNull();
    }

    private static void assertUnmanagedFormatTable(Table table) {
        assertThat(table).isInstanceOf(FormatTable.class);
        FormatTable formatTable = (FormatTable) table;
        assertThat(
                        new org.apache.paimon.CoreOptions(formatTable.options())
                                .partitionedTableInMetastore())
                .isFalse();
        assertThat(formatTable.catalogProvider()).isNull();
    }

    private static Table loadManagedFormatTable(
            String formatTableImplementation, boolean supportsManagedPartitions) throws Exception {
        Identifier identifier = Identifier.create("managed_db", "managed_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .option(TYPE.key(), FORMAT_TABLE.toString())
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(FORMAT_TABLE_IMPLEMENTATION.key(), formatTableImplementation)
                        .option(FILE_FORMAT.key(), "parquet")
                        .option(PATH.key(), "file:///tmp/managed_db.db/managed_table")
                        .build();
        TableMetadata metadata =
                new TableMetadata(TableSchema.create(0, schema), false, "managed-table-id");
        Catalog catalog = mock(Catalog.class);
        when(catalog.catalogLoader()).thenReturn(() -> catalog);
        when(catalog.supportsManagedFormatTablePartitions()).thenReturn(supportsManagedPartitions);

        return loadTable(
                catalog,
                identifier,
                path -> new LocalFileIO(),
                path -> new LocalFileIO(),
                ignored -> metadata,
                null,
                null,
                null,
                supportsManagedPartitions);
    }
}
