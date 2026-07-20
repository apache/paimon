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
import static org.apache.paimon.CoreOptions.FORMAT_TABLE_PARTITION_SOURCE;
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
    void testRejectEngineImplementationForCatalogManagedPartitionsOnCreate() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(TYPE.key(), FORMAT_TABLE.toString())
                        .option(FORMAT_TABLE_PARTITION_SOURCE.key(), "rest")
                        .option(FORMAT_TABLE_IMPLEMENTATION.key(), "engine")
                        .build();

        // The combination is rejected where catalog-managed partitions are actually served.
        assertThatThrownBy(
                        () ->
                                CatalogUtils.validateCatalogManagedFormatTablePartitions(
                                        Identifier.create("db", "t"), schema.options(), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FORMAT_TABLE_PARTITION_SOURCE.key())
                .hasMessageContaining(FORMAT_TABLE_IMPLEMENTATION.key());

        // Elsewhere it is not this validation's business: the option is inert there.
        validateCreateTable(schema, false);
    }

    @Test
    void testEngineImplementationCannotHaveCatalogManagedPartitions() {
        // Asking for catalog-managed partitions where they cannot be served is an error, not a
        // table that silently reads its partitions from somewhere else.
        assertThatThrownBy(() -> loadFormatTableRequestingCatalogManagedPartitions("engine", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FORMAT_TABLE_PARTITION_SOURCE.key())
                .hasMessageContaining(FORMAT_TABLE_IMPLEMENTATION.key());
    }

    @Test
    void testOutsideRestCatalogPartitionsFromCatalogAreRejected() {
        // The option names one source only, so a catalog that cannot serve it says so rather than
        // reading the table's partitions from somewhere the option did not ask for.
        assertThatThrownBy(() -> loadFormatTableRequestingCatalogManagedPartitions("paimon", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FORMAT_TABLE_PARTITION_SOURCE.key())
                .hasMessageContaining("REST catalog");
    }

    @Test
    void testLoadCatalogManagedPartitionsFromRestCatalog() throws Exception {
        Table table = loadFormatTableRequestingCatalogManagedPartitions("paimon", true);
        assertThat(table).isInstanceOf(FormatTable.class);
        FormatTable formatTable = (FormatTable) table;
        assertThat(
                        new org.apache.paimon.CoreOptions(formatTable.options())
                                .formatTablePartitionsFromCatalog())
                .isTrue();
        assertThat(formatTable.partitionManager()).isNotNull();
    }

    private static Table loadFormatTableRequestingCatalogManagedPartitions(
            String formatTableImplementation, boolean isRestCatalog) throws Exception {
        Identifier identifier =
                Identifier.create("catalog_partition_db", "catalog_partition_table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .option(TYPE.key(), FORMAT_TABLE.toString())
                        .option(FORMAT_TABLE_PARTITION_SOURCE.key(), "rest")
                        .option(FORMAT_TABLE_IMPLEMENTATION.key(), formatTableImplementation)
                        .option(FILE_FORMAT.key(), "parquet")
                        .option(
                                PATH.key(),
                                "file:///tmp/catalog_partition_db.db/catalog_partition_table")
                        .build();
        TableMetadata metadata =
                new TableMetadata(
                        TableSchema.create(0, schema), false, "catalog-partition-table-id");
        Catalog catalog = mock(Catalog.class);
        when(catalog.catalogLoader()).thenReturn(() -> catalog);

        return loadTable(
                catalog,
                identifier,
                path -> new LocalFileIO(),
                path -> new LocalFileIO(),
                ignored -> metadata,
                null,
                null,
                null,
                isRestCatalog);
    }
}
