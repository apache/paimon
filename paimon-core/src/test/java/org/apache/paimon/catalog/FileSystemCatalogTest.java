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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog =
                new FileSystemCatalog(
                        fileIO, new Path(warehouse), CatalogContext.create(new Options()));
    }

    @Test
    public void testCreateTableCaseSensitive() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "new_TABLE");
        Schema schema =
                Schema.newBuilder()
                        .column("Pk1", DataTypes.INT())
                        .column("pk2", DataTypes.STRING())
                        .column("pk3", DataTypes.STRING())
                        .column(
                                "Col1",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.BIGINT(),
                                        DataTypes.TIMESTAMP(),
                                        DataTypes.ARRAY(DataTypes.STRING())))
                        .column("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        .column("col3", DataTypes.ARRAY(DataTypes.ROW(DataTypes.STRING())))
                        .partitionKeys("Pk1", "pk2")
                        .primaryKey("Pk1", "pk2", "pk3")
                        .build();
        catalog.createTable(identifier, schema, false);
    }

    @Test
    public void testDropTableToleratesBlankExternalPaths() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "external_paths_t");
        Path external = new Path(new Path(warehouse), "test_db/external_dir");
        fileIO.mkdirs(external);
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .option("data-file.external-paths", " , " + external + " ,")
                        .build();
        catalog.createTable(identifier, schema, false);
        assertThat(fileIO.exists(external)).isTrue();

        // the write side trims each element and skips blank ones; the drop must parse the
        // same way
        catalog.dropTable(identifier, false);
        assertThat(catalog.listTables("test_db")).doesNotContain(identifier.getObjectName());
        assertThat(fileIO.exists(external)).isFalse();
    }

    @Test
    public void testDropTableDoesNotTrimGlobalIndexExternalPath() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "global_index_path_t");
        String indexDir = new Path(new Path(warehouse), "test_db/index_dir").toString();
        Path written = new Path(indexDir + " ");
        Path sibling = new Path(indexDir);
        fileIO.mkdirs(written);
        fileIO.mkdirs(sibling);
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .option("global-index.external-path", indexDir + " ")
                        .build();
        catalog.createTable(identifier, schema, false);

        // the write side does not trim this option, so index files go under "index_dir "
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        assertThat(table.store().pathFactory().globalIndexRootDir()).isEqualTo(written);

        catalog.dropTable(identifier, false);
        assertThat(fileIO.exists(written)).isFalse();
        assertThat(fileIO.exists(sibling)).isTrue();
    }

    @Test
    public void testDropTableWithEmptyGlobalIndexExternalPath() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "empty_global_index_path_t");
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .option("global-index.external-path", "")
                        .build();
        catalog.createTable(identifier, schema, false);

        catalog.dropTable(identifier, false);
        assertThat(catalog.listTables("test_db")).doesNotContain(identifier.getObjectName());
    }

    @Test
    public void testWriteSideSkipsBlankExternalPaths() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "external_paths_write_t");
        Path external1 = new Path(new Path(warehouse), "test_db/external_1");
        Path external2 = new Path(new Path(warehouse), "test_db/external_2");
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .option("data-file.external-paths", external1 + ", ," + external2)
                        .option("data-file.external-paths.strategy", "round-robin")
                        .build();
        catalog.createTable(identifier, schema, false);

        // a blank element is skipped instead of failing every write
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        assertThat(table.store().pathFactory().getExternalPaths())
                .containsExactly(external1, external2);
    }

    @Test
    public void testValidateFormatTableDefaultOptions() throws Exception {
        String database = "format_table_default_validation_db";
        catalog.createDatabase(database, false);
        Identifier existing = Identifier.create(database, "existing_table");
        catalog.createTable(
                existing, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        ((AbstractCatalog) DelegateCatalog.rootCatalog(catalog))
                .tableDefaultOptions.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "0");
        Schema createSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .build();

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create(database, "format_table"),
                                        createSchema,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target-file-row-num should be at least 1.");

        Schema tableReplaceSchema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        assertThatThrownBy(() -> catalog.replaceTable(existing, tableReplaceSchema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target-file-row-num should be at least 1.");
        assertThat(catalog.getTable(existing)).isNotNull();

        Schema replaceSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .build();
        assertThatThrownBy(() -> catalog.replaceTable(existing, replaceSchema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target-file-row-num should be at least 1.");
        assertThat(catalog.getTable(existing)).isNotNull();
    }

    @Test
    public void testAlterDatabase() throws Exception {
        String databaseName = "test_alter_db";
        catalog.createDatabase(databaseName, false);
        assertThatThrownBy(
                        () ->
                                catalog.alterDatabase(
                                        databaseName,
                                        Lists.newArrayList(PropertyChange.removeProperty("a")),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testPartitionsFromCatalogAreRejectedOutsideRestCatalog() throws Exception {
        String database = "rest_partition_source_db";
        Identifier identifier = Identifier.create(database, "rest_partition_source_table");
        catalog.createDatabase(database, false);
        // Write the schema file directly to simulate a format table carrying the REST-only
        // partition source in a filesystem catalog.
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true")
                        .build();
        Path tablePath =
                ((FileSystemCatalog) DelegateCatalog.rootCatalog(catalog))
                        .getTableLocation(identifier);
        new FileSystemSchemaManager(fileIO, tablePath).createTable(schema);

        // The option names one thing only, so a catalog that cannot serve it says so rather than
        // reading the table's partitions from somewhere the option did not ask for.
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.METASTORE_PARTITIONED_TABLE.key())
                .hasMessageContaining("REST catalog");
    }
}
