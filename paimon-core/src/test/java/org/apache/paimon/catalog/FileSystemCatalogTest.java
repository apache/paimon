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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAlterTableWithInvalidOptions(boolean cacheEnabled) throws Exception {
        Identifier identifier = Identifier.create("test_db", "legacy_table");
        FileSystemSchemaManager schemaManager = createLegacyTable(identifier, cacheEnabled);

        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");

        // Fixing just one of the two invalid options must not persist an invalid schema.
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        SchemaChange.removeOption("full-compaction.delta-commits"),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");
        assertThat(schemaManager.listAllIds()).containsExactly(0L);

        catalog.alterTable(
                identifier,
                Arrays.asList(
                        SchemaChange.removeOption("full-compaction.delta-commits"),
                        SchemaChange.setOption("fields.seq.sequence-group", "v")),
                false);

        assertThat(schemaManager.latest().get().id()).isEqualTo(1L);
        assertThat(catalog.getTable(identifier).options())
                .doesNotContainKey("full-compaction.delta-commits")
                .containsEntry("fields.seq.sequence-group", "v");

        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        SchemaChange.setOption("fields.seq.sequence-group", "id,v"),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");
        assertThat(schemaManager.latest().get().id()).isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAlterTableWithInvalidOptionsAndSnapshots(boolean cacheEnabled)
            throws Exception {
        Identifier identifier = Identifier.create("test_db", "legacy_table");
        FileSystemSchemaManager schemaManager = createLegacyTable(identifier, cacheEnabled, true);
        Path tablePath =
                ((FileSystemCatalog) DelegateCatalog.rootCatalog(catalog))
                        .getTableLocation(identifier);
        SnapshotManager snapshotManager =
                new SnapshotManager(fileIO, tablePath, Identifier.DEFAULT_MAIN_BRANCH, null, null);
        assertThat(snapshotManager.latestSnapshot()).isNotNull();
        long snapshotId = snapshotManager.latestSnapshot().id();
        assertThat(snapshotManager.latestSnapshot().schemaId()).isZero();
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");

        List<SchemaChange> repairChanges =
                Arrays.asList(
                        SchemaChange.removeOption("full-compaction.delta-commits"),
                        SchemaChange.setOption("fields.seq.sequence-group", "v"));
        List<SchemaChange> immutableChanges = new ArrayList<>(repairChanges);
        immutableChanges.add(SchemaChange.setOption("merge-engine", "deduplicate"));
        assertThatThrownBy(() -> catalog.alterTable(identifier, immutableChanges, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'merge-engine' is not supported yet.");
        assertThat(schemaManager.listAllIds()).containsExactly(0L);
        assertThat(schemaManager.latest().get().options())
                .containsEntry("merge-engine", "partial-update")
                .containsEntry("full-compaction.delta-commits", "1")
                .containsEntry("fields.seq.sequence-group", "id,v");

        catalog.alterTable(identifier, repairChanges, false);

        FileStoreTable repairedTable = (FileStoreTable) catalog.getTable(identifier);
        assertThat(repairedTable.schema().id()).isEqualTo(1L);
        assertThat(repairedTable.options())
                .containsEntry("merge-engine", "partial-update")
                .containsEntry("fields.seq.sequence-group", "v")
                .doesNotContainKey("full-compaction.delta-commits");
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(snapshotManager.latestSnapshot().schemaId()).isZero();
        assertThat(read(repairedTable, null, null, null, null))
                .extracting(row -> row.getInt(0), row -> row.getInt(1), row -> row.getInt(2))
                .containsExactlyInAnyOrder(tuple(1, 10, 100), tuple(2, 20, 200));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAlterTableWithInvalidOptionsOnBranch(boolean cacheEnabled) throws Exception {
        Identifier identifier = Identifier.create("test_db", "legacy_table");
        FileSystemSchemaManager mainSchemaManager = createLegacyTable(identifier, cacheEnabled);
        TableSchema legacySchema = mainSchemaManager.latest().get();
        FileSystemSchemaManager branchSchemaManager = mainSchemaManager.copyWithBranch("dev");
        writeLegacySchema(branchSchemaManager, legacySchema, 0, legacySchema.options());
        Identifier branchIdentifier = new Identifier("test_db", "legacy_table", "dev");

        catalog.alterTable(
                branchIdentifier,
                Arrays.asList(
                        SchemaChange.removeOption("full-compaction.delta-commits"),
                        SchemaChange.setOption("fields.seq.sequence-group", "v")),
                false);

        assertThat(branchSchemaManager.latest().get().id()).isEqualTo(1L);
        assertThat(mainSchemaManager.latest().get().id()).isZero();
        assertThat(catalog.getTable(branchIdentifier).options())
                .containsEntry("fields.seq.sequence-group", "v");
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDropTableWithInvalidOptions(boolean cacheEnabled) throws Exception {
        Identifier identifier = Identifier.create("test_db", "legacy_table");
        createLegacyTable(identifier, cacheEnabled);
        Path tablePath =
                ((FileSystemCatalog) DelegateCatalog.rootCatalog(catalog))
                        .getTableLocation(identifier);

        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class);
        catalog.dropTable(identifier, false);

        assertThat(fileIO.exists(tablePath)).isFalse();
        assertThatThrownBy(() -> catalog.dropTable(identifier, false))
                .isInstanceOf(Catalog.TableNotExistException.class);
        catalog.dropTable(identifier, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDropTableWithInvalidOptionsAndExternalPaths(boolean cacheEnabled)
            throws Exception {
        Identifier identifier = Identifier.create("test_db", "legacy_table");
        FileSystemSchemaManager schemaManager = createLegacyTable(identifier, cacheEnabled);
        TableSchema legacySchema = schemaManager.latest().get();
        java.nio.file.Path oldMainPath = Files.createDirectory(tempFile.resolve("old-main-data"));
        java.nio.file.Path newMainPath = Files.createDirectory(tempFile.resolve("new-main-data"));
        java.nio.file.Path oldBranchPath =
                Files.createDirectory(tempFile.resolve("old-branch-index"));
        java.nio.file.Path newBranchPath =
                Files.createDirectory(tempFile.resolve("new-branch-index"));
        java.nio.file.Path unrelatedPath = Files.createDirectory(tempFile.resolve("unrelated"));

        Map<String, String> mainOptions = new HashMap<>(legacySchema.options());
        mainOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), oldMainPath.toUri().toString());
        writeLegacySchema(schemaManager, legacySchema, 0, mainOptions);
        mainOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), newMainPath.toUri().toString());
        writeLegacySchema(schemaManager, legacySchema, 1, mainOptions);

        FileSystemSchemaManager branchSchemaManager = schemaManager.copyWithBranch("dev");
        Map<String, String> branchOptions = new HashMap<>(legacySchema.options());
        branchOptions.put(
                CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(), oldBranchPath.toUri().toString());
        writeLegacySchema(branchSchemaManager, legacySchema, 0, branchOptions);
        branchOptions.put(
                CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(), newBranchPath.toUri().toString());
        writeLegacySchema(branchSchemaManager, legacySchema, 1, branchOptions);

        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(IllegalArgumentException.class);
        catalog.dropTable(identifier, false);

        for (java.nio.file.Path externalPath :
                Arrays.asList(oldMainPath, newMainPath, oldBranchPath, newBranchPath)) {
            assertThat(externalPath).doesNotExist();
        }
        assertThat(unrelatedPath).exists();
    }

    @ParameterizedTest
    @EnumSource(
            value = TableType.class,
            names = {"FORMAT_TABLE", "OBJECT_TABLE"})
    public void testDropNonDataTablePreservesExternalPaths(TableType tableType) throws Exception {
        Identifier identifier = Identifier.create("test_db", "non_data_table");
        java.nio.file.Path externalPath = Files.createDirectory(tempFile.resolve("external-data"));
        catalog.createDatabase(identifier.getDatabaseName(), false);
        // The filesystem catalog can load these table types but does not create them.
        Path tablePath = ((FileSystemCatalog) catalog).getTableLocation(identifier);
        new FileSystemSchemaManager(fileIO, tablePath)
                .createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option(CoreOptions.TYPE.key(), tableType.toString())
                                .option(
                                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                                        externalPath.toUri().toString())
                                .build());

        catalog.dropTable(identifier, false);

        assertThat(externalPath).exists();
    }

    private FileSystemSchemaManager createLegacyTable(Identifier identifier, boolean cacheEnabled)
            throws Exception {
        return createLegacyTable(identifier, cacheEnabled, false);
    }

    private FileSystemSchemaManager createLegacyTable(
            Identifier identifier, boolean cacheEnabled, boolean withData) throws Exception {
        if (cacheEnabled) {
            catalog = new CachingCatalog(catalog, new Options());
        }
        catalog.createDatabase(identifier.getDatabaseName(), false);
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("merge-engine", "partial-update")
                        .option("changelog-producer", "lookup")
                        .option("fields.seq.sequence-group", "v")
                        .build(),
                false);
        Path tablePath =
                ((FileSystemCatalog) DelegateCatalog.rootCatalog(catalog))
                        .getTableLocation(identifier);
        FileSystemSchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tablePath);
        if (withData) {
            BatchWriteBuilder writeBuilder = catalog.getTable(identifier).newBatchWriteBuilder();
            try (IOManager ioManager = IOManager.create(tempFile.resolve("io").toString());
                    BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                write.write(GenericRow.of(1, 10, 100));
                write.write(GenericRow.of(2, 20, 200));
                commit.commit(write.prepareCommit());
            }
            catalog.invalidateTable(identifier);
        }
        TableSchema schema = schemaManager.latest().get();
        Map<String, String> options = new HashMap<>(schema.options());
        options.put("full-compaction.delta-commits", "1");
        options.put("fields.seq.sequence-group", "id,v");
        // Persist a legacy schema without passing it through the current CREATE validator.
        writeLegacySchema(schemaManager, schema, schema.id(), options);
        return schemaManager;
    }

    private void writeLegacySchema(
            FileSystemSchemaManager schemaManager,
            TableSchema schema,
            long id,
            Map<String, String> options)
            throws Exception {
        TableSchema legacySchema =
                new TableSchema(
                        id,
                        schema.fields(),
                        schema.highestFieldId(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        new HashMap<>(options),
                        schema.comment());
        Path schemaPath = schemaManager.toSchemaPath(id);
        fileIO.mkdirs(schemaPath.getParent());
        fileIO.overwriteFileUtf8(schemaPath, legacySchema.toString());
    }
}
