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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for tables using {@link ExternalPathStrategy#ENTROPY_INJECT_TEMPLATE}, whose data files
 * live in external paths shared with other tables.
 */
public class EntropyInjectTemplateTest {

    private static final String DATABASE = "shared_db";

    @TempDir java.nio.file.Path tempPath;

    private Catalog catalog;
    private java.nio.file.Path sharedRoot;

    @BeforeEach
    public void before() throws Exception {
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new Path("file://" + tempPath + "/warehouse")));
        catalog.createDatabase(DATABASE, true);
        sharedRoot = tempPath.resolve("shared");
    }

    @Test
    public void testTablesShareHashPrefixes() throws Exception {
        FileStoreTable t1 = createTable("t1");
        FileStoreTable t2 = createTable("t2");
        write(t1, 1, "a");
        write(t2, 2, "b");

        // every data file is laid out as <hash dirs>/<db>.db/<table>/bucket-0/<file>
        for (String file : listSharedFiles()) {
            assertThat(file)
                    .matches(
                            "[01]{4}/[01]{4}/[01]{4}/[01]{8}/"
                                    + DATABASE
                                    + "\\.db/t[12]/bucket-0/.+");
        }
        assertThat(sharedTableFiles("t1")).hasSize(1);
        assertThat(sharedTableFiles("t2")).hasSize(1);
    }

    @Test
    public void testExpireSnapshotsDeletesSharedFiles() throws Exception {
        FileStoreTable table = createTable("t1");
        write(table, 1, "a");
        write(table, 2, "b");
        write(table, 3, "c");
        assertThat(sharedTableFiles("t1")).hasSize(3);

        // the overwrite makes the three files unreferenced by the latest snapshot
        overwrite(table, 4, "d");
        assertThat(sharedTableFiles("t1")).hasSize(4);

        table.newExpireSnapshots()
                .config(ExpireConfig.builder().snapshotRetainMax(1).snapshotRetainMin(1).build())
                .expire();

        // expiration locates the files by their full external path, the overwritten ones are gone
        assertThat(sharedTableFiles("t1")).hasSize(1);
    }

    @Test
    public void testDropTableOnlyDeletesItsOwnFiles() throws Exception {
        FileStoreTable t1 = createTable("t1");
        FileStoreTable t2 = createTable("t2");
        write(t1, 1, "a");
        write(t1, 2, "b");
        write(t2, 3, "c");
        Set<String> t2Files = sharedTableFiles("t2");
        assertThat(sharedTableFiles("t1")).hasSize(2);
        assertThat(t2Files).hasSize(1);

        catalog.dropTable(Identifier.create(DATABASE, "t1"), false);

        assertThat(sharedTableFiles("t1")).isEmpty();
        // the shared hash directories and the files of the other table are untouched
        assertThat(sharedTableFiles("t2")).isEqualTo(t2Files);
    }

    /**
     * Deletion walks one manifest at a time, and a file overwritten by a later snapshot is recorded
     * as an {@code ADD} entry in one manifest and as a {@code DELETE} entry in another. Both kinds
     * have to be followed, and following the same file twice has to stay harmless.
     */
    @Test
    public void testDropTableAcrossManyManifests() throws Exception {
        FileStoreTable t1 = createTable("t1");
        FileStoreTable t2 = createTable("t2");
        write(t2, 0, "keep");
        Set<String> t2Files = sharedTableFiles("t2");

        for (int i = 1; i <= 6; i++) {
            write(t1, i, "v" + i);
        }
        overwrite(t1, 7, "v7");
        for (int i = 8; i <= 10; i++) {
            write(t1, i, "v" + i);
        }
        assertThat(sharedTableFiles("t1")).hasSize(10);
        assertThat(manifestCount(t1)).isGreaterThan(1);

        catalog.dropTable(Identifier.create(DATABASE, "t1"), false);

        assertThat(sharedTableFiles("t1")).isEmpty();
        assertThat(sharedTableFiles("t2")).isEqualTo(t2Files);
    }

    /** The number of distinct manifests the deletion walks over. */
    private int manifestCount(FileStoreTable t) throws Exception {
        ManifestList manifestList = t.store().manifestListFactory().create();
        Set<String> names = new HashSet<>();
        for (Snapshot snapshot : t.snapshotManager().safelyGetAllSnapshots()) {
            manifestList.readAllManifests(snapshot).forEach(meta -> names.add(meta.fileName()));
        }
        return names.size();
    }

    /** The literal template path is excluded from recursive directory cleanup. */
    @Test
    public void testDropTablePreservesTheLiteralPlaceholderDirectory() throws Exception {
        FileStoreTable t1 = createTable("t1");
        FileStoreTable t2 = createTable("t2");
        write(t1, 1, "a");
        write(t2, 2, "b");
        Set<String> t2Files = sharedTableFiles("t2");

        java.nio.file.Path literal =
                sharedRoot.resolve("{hash}").resolve(DATABASE + ".db").resolve("t1");
        Files.createDirectories(literal);
        Files.write(literal.resolve("legacy.parquet"), new byte[] {1});

        catalog.dropTable(Identifier.create(DATABASE, "t1"), false);

        assertThat(sharedTableFiles("t1"))
                .containsExactly(
                        sharedRoot.relativize(literal.resolve("legacy.parquet")).toString());
        assertThat(sharedTableFiles("t2")).isEqualTo(t2Files);
    }

    /**
     * An external table keeps its data, the way {@code HiveCatalog} leaves the directories of one
     * alone. Dropping then leaves the files behind, and because they sit under prefixes shared with
     * other tables they can no longer be told apart by path alone.
     */
    @Test
    public void testDropTableKeepsTheFilesOfAnExternalTable() throws Exception {
        java.nio.file.Path warehouse = tempPath.resolve("external-warehouse");
        try (Catalog external =
                new FileSystemCatalog(LocalFileIO.create(), new Path("file://" + warehouse)) {
                    @Override
                    protected TableMetadata loadTableMetadata(Identifier identifier)
                            throws TableNotExistException {
                        return new TableMetadata(loadTableSchema(identifier), true, null);
                    }
                }) {
            external.createDatabase(DATABASE, true);
            Identifier id = Identifier.create(DATABASE, "kept");
            external.createTable(id, sharedSchema("kept"), false);
            FileStoreTable t = (FileStoreTable) external.getTable(id);
            write(t, 1, "a");
            Set<String> files = sharedTableFiles("kept");
            assertThat(files).hasSize(1);

            external.dropTable(id, false);

            assertThat(sharedTableFiles("kept")).isEqualTo(files);
        }
    }

    @Test
    public void testRemoveOrphanFilesIsNotSupported() throws Exception {
        FileStoreTable table = createTable("t1");
        write(table, 1, "a");

        assertThatThrownBy(() -> new LocalOrphanFilesClean(table))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
    }

    @ParameterizedTest
    @ValueSource(strings = {"/bad", "/{hash}/.."})
    public void testExternalPathMustContainPlaceholder(String suffix) {
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create(DATABASE, "bad"),
                                        schemaBuilder()
                                                .option(
                                                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                                                        "file://" + sharedRoot + suffix)
                                                .option(
                                                        CoreOptions
                                                                .DATA_FILE_EXTERNAL_PATHS_STRATEGY
                                                                .key(),
                                                        ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE
                                                                .toString())
                                                .build(),
                                        false))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("{hash}");
    }

    @Test
    public void testRoundRobinAcceptsLiteralPlaceholderDirectory() throws Exception {
        Identifier identifier = Identifier.create(DATABASE, "literal");
        catalog.createTable(
                identifier,
                schemaBuilder()
                        .option(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                                "file://" + sharedRoot + "/{hash}/literal")
                        .option(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                ExternalPathStrategy.ROUND_ROBIN.toString())
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        write(table, 1, "a");

        assertThat(table.newReadBuilder().newScan().plan().splits()).isNotEmpty();
        assertThat(Files.isDirectory(sharedRoot.resolve("{hash}").resolve("literal"))).isTrue();
    }

    @Test
    public void testRejectsChainTableWithTemplateStrategy() throws Exception {
        Identifier identifier = Identifier.create(DATABASE, "chain_template");
        String chainEnabled = CoreOptions.CHAIN_TABLE_ENABLED.key();
        Schema schema =
                schemaBuilder()
                        .partitionKeys("name")
                        .primaryKey("name", "id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "id")
                        .option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$name")
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd")
                        .option(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                                "file://"
                                        + sharedRoot
                                        + "/{hash}/"
                                        + DATABASE
                                        + ".db/chain_template")
                        .option(
                                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString())
                        .option(chainEnabled, "true")
                        .build();
        String message = "Chain tables do not support the 'entropy-inject-template'";
        assertThatThrownBy(() -> catalog.createTable(identifier, schema, false))
                .rootCause()
                .hasMessageContaining(message);

        schema.options().remove(chainEnabled);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        write(table, 1, "2026-11-19");
        assertThat(sharedTableFiles("chain_template")).hasSize(1);
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Collections.singletonList(
                                                SchemaChange.setOption(chainEnabled, "true")),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);

        // Simulate an existing table whose schema was persisted before validation.
        schema.options().put(chainEnabled, "true");
        long legacySchemaId = table.schema().id() + 1;
        FileSystemSchemaManager schemaManager =
                new FileSystemSchemaManager(table.fileIO(), table.location());
        assertThat(
                        table.fileIO()
                                .tryToWriteAtomic(
                                        schemaManager.toSchemaPath(legacySchemaId),
                                        TableSchema.create(legacySchemaId, schema).toString()))
                .isTrue();

        assertThatThrownBy(() -> catalog.dropTable(identifier, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
        assertThat(sharedTableFiles("chain_template")).hasSize(1);
    }

    @Test
    public void testTemplateRequiresAnExternalPathOnCreateAndAlter() throws Exception {
        String strategy = CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key();
        String template = ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString();
        Identifier identifier = Identifier.create(DATABASE, "missing_path");
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        identifier,
                                        schemaBuilder().option(strategy, template).build(),
                                        false))
                .rootCause()
                .hasMessageContaining("requires a non-empty template");
        catalog.createTable(identifier, schemaBuilder().build(), false);
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Collections.singletonList(
                                                SchemaChange.setOption(strategy, template)),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a non-empty template");
    }

    @Test
    public void testOnlyOneTemplateIsAllowed() {
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create(DATABASE, "bad"),
                                        schemaBuilder()
                                                .option(
                                                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                                                        "file://"
                                                                + sharedRoot
                                                                + "/a/{hash}/bad,file://"
                                                                + sharedRoot
                                                                + "/b/{hash}/bad")
                                                .option(
                                                        CoreOptions
                                                                .DATA_FILE_EXTERNAL_PATHS_STRATEGY
                                                                .key(),
                                                        ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE
                                                                .toString())
                                                .build(),
                                        false))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only accepts a single path");
    }

    private Schema.Builder schemaBuilder() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .option(CoreOptions.BUCKET.key(), "-1");
    }

    private Schema sharedSchema(String tableName) {
        return schemaBuilder()
                .option(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                        "file://" + sharedRoot + "/{hash}/" + DATABASE + ".db/" + tableName)
                .option(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                        ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString())
                .build();
    }

    private FileStoreTable createTable(String tableName) throws Exception {
        Identifier identifier = Identifier.create(DATABASE, tableName);
        catalog.createTable(identifier, sharedSchema(tableName), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void write(FileStoreTable table, int id, String name) throws Exception {
        write(table.newBatchWriteBuilder(), id, name);
    }

    private void overwrite(FileStoreTable table, int id, String name) throws Exception {
        write(table.newBatchWriteBuilder().withOverwrite(), id, name);
    }

    private void write(BatchWriteBuilder builder, int id, String name) throws Exception {
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name)));
            commit.commit(write.prepareCommit());
        }
    }

    /** All data file paths under the shared root, relative to it. */
    private Set<String> listSharedFiles() throws IOException {
        if (!Files.exists(sharedRoot)) {
            return Collections.emptySet();
        }
        try (Stream<java.nio.file.Path> files = Files.walk(sharedRoot)) {
            return files.filter(Files::isRegularFile)
                    .map(sharedRoot::relativize)
                    .map(java.nio.file.Path::toString)
                    .collect(Collectors.toSet());
        }
    }

    private Set<String> sharedTableFiles(String tableName) throws IOException {
        return listSharedFiles().stream()
                .filter(file -> file.contains("/" + DATABASE + ".db/" + tableName + "/"))
                .collect(Collectors.toSet());
    }
}
