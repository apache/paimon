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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.MySqlStartupSchemaEvolution.IncompatibleSchemaException;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.schemaCompatible;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for repairing MySQL schema drift before building a CDC job. */
class MySqlStartupSchemaEvolutionTest {

    private static final Identifier IDENTIFIER = Identifier.create("test_db", "test_table");

    @TempDir private Path warehouse;

    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        catalog =
                new FileSystemCatalog(
                        LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toString()));
        catalog.createDatabase(IDENTIFIER.getDatabaseName(), false);
        catalog.createTable(
                IDENTIFIER,
                baseSchema().option("bucket", "1").option("bucket-key", "id").build(),
                false);
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @Test
    void testAddsNonNullableSourceColumnAsNullable() throws Exception {
        FileStoreTable original = table();
        Schema source =
                baseSchema()
                        .column("added", DataTypes.INT().notNull(), "new source column")
                        .build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fieldNames()).containsExactly("id", "value", "added");
        assertThat(repaired.schema().fields().get(2).type()).isEqualTo(DataTypes.INT());
        assertThat(repaired.schema().fields().get(2).description()).isEqualTo("new source column");
        assertThat(repaired.schema().fields().get(0)).isEqualTo(original.schema().fields().get(0));
        assertThat(repaired.primaryKeys()).containsExactly("id");
        assertThat(schemaCompatible(repaired.schema(), source.fields())).isTrue();
        assertThat(repair(repaired, source, source)).isSameAs(repaired);
        assertThat(table().schema().id()).isEqualTo(repaired.schema().id());
    }

    @Test
    void testIncompatibleExistingColumnPreventsPartialAdd() throws Exception {
        FileStoreTable original = table();
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("added", DataTypes.INT())
                        .column("value", DataTypes.DATE())
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
        assertThat(schemaCompatible(original.schema(), source.fields())).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testChangedPrimaryKeyPreventsAddingColumns(boolean missingKeyColumn) throws Exception {
        FileStoreTable original = table();
        Schema source =
                baseSchema()
                        .column("added", DataTypes.INT())
                        .primaryKey(missingKeyColumn ? "added" : "value")
                        .build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testExplicitPartitionChangePreventsAddingColumns() throws Exception {
        FileStoreTable original = table();
        Schema source = baseSchema().column("added", DataTypes.INT()).build();
        Schema expected =
                baseSchema().column("added", DataTypes.INT()).partitionKeys("value").build();

        assertThatThrownBy(() -> repair(original, expected, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testExplicitBucketKeyChangePreventsAddingColumns() throws Exception {
        FileStoreTable original = table();
        Schema source = baseSchema().column("added", DataTypes.INT()).build();

        assertThatThrownBy(
                        () ->
                                MySqlStartupSchemaEvolution.addMissingColumns(
                                        catalog,
                                        IDENTIFIER,
                                        original,
                                        source,
                                        source,
                                        true,
                                        Collections.singletonMap("bucket-key", "value"),
                                        TypeMapping.defaultMapping()))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testMissingSyntheticColumnPreventsPartialAdd() throws Exception {
        FileStoreTable original = table();
        Schema source = baseSchema().column("added", DataTypes.INT()).build();
        Schema expected =
                baseSchema()
                        .column("added", DataTypes.INT())
                        .column("computed", DataTypes.STRING())
                        .build();

        assertThatThrownBy(() -> repair(original, expected, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testWidensVarcharWithoutMissingColumns() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.VARCHAR(32).notNull());
        Schema source = baseSchema(DataTypes.VARCHAR(128)).build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fieldNames()).containsExactly("id", "value");
        assertThat(repaired.schema().fields().get(1).type())
                .isEqualTo(DataTypes.VARCHAR(128).notNull());
        assertThat(repaired.schema().fields().get(1).id())
                .isEqualTo(original.schema().fields().get(1).id());
        assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 1);
        assertThat(repair(repaired, source, source)).isSameAs(repaired);
        assertThat(table().schema().id()).isEqualTo(repaired.schema().id());
    }

    @Test
    void testWidensIntegerAndAddsColumnInOneSchemaVersion() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.INT());
        Schema source =
                baseSchema(DataTypes.BIGINT())
                        .column("added", DataTypes.STRING().notNull())
                        .build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fieldNames()).containsExactly("id", "value", "added");
        assertThat(repaired.schema().fields().get(1).type()).isEqualTo(DataTypes.BIGINT());
        assertThat(repaired.schema().fields().get(1).id())
                .isEqualTo(original.schema().fields().get(1).id());
        assertThat(repaired.schema().fields().get(2).type()).isEqualTo(DataTypes.STRING());
        assertThat(repaired.schema().fields().get(2).id())
                .isEqualTo(original.schema().highestFieldId() + 1);
        assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 1);
    }

    @Test
    void testStringToIntegerPreventsPartialAdd() throws Exception {
        FileStoreTable original = table();
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("added", DataTypes.INT())
                        .column("value", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testKeepsWiderTargetWithoutSchemaChange() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.BIGINT());
        Schema source = baseSchema(DataTypes.INT()).build();

        assertThat(repair(original, source, source)).isSameAs(original);
        assertUnchanged(original);
        assertThat(table().schema().fields().get(1).type()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testKeepsWiderPrimaryKeyAndAddsOrdinaryColumn() throws Exception {
        catalog.dropTable(IDENTIFIER, false);
        catalog.createTable(
                IDENTIFIER,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .build(),
                false);
        FileStoreTable original = table();
        Schema source = baseSchema().column("added", DataTypes.INT()).build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fieldNames()).containsExactly("id", "value", "added");
        assertThat(repaired.schema().fields().get(0)).isEqualTo(original.schema().fields().get(0));
        assertThat(repaired.schema().fields().get(0).type())
                .isEqualTo(DataTypes.BIGINT().notNull());
        assertThat(repaired.schema().fields().get(2).type()).isEqualTo(DataTypes.INT());
        assertThat(repaired.primaryKeys()).containsExactly("id");
        assertThat(repaired.schema().bucketKeys()).containsExactly("id");
        assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 1);
        assertThat(schemaCompatible(repaired.schema(), source.fields())).isTrue();
        assertThat(table().schema()).isEqualTo(repaired.schema());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testKeepsHigherTargetTimePrecision(boolean addColumn) throws Exception {
        FileStoreTable original = recreateTable(DataTypes.TIME(3).notNull());
        Schema.Builder sourceBuilder = baseSchema(DataTypes.TIME(0));
        if (addColumn) {
            sourceBuilder.column("added", DataTypes.INT());
        }
        Schema source = sourceBuilder.build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fields().get(1).type()).isEqualTo(DataTypes.TIME(3).notNull());
        assertThat(repaired.schema().fields().get(1).id())
                .isEqualTo(original.schema().fields().get(1).id());
        assertThat(repaired.schema().fieldNames())
                .containsExactlyElementsOf(
                        addColumn
                                ? Arrays.asList("id", "value", "added")
                                : Arrays.asList("id", "value"));
        assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + (addColumn ? 1 : 0));
        assertThat(repair(repaired, source, source)).isSameAs(repaired);
        assertThat(table().schema()).isEqualTo(repaired.schema());
    }

    @Test
    void testWidensDecimalWithPreservedIntegerRange() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.DECIMAL(10, 2));
        Schema source = baseSchema(DataTypes.DECIMAL(13, 4)).build();

        FileStoreTable repaired = repair(original, source, source);

        assertThat(repaired.schema().fields().get(1).type()).isEqualTo(DataTypes.DECIMAL(13, 4));
        assertThat(repaired.schema().fields().get(1).id())
                .isEqualTo(original.schema().fields().get(1).id());
        assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 1);
    }

    @Test
    void testDecimalScaleIncreaseLosingIntegerRangePreventsPartialAdd() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.DECIMAL(10, 2));
        Schema source =
                baseSchema(DataTypes.DECIMAL(10, 4)).column("added", DataTypes.INT()).build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testDecimalNoChangePreventsRequiredWidening() throws Exception {
        FileStoreTable original = recreateTable(DataTypes.DECIMAL(10, 2));
        Schema source =
                baseSchema(DataTypes.DECIMAL(13, 4)).column("added", DataTypes.INT()).build();

        assertThatThrownBy(
                        () ->
                                MySqlStartupSchemaEvolution.addMissingColumns(
                                        catalog,
                                        IDENTIFIER,
                                        original,
                                        source,
                                        source,
                                        true,
                                        Collections.emptyMap(),
                                        TypeMapping.parse(new String[] {"decimal-no-change"})))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testPrimaryKeyTypeChangeRejectedWithoutMissingColumns() throws Exception {
        FileStoreTable original = table();
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
    }

    @Test
    void testPrimaryKeyIndexTypeChangePreventsPartialAdd() throws Exception {
        catalog.dropTable(IDENTIFIER, false);
        catalog.createTable(
                IDENTIFIER,
                baseSchema(DataTypes.INT())
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("deletion-vectors.enabled", "true")
                        .option("global-index.bitmap.index-columns", "value")
                        .build(),
                false);
        FileStoreTable original = table();
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("added", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(() -> repair(original, source, source))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertUnchanged(original);
        assertThat(table().schema().fieldNames()).containsExactly("id", "value");
    }

    @Test
    void testConcurrentSameNameAdditionReloadsCacheAndAddsRemainingColumn() throws Exception {
        Schema source =
                baseSchema()
                        .column("added", DataTypes.INT())
                        .column("remaining", DataTypes.STRING())
                        .build();
        try (Catalog cached = concurrentCatalog(DataTypes.INT())) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            FileStoreTable repaired =
                    MySqlStartupSchemaEvolution.addMissingColumns(
                            cached,
                            IDENTIFIER,
                            original,
                            source,
                            source,
                            true,
                            Collections.emptyMap(),
                            TypeMapping.defaultMapping());

            assertThat(repaired.schema().fieldNames())
                    .containsExactly("id", "value", "added", "remaining");
            assertThat(table().schema().fieldNames())
                    .containsExactly("id", "value", "added", "remaining");
            assertThat(repaired.schema().fields())
                    .extracting(DataField::id)
                    .doesNotHaveDuplicates();
            assertThat(schemaCompatible(repaired.schema(), source.fields())).isTrue();
        }
    }

    @Test
    void testRestSchemaVersionConflictReloadsAndRetries() throws Exception {
        String warehousePath = warehouse.resolve("rest").toUri().toString();
        String token = "test-token";
        ConfigResponse config =
                new ConfigResponse(
                        Collections.singletonMap(RESTCatalogInternalOptions.PREFIX.key(), "paimon"),
                        Collections.emptyMap());
        RESTCatalogServer server =
                new RESTCatalogServer(
                        warehousePath, new BearTokenAuthProvider(token), config, warehousePath);
        server.start();
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehousePath);
        options.set(RESTCatalogOptions.URI, server.getUrl());
        options.set(RESTCatalogOptions.TOKEN, token);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());

        try (Catalog writer = new RESTCatalog(CatalogContext.create(options));
                Catalog cached =
                        new CachingCatalog(
                                new RESTCatalog(CatalogContext.create(options)), options)) {
            writer.createDatabase(IDENTIFIER.getDatabaseName(), false);
            writer.createTable(IDENTIFIER, baseSchema().build(), false);
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);
            writer.alterTable(IDENTIFIER, SchemaChange.addColumn("added", DataTypes.INT()), false);
            assertThatThrownBy(
                            () ->
                                    writer.alterTable(
                                            IDENTIFIER,
                                            Arrays.asList(
                                                    SchemaChange.checkSchemaVersion(
                                                            original.schema().id()),
                                                    SchemaChange.addColumn(
                                                            "remaining", DataTypes.STRING())),
                                            false))
                    .isInstanceOf(ConcurrentModificationException.class);

            Schema source =
                    baseSchema()
                            .column("added", DataTypes.INT())
                            .column("remaining", DataTypes.STRING())
                            .build();
            FileStoreTable repaired =
                    MySqlStartupSchemaEvolution.addMissingColumns(
                            cached,
                            IDENTIFIER,
                            original,
                            source,
                            source,
                            true,
                            Collections.emptyMap(),
                            TypeMapping.defaultMapping());

            assertThat(repaired.schema().fieldNames())
                    .containsExactly("id", "value", "added", "remaining");
            assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 2);
            FileStoreTable persisted = (FileStoreTable) writer.getTable(IDENTIFIER);
            assertThat(persisted.schema().id()).isEqualTo(repaired.schema().id());
            assertThat(persisted.schema().fields()).isEqualTo(repaired.schema().fields());
            assertThatThrownBy(
                            () ->
                                    writer.alterTable(
                                            IDENTIFIER,
                                            SchemaChange.addColumn("added", DataTypes.INT()),
                                            false))
                    .isInstanceOf(Catalog.ColumnAlreadyExistException.class);
        } finally {
            server.shutdown();
        }
    }

    @Test
    void testLastConcurrentAdditionCompletesRepair() throws Exception {
        Schema source =
                baseSchema()
                        .column("added", DataTypes.INT())
                        .column("remaining", DataTypes.STRING())
                        .column("last_added", DataTypes.BIGINT())
                        .build();
        try (Catalog cached =
                concurrentCatalog(
                        SchemaChange.addColumn("added", DataTypes.INT()),
                        SchemaChange.addColumn("remaining", DataTypes.STRING()),
                        SchemaChange.addColumn("last_added", DataTypes.BIGINT()))) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            FileStoreTable repaired =
                    MySqlStartupSchemaEvolution.addMissingColumns(
                            cached,
                            IDENTIFIER,
                            original,
                            source,
                            source,
                            true,
                            Collections.emptyMap(),
                            TypeMapping.defaultMapping());

            assertThat(repaired.schema().fieldNames())
                    .containsExactly("id", "value", "added", "remaining", "last_added");
            assertThat(repaired.schema().fields())
                    .extracting(DataField::id)
                    .doesNotHaveDuplicates();
            assertThat(repaired.schema().id()).isEqualTo(original.schema().id() + 3);
            assertThat(table().schema()).isEqualTo(repaired.schema());
            assertThat(schemaCompatible(repaired.schema(), source.fields())).isTrue();
        }
    }

    @Test
    void testConcurrentAddOnlyPlanRejectsIncompatibleTypeChange() throws Exception {
        FileStoreTable initial = recreateTable(DataTypes.INT());
        Schema source = baseSchema(DataTypes.INT()).column("added", DataTypes.STRING()).build();
        try (Catalog cached =
                concurrentCatalog(SchemaChange.updateColumnType("value", DataTypes.STRING()))) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            assertThatThrownBy(
                            () ->
                                    MySqlStartupSchemaEvolution.addMissingColumns(
                                            cached,
                                            IDENTIFIER,
                                            original,
                                            source,
                                            source,
                                            true,
                                            Collections.emptyMap(),
                                            TypeMapping.defaultMapping()))
                    .isInstanceOf(IncompatibleSchemaException.class);

            assertThat(table().schema().fieldNames()).containsExactly("id", "value");
            assertThat(table().schema().fields().get(1).type()).isEqualTo(DataTypes.STRING());
            assertThat(table().schema().id()).isEqualTo(initial.schema().id() + 1);
        }
    }

    @Test
    void testConcurrentWideningReloadsCacheWithoutNarrowing() throws Exception {
        FileStoreTable initial = recreateTable(DataTypes.TINYINT());
        Schema source = baseSchema(DataTypes.INT()).column("added", DataTypes.STRING()).build();
        try (Catalog cached =
                concurrentCatalog(SchemaChange.updateColumnType("value", DataTypes.BIGINT()))) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            FileStoreTable repaired =
                    MySqlStartupSchemaEvolution.addMissingColumns(
                            cached,
                            IDENTIFIER,
                            original,
                            source,
                            source,
                            true,
                            Collections.emptyMap(),
                            TypeMapping.defaultMapping());

            assertThat(repaired.schema().fieldNames()).containsExactly("id", "value", "added");
            assertThat(repaired.schema().fields().get(1).type()).isEqualTo(DataTypes.BIGINT());
            assertThat(table().schema()).isEqualTo(repaired.schema());
            assertThat(repaired.schema().id()).isEqualTo(initial.schema().id() + 2);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentColumnRemovalReloadsBeforeWidening(boolean renameColumn) throws Exception {
        FileStoreTable initial = recreateTable(DataTypes.TINYINT());
        Schema source = baseSchema(DataTypes.INT()).column("added", DataTypes.STRING()).build();
        SchemaChange competingChange =
                renameColumn
                        ? SchemaChange.renameColumn("value", "renamed_value")
                        : SchemaChange.dropColumn("value");
        try (Catalog cached = concurrentCatalog(competingChange)) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            FileStoreTable repaired =
                    MySqlStartupSchemaEvolution.addMissingColumns(
                            cached,
                            IDENTIFIER,
                            original,
                            source,
                            source,
                            true,
                            Collections.emptyMap(),
                            TypeMapping.defaultMapping());

            assertThat(repaired.schema().fieldNames())
                    .containsExactlyElementsOf(
                            renameColumn
                                    ? Arrays.asList("id", "renamed_value", "value", "added")
                                    : Arrays.asList("id", "value", "added"));
            DataField recreated = repaired.schema().nameToFieldMap().get("value");
            assertThat(recreated.type()).isEqualTo(DataTypes.INT());
            assertThat(recreated.id()).isGreaterThan(initial.schema().highestFieldId());
            assertThat(repaired.schema().id()).isEqualTo(initial.schema().id() + 2);
            assertThat(table().schema()).isEqualTo(repaired.schema());
            assertThat(schemaCompatible(repaired.schema(), source.fields())).isTrue();
        }
    }

    @Test
    void testLastConcurrentIncompatibleChangePreservesConflict() throws Exception {
        FileStoreTable initial = recreateTable(DataTypes.INT());
        Schema source =
                baseSchema(DataTypes.INT())
                        .column("added", DataTypes.INT())
                        .column("remaining", DataTypes.STRING())
                        .column("last_added", DataTypes.BIGINT())
                        .build();
        try (Catalog cached =
                concurrentCatalog(
                        SchemaChange.addColumn("added", DataTypes.INT()),
                        SchemaChange.addColumn("remaining", DataTypes.STRING()),
                        SchemaChange.updateColumnType("value", DataTypes.STRING()))) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            assertThatThrownBy(
                            () ->
                                    MySqlStartupSchemaEvolution.addMissingColumns(
                                            cached,
                                            IDENTIFIER,
                                            original,
                                            source,
                                            source,
                                            true,
                                            Collections.emptyMap(),
                                            TypeMapping.defaultMapping()))
                    .isInstanceOf(IncompatibleSchemaException.class)
                    .satisfies(
                            error -> {
                                assertThat(error.getSuppressed()).hasSize(1);
                                assertThat(error.getSuppressed()[0])
                                        .isInstanceOf(ConcurrentModificationException.class);
                            });

            assertThat(table().schema().fieldNames())
                    .containsExactly("id", "value", "added", "remaining");
            assertThat(table().schema().fields().get(1).type()).isEqualTo(DataTypes.STRING());
            assertThat(table().schema().id()).isEqualTo(initial.schema().id() + 3);
        }
    }

    @Test
    void testConcurrentIncompatibleTypeChangePreventsPartialAdd() throws Exception {
        FileStoreTable initial = recreateTable(DataTypes.TINYINT());
        Schema source = baseSchema(DataTypes.INT()).column("added", DataTypes.STRING()).build();
        try (Catalog cached =
                concurrentCatalog(SchemaChange.updateColumnType("value", DataTypes.STRING()))) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            assertThatThrownBy(
                            () ->
                                    MySqlStartupSchemaEvolution.addMissingColumns(
                                            cached,
                                            IDENTIFIER,
                                            original,
                                            source,
                                            source,
                                            true,
                                            Collections.emptyMap(),
                                            TypeMapping.defaultMapping()))
                    .isInstanceOf(IncompatibleSchemaException.class);

            assertThat(table().schema().fieldNames()).containsExactly("id", "value");
            assertThat(table().schema().fields().get(1).type()).isEqualTo(DataTypes.STRING());
            assertThat(table().schema().id()).isEqualTo(initial.schema().id() + 1);
        }
    }

    @Test
    void testConcurrentIncompatibleAdditionPreventsRemainingColumn() throws Exception {
        Schema source =
                baseSchema()
                        .column("added", DataTypes.INT())
                        .column("remaining", DataTypes.STRING())
                        .build();
        try (Catalog cached = concurrentCatalog(DataTypes.DATE())) {
            FileStoreTable original = (FileStoreTable) cached.getTable(IDENTIFIER);

            assertThatThrownBy(
                            () ->
                                    MySqlStartupSchemaEvolution.addMissingColumns(
                                            cached,
                                            IDENTIFIER,
                                            original,
                                            source,
                                            source,
                                            true,
                                            Collections.emptyMap(),
                                            TypeMapping.defaultMapping()))
                    .isInstanceOf(IncompatibleSchemaException.class);

            assertThat(table().schema().fieldNames()).containsExactly("id", "value", "added");
            assertThat(table().schema().fields().get(2).type()).isEqualTo(DataTypes.DATE());
            assertThat(table().schema().id()).isEqualTo(original.schema().id() + 1);
            assertThat(schemaCompatible(table().schema(), source.fields())).isFalse();
        }
    }

    private FileStoreTable repair(FileStoreTable table, Schema expected, Schema source)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        return MySqlStartupSchemaEvolution.addMissingColumns(
                catalog,
                IDENTIFIER,
                table,
                expected,
                source,
                true,
                Collections.emptyMap(),
                TypeMapping.defaultMapping());
    }

    private Catalog concurrentCatalog(DataType competingType) {
        return concurrentCatalog(SchemaChange.addColumn("added", competingType));
    }

    private Catalog concurrentCatalog(SchemaChange... competingChanges) {
        Catalog competingWriter = catalog;
        return new CachingCatalog(
                new DelegateCatalog(catalog) {

                    private int alterCount;

                    @Override
                    public CatalogLoader catalogLoader() {
                        return competingWriter.catalogLoader();
                    }

                    @Override
                    public void alterTable(
                            Identifier identifier,
                            List<SchemaChange> changes,
                            boolean ignoreIfNotExists)
                            throws TableNotExistException, ColumnAlreadyExistException,
                                    ColumnNotExistException {
                        if (alterCount < competingChanges.length) {
                            competingWriter.alterTable(
                                    identifier, competingChanges[alterCount++], false);
                        }
                        super.alterTable(identifier, changes, ignoreIfNotExists);
                    }
                },
                new Options());
    }

    private FileStoreTable recreateTable(DataType valueType) throws Exception {
        catalog.dropTable(IDENTIFIER, false);
        catalog.createTable(
                IDENTIFIER,
                baseSchema(valueType).option("bucket", "1").option("bucket-key", "id").build(),
                false);
        return table();
    }

    private void assertUnchanged(FileStoreTable original) throws Catalog.TableNotExistException {
        assertThat(table().schema()).isEqualTo(original.schema());
    }

    private FileStoreTable table() throws Catalog.TableNotExistException {
        return (FileStoreTable) catalog.getTable(IDENTIFIER);
    }

    private static Schema.Builder baseSchema() {
        return baseSchema(DataTypes.STRING());
    }

    private static Schema.Builder baseSchema(DataType valueType) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("value", valueType)
                .primaryKey("id");
    }
}
