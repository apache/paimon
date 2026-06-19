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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FileFormatProvider;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    private static final AtomicInteger RUNTIME_PROVIDER_VALIDATIONS = new AtomicInteger();

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
    public void testCreateTableWithRuntimeCatalogOptions() throws Exception {
        RUNTIME_PROVIDER_VALIDATIONS.set(0);
        Options options = new Options();
        options.set(
                Catalog.TABLE_RUNTIME_OPTION_PREFIX + FileFormatProvider.VALIDATION_FORMAT_PROVIDER,
                RuntimeOptionFileFormatProvider.IDENTIFIER);
        String runtimePath = new Path(new Path(warehouse), "runtime-path-ignored").toString();
        options.set(Catalog.TABLE_RUNTIME_OPTION_PREFIX + CoreOptions.PATH.key(), runtimePath);
        catalog =
                new FileSystemCatalog(fileIO, new Path(warehouse), CatalogContext.create(options));

        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "new_table");
        String schemaPath = new Path(new Path(warehouse), "test_db.db/new_table").toString();
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("pk")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_AVRO)
                        .build();
        catalog.createTable(identifier, schema, false);

        assertThat(RUNTIME_PROVIDER_VALIDATIONS.get()).isGreaterThan(0);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        assertThat(table.options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_AVRO)
                .containsEntry(CoreOptions.PATH.key(), schemaPath)
                .containsEntry(
                        FileFormatProvider.VALIDATION_FORMAT_PROVIDER,
                        RuntimeOptionFileFormatProvider.IDENTIFIER);
        assertThat(table.location().toString()).isEqualTo(schemaPath);
        assertThat(
                        new SchemaManager(
                                        fileIO, new Path(new Path(warehouse), "test_db.db/new_table"))
                                .latestOrThrow("Table schema should exist")
                                .options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_AVRO)
                .doesNotContainKey(CoreOptions.PATH.key())
                .doesNotContainKey(FileFormatProvider.VALIDATION_FORMAT_PROVIDER);

        RUNTIME_PROVIDER_VALIDATIONS.set(0);
        catalog.alterTable(
                identifier, SchemaChange.addColumn("new_value", DataTypes.STRING()), false);

        assertThat(RUNTIME_PROVIDER_VALIDATIONS.get()).isGreaterThan(0);
        assertThat(
                        new SchemaManager(
                                        fileIO, new Path(new Path(warehouse), "test_db.db/new_table"))
                                .latestOrThrow("Table schema should exist")
                                .options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_AVRO)
                .doesNotContainKey(CoreOptions.PATH.key())
                .doesNotContainKey(FileFormatProvider.VALIDATION_FORMAT_PROVIDER);
    }

    @Test
    public void testLoadFormatTableWithRuntimeCatalogOptions() throws Exception {
        Options options = new Options();
        options.set(
                Catalog.TABLE_RUNTIME_OPTION_PREFIX + FileFormatProvider.READ_FORMAT_PROVIDER,
                RuntimeOptionFileFormatProvider.IDENTIFIER);
        String schemaPath = new Path(new Path(warehouse), "format-table-data").toString();
        String runtimePath = new Path(new Path(warehouse), "runtime-path-ignored").toString();
        options.set(Catalog.TABLE_RUNTIME_OPTION_PREFIX + CoreOptions.PATH.key(), runtimePath);
        catalog =
                new FileSystemCatalog(fileIO, new Path(warehouse), CatalogContext.create(options));

        Identifier identifier = Identifier.create("test_db", "format_table");
        Schema schema =
                Schema.newBuilder()
                        .column("value", DataTypes.STRING())
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                        .option(CoreOptions.PATH.key(), schemaPath)
                        .build();

        Catalog loadCatalog = Mockito.mock(Catalog.class);
        when(loadCatalog.supportsVersionManagement()).thenReturn(false);
        when(loadCatalog.supportsPartitionModification()).thenReturn(false);
        FormatTable table =
                (FormatTable)
                        CatalogUtils.loadTable(
                                loadCatalog,
                                identifier,
                                path -> fileIO,
                                path -> fileIO,
                                ignored ->
                                        new TableMetadata(
                                                TableSchema.create(0, schema), false, null),
                                null,
                                null,
                                CatalogContext.create(options),
                                CatalogUtils.tableRuntimeOptions(options.toMap()),
                                false);

        assertThat(table.options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                .containsEntry(CoreOptions.PATH.key(), schemaPath)
                .containsEntry(
                        FileFormatProvider.READ_FORMAT_PROVIDER,
                        RuntimeOptionFileFormatProvider.IDENTIFIER);
        assertThat(table.location()).isEqualTo(schemaPath);
        assertThat(TableSchema.create(0, schema).options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                .containsEntry(CoreOptions.PATH.key(), schemaPath)
                .doesNotContainKey(FileFormatProvider.READ_FORMAT_PROVIDER);
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

    /** Test provider selected through catalog runtime options. */
    public static class RuntimeOptionFileFormatProvider implements FileFormatProvider {

        static final String IDENTIFIER = "catalog-runtime-provider";

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public Optional<FileFormat> create(String identifier, FormatContext context) {
            if (CoreOptions.FILE_FORMAT_AVRO.equals(identifier)) {
                return Optional.of(new RuntimeOptionFileFormat(identifier));
            }
            return Optional.empty();
        }
    }

    private static class RuntimeOptionFileFormat extends FileFormat {

        private RuntimeOptionFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }

        @Override
        public FormatReaderFactory createReaderFactory(
                RowType dataSchemaRowType,
                RowType projectedRowType,
                @Nullable List<Predicate> filters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatWriterFactory createWriterFactory(RowType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateDataFields(RowType rowType) {
            RUNTIME_PROVIDER_VALIDATIONS.incrementAndGet();
        }

        @Override
        public Optional<SimpleStatsExtractor> createStatsExtractor(
                RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
            return Optional.empty();
        }
    }
}
