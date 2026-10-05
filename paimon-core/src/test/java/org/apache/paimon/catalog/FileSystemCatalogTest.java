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
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Test
    public void testLookupTableWithFullCompactionDeltaCommitsStillLoads() throws Exception {
        String database = "lookup_delta_commits_db";
        catalog.createDatabase(database, false);
        Identifier identifier = Identifier.create(database, "t");
        Identifier dropped = Identifier.create(database, "dropped");
        String deltaCommits = CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key();
        // Tables created before the combination was rejected carry both options.
        for (Identifier legacy : new Identifier[] {identifier, dropped}) {
            createLookupTableWithDeltaCommits(legacy);
        }

        Table table = catalog.getTable(identifier);
        assertThat(table.options()).containsEntry(deltaCommits, "1000");
        // Flink restates the stored options as dynamic options.
        ((FileStoreTable) table).copy(table.options());
        writeRow(table, 2);
        assertThat(readKeys(catalog.getTable(identifier))).containsExactlyInAnyOrder(1, 2);

        FileStoreTable loaded = (FileStoreTable) catalog.getTable(identifier);
        // Altering the table still rejects the combination, until the option is removed.
        SchemaChange unrelated = SchemaChange.setOption("snapshot.time-retained", "2h");
        assertThatThrownBy(() -> catalog.alterTable(identifier, unrelated, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(deltaCommits);
        catalog.alterTable(identifier, SchemaChange.removeOption(deltaCommits), false);
        assertThat(catalog.getTable(identifier).options()).doesNotContainKey(deltaCommits);
        // A table loaded before the option was removed can still refresh its schema.
        loaded.copyWithLatestSchema();
        catalog.alterTable(identifier, unrelated, false);

        catalog.dropTable(dropped, false);
        assertThat(catalog.listTables(database)).containsExactly("t");

        // Setting up the combination with a dynamic option is still rejected.
        FileStoreTable lookupTable = (FileStoreTable) catalog.getTable(identifier);
        assertThatThrownBy(() -> lookupTable.copy(Collections.singletonMap(deltaCommits, "1000")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(deltaCommits);
    }

    private void createLookupTableWithDeltaCommits(Identifier identifier) throws Exception {
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup")
                        .build(),
                false);
        writeRow(catalog.getTable(identifier), 1);

        // Write the schema file directly, as the combination can no longer be created.
        Path tablePath =
                ((FileSystemCatalog) DelegateCatalog.rootCatalog(catalog))
                        .getTableLocation(identifier);
        TableSchema latest = new FileSystemSchemaManager(fileIO, tablePath).latest().get();
        Map<String, String> options = new HashMap<>(latest.options());
        options.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1000");
        TableSchema legacy =
                new TableSchema(
                        latest.id() + 1,
                        latest.fields(),
                        latest.highestFieldId(),
                        latest.partitionKeys(),
                        latest.primaryKeys(),
                        options,
                        latest.comment());
        fileIO.writeFile(
                new Path(tablePath, "schema/schema-" + legacy.id()), legacy.toString(), false);
    }

    private void writeRow(Table table, int key) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(IOManager.create(tempFile.toString()));
            write.write(GenericRow.of(key, key));
            commit.commit(write.prepareCommit());
        }
    }

    private static List<Integer> readKeys(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> keys = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> keys.add(row.getInt(0)));
        return keys;
    }
}
