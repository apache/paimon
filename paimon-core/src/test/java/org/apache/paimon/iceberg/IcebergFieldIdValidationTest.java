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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that unpublishable collection IDs fail before making metadata visible. */
class IcebergFieldIdValidationTest {
    @TempDir java.nio.file.Path tempDir;

    private static final Identifier IDENTIFIER = Identifier.create("db", "t");

    @ParameterizedTest
    @ValueSource(strings = {"table-location", "hadoop-catalog", "hive-catalog", "rest-catalog"})
    void testCreateRejectsCollidingIds(String storage) throws Exception {
        try (FileSystemCatalog catalog = catalog()) {
            assertThatThrownBy(
                            () ->
                                    catalog.createTable(
                                            IDENTIFIER, schema(storage, collidingType()), false))
                    .hasStackTraceContaining("duplicate field ID");
            assertThat(catalog.listTables("db")).doesNotContain("t");
        }
    }

    @Test
    void testAlterRejectsCollidingIds() throws Exception {
        try (FileSystemCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, schema("table-location", DataTypes.INT()), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(IDENTIFIER);
            long schemaId = table.schema().id();
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            IDENTIFIER,
                                            SchemaChange.addColumn("bad", collidingType()),
                                            false))
                    .hasStackTraceContaining("duplicate field ID");
            assertThat(table.schemaManager().latest().get().id()).isEqualTo(schemaId);
        }
    }

    @Test
    void testEnableChecksHistoricalSchemas() throws Exception {
        try (FileSystemCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, schema("disabled", collidingType()), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(IDENTIFIER);
            write(table, 1);
            assertPaimonRow(table);
            catalog.alterTable(IDENTIFIER, SchemaChange.dropColumn("m"), false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(IDENTIFIER);
            long schemaId = latest.schema().id();
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            IDENTIFIER,
                                            SchemaChange.setOption(
                                                    "metadata.iceberg.storage", "table-location"),
                                            false))
                    .hasStackTraceContaining("duplicate field ID");
            assertThat(latest.schemaManager().latest().get().id()).isEqualTo(schemaId);
            assertThatThrownBy(
                            () ->
                                    latest.copy(
                                            Collections.singletonMap(
                                                    "metadata.iceberg.storage", "table-location")))
                    .hasStackTraceContaining("duplicate field ID");
        }
    }

    @Test
    void testLegacyTableCanBeReadAndDisabledButNotPublished() throws Exception {
        try (FileSystemCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, schema("disabled", collidingType()), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(IDENTIFIER);
            write(table, 1);
            TableSchema old = table.schema();
            Map<String, String> options = new HashMap<>(old.options());
            options.put("metadata.iceberg.storage", "table-location");
            // Simulate an ALTER accepted by a runtime before field-ID validation existed.
            TableSchema legacy =
                    new TableSchema(
                            old.id() + 1,
                            old.fields(),
                            old.highestFieldId(),
                            old.partitionKeys(),
                            old.primaryKeys(),
                            options,
                            old.comment());
            table.fileIO()
                    .writeFile(
                            new Path(table.location(), "schema/schema-" + legacy.id()),
                            legacy.toString(),
                            true);
            FileStoreTable loaded = (FileStoreTable) catalog.getTable(IDENTIFIER);
            assertPaimonRow(loaded);
            assertThatThrownBy(() -> write(loaded, 2))
                    .hasStackTraceContaining("duplicate field ID");
            assertThat(loaded.snapshotManager().latestSnapshotId()).isEqualTo(1L);
            try (IcebergCommitCallback callback = new IcebergCommitCallback(loaded, "writer")) {
                assertThatThrownBy(() -> callback.retry(new ManifestCommittable(1)))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("duplicate field ID");
            }
            assertThat(
                            loaded.fileIO()
                                    .exists(
                                            new Path(
                                                    loaded.location(),
                                                    "metadata/v1.metadata.json")))
                    .isFalse();
            catalog.alterTable(
                    IDENTIFIER,
                    SchemaChange.setOption("metadata.iceberg.storage", "disabled"),
                    false);
            FileStoreTable disabled = (FileStoreTable) catalog.getTable(IDENTIFIER);
            assertPaimonRow(disabled);
            write(disabled, 2);
            assertThat(disabled.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        }
    }

    @Test
    void testUniqueCollectionIdsRemainAllowed() throws Exception {
        List<DataType> types = new ArrayList<>();
        types.add(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())));
        types.add(DataTypes.MAP(DataTypes.INT().notNull(), DataTypes.ARRAY(DataTypes.INT())));
        types.add(DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()).notNull(), DataTypes.INT()));
        try (FileSystemCatalog catalog = catalog()) {
            for (int i = 0; i < types.size(); i++) {
                Identifier identifier = Identifier.create("db", "t" + i);
                catalog.createTable(identifier, schema("table-location", types.get(i)), false);
                assertThat(catalog.listTables("db")).contains(identifier.getObjectName());
            }
        }
    }

    private FileSystemCatalog catalog() throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("db", true);
        return catalog;
    }

    private static Schema schema(String storage, DataType type) {
        return Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("m", type)
                .option("file.format", "avro")
                .option("metadata.iceberg.storage", storage)
                .build();
    }

    private static DataType collidingType() {
        return DataTypes.MAP(
                DataTypes.ARRAY(DataTypes.INT()).notNull(), DataTypes.ARRAY(DataTypes.INT()));
    }

    private static void write(FileStoreTable table, long commitId) throws Exception {
        Map<GenericArray, GenericArray> values = new HashMap<>();
        values.put(new GenericArray(new int[] {1, 2}), new GenericArray(new int[] {3, 4}));
        try (TableWriteImpl<?> write = table.newWrite("writer");
                TableCommitImpl commit = table.newCommit("writer")) {
            write.write(GenericRow.of(42, new GenericMap(values)));
            commit.commit(commitId, write.prepareCommit(false, commitId));
        }
    }

    private static void assertPaimonRow(FileStoreTable table) throws Exception {
        ReadBuilder read = table.newReadBuilder();
        List<Integer> keys = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        keys.add(row.getInt(0));
                        assertThat(row.getMap(1).keyArray().getArray(0).getInt(0)).isEqualTo(1);
                        assertThat(row.getMap(1).valueArray().getArray(0).getInt(1)).isEqualTo(4);
                    });
        }
        assertThat(keys).containsExactly(42);
    }
}
