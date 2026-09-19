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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.NestedFieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests nested-field identity in query authorization across schema changes. */
class NestedFieldQueryAuthTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testHistoricalReadRejectsReAddedNestedFieldOfSameName() throws Exception {
        Path path = new Path(tempDir.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FileSystemSchemaManager schemaManager = new FileSystemSchemaManager(fileIO, path);
        RowType initialType =
                RowType.of(
                        new DataField(0, "pk", DataTypes.INT()),
                        new DataField(
                                1,
                                "info",
                                RowType.of(
                                        new DataField(2, "secret", DataTypes.STRING()),
                                        new DataField(3, "region", DataTypes.STRING()))));
        schemaManager.createTable(
                new Schema(
                        initialType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap("query-auth.enabled", "true"),
                        ""));
        TableSchema historicalSchema = schemaManager.latest().get();
        writeInitialRow(FileStoreTableFactory.create(fileIO, path, historicalSchema));

        schemaManager.commitChanges(SchemaChange.dropColumn(new String[] {"info", "secret"}));
        schemaManager.commitChanges(
                SchemaChange.addColumn(
                        new String[] {"info", "secret"}, DataTypes.STRING(), null, null));
        TableSchema latestSchema = schemaManager.latest().get();
        TableQueryAuthResult authResult = authResult(latestSchema.logicalRowType());
        CatalogEnvironment environment = authEnvironment(authResult);

        assertThat(readKeys(FileStoreTableFactory.create(fileIO, path, latestSchema, environment)))
                .isEmpty();

        FileStoreTable historicalTable =
                FileStoreTableFactory.create(fileIO, path, historicalSchema, environment);
        assertThatThrownBy(() -> historicalTable.newReadBuilder().newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("changed identity")
                .hasMessageContaining("2")
                .hasMessageContaining("4");
    }

    private void writeInitialRow(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            GenericRow.of(
                                    BinaryString.fromString("x"), BinaryString.fromString("US"))));
            commit.commit(write.prepareCommit());
        }
    }

    private TableQueryAuthResult authResult(RowType rowType) {
        RowType info = (RowType) rowType.getField("info").type();
        Predicate filter =
                new PredicateBuilder(rowType)
                        .equal(
                                new NestedFieldTransform(
                                        new FieldRef(1, "info", info),
                                        Collections.singletonList("secret")),
                                BinaryString.fromString("x"));
        return new TableQueryAuthResult(
                Collections.singletonList(JsonSerdeUtil.toFlatJson(filter)), null);
    }

    private CatalogEnvironment authEnvironment(TableQueryAuthResult authResult) throws Exception {
        Catalog catalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.authTableQuery(Mockito.any(), Mockito.any())).thenReturn(authResult);
        Mockito.when(catalog.loadSnapshot(Mockito.any(Identifier.class)))
                .thenThrow(new UnsupportedOperationException());
        return new CatalogEnvironment(
                Identifier.create("default", "t"),
                null,
                () -> catalog,
                null,
                null,
                null,
                false,
                false);
    }

    private List<Integer> readKeys(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<Integer> keys = new ArrayList<>();
        try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> keys.add(row.getInt(0)));
        }
        return keys;
    }
}
