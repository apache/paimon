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

package org.apache.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;

/** Mixed language overwrite test for Java and Python interoperability. */
public class JavaPyE2ETest {

    java.nio.file.Path tempDir = Paths.get("../paimon-python/pypaimon/tests/e2e").toAbsolutePath();

    // Fields from TableTestBase that we need
    protected final String commitUser = UUID.randomUUID().toString();
    protected Path warehouse;
    protected Catalog catalog;
    protected String database;

    @BeforeEach
    public void before() throws Exception {
        database = "default";

        // Create warehouse directory if it doesn't exist
        if (!Files.exists(tempDir.resolve("warehouse"))) {
            Files.createDirectories(tempDir.resolve("warehouse"));
        }

        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.resolve("warehouse"));
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));

        // Create database if it doesn't exist
        try {
            catalog.createDatabase(database, false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // Database already exists, ignore
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteRead() throws Exception {
        Identifier identifier = identifier("mixed_test_tablej");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("category", DataTypes.STRING())
                        .column("value", DataTypes.DOUBLE())
                        .partitionKeys("category")
                        .option("dynamic-partition-overwrite", "false")
                        .build();

        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        try (StreamTableWrite write = fileStoreTable.newWrite(commitUser);
                InnerTableCommit commit = fileStoreTable.newCommit(commitUser)) {

            write.write(createRow(1, "Apple", "Fruit", 1.5));
            write.write(createRow(2, "Banana", "Fruit", 0.8));
            write.write(createRow(3, "Carrot", "Vegetable", 0.6));
            write.write(createRow(4, "Broccoli", "Vegetable", 1.2));
            write.write(createRow(5, "Chicken", "Meat", 5.0));
            write.write(createRow(6, "Beef", "Meat", 8.0));

            commit.commit(0, write.prepareCommit(true, 0));
        }

        List<Split> splits =
                new ArrayList<>(fileStoreTable.newSnapshotReader().read().dataSplits());
        TableRead read = fileStoreTable.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, Apple, Fruit, 1.5",
                        "2, Banana, Fruit, 0.8",
                        "3, Carrot, Vegetable, 0.6",
                        "4, Broccoli, Vegetable, 1.2",
                        "5, Chicken, Meat, 5.0",
                        "6, Beef, Meat, 8.0");
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testRead() throws Exception {
        Identifier identifier = identifier("mixed_test_tablep");
        Table table = catalog.getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<Split> splits =
                new ArrayList<>(fileStoreTable.newSnapshotReader().read().dataSplits());
        TableRead read = fileStoreTable.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        System.out.println(res);
    }

    // Helper method from TableTestBase
    protected Identifier identifier(String tableName) {
        return new Identifier(database, tableName);
    }

    private static InternalRow createRow(int id, String name, String category, double value) {
        return GenericRow.of(
                id, BinaryString.fromString(name), BinaryString.fromString(category), value);
    }
}
