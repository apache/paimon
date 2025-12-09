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
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.TARGET_FILE_SIZE;
import static org.apache.paimon.data.DataFormatTestUtil.internalRowToString;
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
    public void testJavaWriteReadAppendTable() throws Exception {
        Identifier identifier = identifier("mixed_test_append_tablej");
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

            write.write(createRow4Cols(1, "Apple", "Fruit", 1.5));
            write.write(createRow4Cols(2, "Banana", "Fruit", 0.8));
            write.write(createRow4Cols(3, "Carrot", "Vegetable", 0.6));
            write.write(createRow4Cols(4, "Broccoli", "Vegetable", 1.2));
            write.write(createRow4Cols(5, "Chicken", "Meat", 5.0));
            write.write(createRow4Cols(6, "Beef", "Meat", 8.0));

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
    public void testReadAppendTable() throws Exception {
        Identifier identifier = identifier("mixed_test_append_tablep");
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

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteReadPkTable() throws Exception {
        Identifier identifier = identifier("mixed_test_pk_tablej");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("category", DataTypes.STRING())
                        .column("value", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .partitionKeys("category")
                        .option("dynamic-partition-overwrite", "false")
                        .option("bucket", "2")
                        .build();

        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        try (StreamTableWrite write = fileStoreTable.newWrite(commitUser);
                InnerTableCommit commit = fileStoreTable.newCommit(commitUser)) {

            write.write(createRow4Cols(1, "Apple", "Fruit", 1.5));
            write.write(createRow4Cols(2, "Banana", "Fruit", 0.8));
            write.write(createRow4Cols(3, "Carrot", "Vegetable", 0.6));
            write.write(createRow4Cols(4, "Broccoli", "Vegetable", 1.2));
            write.write(createRow4Cols(5, "Chicken", "Meat", 5.0));
            write.write(createRow4Cols(6, "Beef", "Meat", 8.0));

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
    public void testPKDeletionVectorWrite() throws Exception {
        Consumer<Options> optionsSetter =
                options -> {
                    // let level has many files
                    options.set(TARGET_FILE_SIZE, new MemorySize(1));
                    options.set(DELETION_VECTORS_ENABLED, true);
                };
        String tableName = "test_pk_dv";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        FileStoreTable table = createFileStoreTable(optionsSetter, tablePath);
        StreamTableWrite write = table.newWrite(commitUser);
        IOManager ioManager = IOManager.create(tablePath.toString());
        write.withIOManager(ioManager);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(createRow3Cols(1, 10, 100L));
        write.write(createRow3Cols(2, 20, 200L));
        write.write(createRow3Cols(1, 11, 101L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(createRow3Cols(1, 10, 1000L));
        write.write(createRow3Cols(2, 21, 201L));
        write.write(createRow3Cols(2, 21, 2001L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(createRow3Cols(1, 11, 1001L));
        write.write(createRow3Cols(2, 21, 20001L));
        write.write(createRow3Cols(2, 22, 202L));
        write.write(createRow3ColsWithKind(RowKind.DELETE, 1, 11, 1001L));
        commit.commit(2, write.prepareCommit(true, 2));
        write.write(createRow3ColsWithKind(RowKind.DELETE, 2, 20, 200L));
        commit.commit(2, write.prepareCommit(true, 2));

        // test result
        Function<InternalRow, String> rowDataToString =
                row ->
                        internalRowToString(
                                row,
                                DataTypes.ROW(
                                        DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()));
        List<String> result =
                getResult(table.newRead(), table.newScan().plan().splits(), rowDataToString);
        assertThat(result)
                .containsExactlyInAnyOrder("+I[1, 10, 1000]", "+I[2, 21, 20001]", "+I[2, 22, 202]");
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testPKDeletionVectorWriteMultiBatch() throws Exception {
        Consumer<Options> optionsSetter =
                options -> {
                    // let level has many files
                    options.set(TARGET_FILE_SIZE, new MemorySize(128 * 1024));
                    options.set(DELETION_VECTORS_ENABLED, true);
                };
        String tableName = "test_pk_dv_multi_batch";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        FileStoreTable table = createFileStoreTable(optionsSetter, tablePath);
        StreamTableWrite write = table.newWrite(commitUser);
        IOManager ioManager = IOManager.create(tablePath.toString());
        write.withIOManager(ioManager);
        StreamTableCommit commit = table.newCommit(commitUser);

        // Write 10000 records
        for (int i = 1; i <= 10000; i++) {
            write.write(createRow3Cols(1, i * 10, (long) i * 100));
        }
        commit.commit(0, write.prepareCommit(false, 0));

        // Delete the 81930th record
        write.write(createRow3ColsWithKind(RowKind.DELETE, 1, 81930, 819300L));
        commit.commit(1, write.prepareCommit(true, 1));

        Function<InternalRow, String> rowDataToString =
                row ->
                        internalRowToString(
                                row,
                                DataTypes.ROW(
                                        DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()));
        List<String> result =
                getResult(table.newRead(), table.newScan().plan().splits(), rowDataToString);

        // Verify the count is 9999
        assertThat(result).hasSize(9999);

        assertThat(result).doesNotContain("+I[1, 81930, 819300]");

        assertThat(result).contains("+I[1, 10, 100]");
        assertThat(result).contains("+I[1, 100000, 1000000]");
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testPKDeletionVectorWriteMultiBatchRawConvertable() throws Exception {
        Consumer<Options> optionsSetter =
                options -> {
                    options.set(DELETION_VECTORS_ENABLED, true);
                };
        String tableName = "test_pk_dv_raw_convertable";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        FileStoreTable table = createFileStoreTable(optionsSetter, tablePath);
        StreamTableWrite write = table.newWrite(commitUser);
        IOManager ioManager = IOManager.create(tablePath.toString());
        write.withIOManager(ioManager);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (int i = 1; i <= 10000; i++) {
            write.write(createRow3Cols(1, i * 10, (long) i * 100));
        }
        commit.commit(0, write.prepareCommit(false, 0));

        write.write(createRow3ColsWithKind(RowKind.DELETE, 1, 81930, 819300L));
        commit.commit(1, write.prepareCommit(true, 1));

        Function<InternalRow, String> rowDataToString =
                row ->
                        internalRowToString(
                                row,
                                DataTypes.ROW(
                                        DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()));
        List<String> result =
                getResult(table.newRead(), table.newScan().plan().splits(), rowDataToString);

        assertThat(result).hasSize(9999);

        assertThat(result).doesNotContain("+I[1, 81930, 819300]");

        // Verify some sample records exist
        assertThat(result).contains("+I[1, 10, 100]");
        assertThat(result).contains("+I[1, 100000, 1000000]");
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testReadPkTable() throws Exception {
        Identifier identifier = identifier("mixed_test_pk_tablep_parquet");
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
        System.out.println("Result: " + res);
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, Apple, Fruit, 1.5",
                        "2, Banana, Fruit, 0.8",
                        "3, Carrot, Vegetable, 0.6",
                        "4, Broccoli, Vegetable, 1.2",
                        "5, Chicken, Meat, 5.0",
                        "6, Beef, Meat, 8.0");
    }

    // Helper method from TableTestBase
    protected Identifier identifier(String tableName) {
        return new Identifier(database, tableName);
    }

    protected FileStoreTable createFileStoreTable(Consumer<Options> configure, Path tablePath)
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "a", "b"});
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(BUCKET, 1);
        configure.accept(options);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                options.toMap(),
                                ""));
        return new PrimaryKeyFileStoreTable(
                FileIOFinder.find(tablePath), tablePath, tableSchema, CatalogEnvironment.empty());
    }

    private static InternalRow createRow4Cols(int id, String name, String category, double value) {
        return GenericRow.of(
                id, BinaryString.fromString(name), BinaryString.fromString(category), value);
    }

    protected GenericRow createRow3Cols(Object... values) {
        return GenericRow.of(values[0], values[1], values[2]);
    }

    protected GenericRow createRow3ColsWithKind(RowKind rowKind, Object... values) {
        return GenericRow.ofKind(rowKind, values[0], values[1], values[2]);
    }
}
