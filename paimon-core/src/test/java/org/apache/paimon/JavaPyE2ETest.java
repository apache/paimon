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
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ENABLED;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.CoreOptions.TARGET_FILE_SIZE;
import static org.apache.paimon.data.DataFormatTestUtil.internalRowToString;
import static org.apache.paimon.globalindex.btree.BTreeIndexOptions.BTREE_INDEX_COMPRESSION;
import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;

/** Mixed language overwrite test for Java and Python interoperability. */
public class JavaPyE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(JavaPyE2ETest.class);

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
    public void testReadAppendTable() throws Exception {
        for (String format : Arrays.asList("parquet", "orc", "avro")) {
            Identifier identifier = identifier("mixed_test_append_tablep_" + format);
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
            LOG.info("Read append table: {} row(s)", res.size());
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteReadPkTable() throws Exception {
        for (String format : Arrays.asList("parquet", "orc", "avro")) {
            Identifier identifier = identifier("mixed_test_pk_tablej_" + format);
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("category", DataTypes.STRING())
                            .column("value", DataTypes.DOUBLE())
                            .column("ts", DataTypes.TIMESTAMP())
                            .column("ts_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                            .column(
                                    "metadata",
                                    DataTypes.ROW(
                                            DataTypes.FIELD(0, "source", DataTypes.STRING()),
                                            DataTypes.FIELD(1, "created_at", DataTypes.BIGINT()),
                                            DataTypes.FIELD(
                                                    2,
                                                    "location",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    0, "city", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    1,
                                                                    "country",
                                                                    DataTypes.STRING())))))
                            .primaryKey("id")
                            .partitionKeys("category")
                            .option("dynamic-partition-overwrite", "false")
                            .option("bucket", "2")
                            .option("file.format", format)
                            .build();

            catalog.createTable(identifier, schema, true);
            Table table = catalog.getTable(identifier);
            FileStoreTable fileStoreTable = (FileStoreTable) table;

            try (StreamTableWrite write = fileStoreTable.newWrite(commitUser);
                    InnerTableCommit commit = fileStoreTable.newCommit(commitUser)) {

                write.write(
                        createRow7Cols(
                                1, "Apple", "Fruit", 1.5, 1000000L, 2000000L, "store1", 1001L,
                                "Beijing", "China"));
                write.write(
                        createRow7Cols(
                                2,
                                "Banana",
                                "Fruit",
                                0.8,
                                1000001L,
                                2000001L,
                                "store1",
                                1002L,
                                "Shanghai",
                                "China"));
                write.write(
                        createRow7Cols(
                                3,
                                "Carrot",
                                "Vegetable",
                                0.6,
                                1000002L,
                                2000002L,
                                "store2",
                                1003L,
                                "Tokyo",
                                "Japan"));
                write.write(
                        createRow7Cols(
                                4,
                                "Broccoli",
                                "Vegetable",
                                1.2,
                                1000003L,
                                2000003L,
                                "store2",
                                1004L,
                                "Seoul",
                                "Korea"));
                write.write(
                        createRow7Cols(
                                5, "Chicken", "Meat", 5.0, 1000004L, 2000004L, "store3", 1005L,
                                "NewYork", "USA"));
                write.write(
                        createRow7Cols(
                                6, "Beef", "Meat", 8.0, 1000005L, 2000005L, "store3", 1006L,
                                "London", "UK"));

                commit.commit(0, write.prepareCommit(true, 0));
            }

            List<Split> splits =
                    new ArrayList<>(fileStoreTable.newSnapshotReader().read().dataSplits());
            TableRead read = fileStoreTable.newRead();
            List<String> res =
                    getResult(read, splits, row -> rowToStringWithStruct(row, table.rowType()));
            assertThat(res)
                    .containsExactlyInAnyOrder(
                            "+I[1, Apple, Fruit, 1.5, 1970-01-01T00:16:40, 1970-01-01T00:33:20, (store1, 1001, (Beijing, China))]",
                            "+I[2, Banana, Fruit, 0.8, 1970-01-01T00:16:40.001, 1970-01-01T00:33:20.001, (store1, 1002, (Shanghai, China))]",
                            "+I[3, Carrot, Vegetable, 0.6, 1970-01-01T00:16:40.002, 1970-01-01T00:33:20.002, (store2, 1003, (Tokyo, Japan))]",
                            "+I[4, Broccoli, Vegetable, 1.2, 1970-01-01T00:16:40.003, 1970-01-01T00:33:20.003, (store2, 1004, (Seoul, Korea))]",
                            "+I[5, Chicken, Meat, 5.0, 1970-01-01T00:16:40.004, 1970-01-01T00:33:20.004, (store3, 1005, (NewYork, USA))]",
                            "+I[6, Beef, Meat, 8.0, 1970-01-01T00:16:40.005, 1970-01-01T00:33:20.005, (store3, 1006, (London, UK))]");
        }
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
        for (String format : Arrays.asList("parquet", "orc", "avro")) {
            Identifier identifier = identifier("mixed_test_pk_tablep_" + format);
            Table table = catalog.getTable(identifier);
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            List<Split> splits =
                    new ArrayList<>(fileStoreTable.newSnapshotReader().read().dataSplits());
            TableRead read = fileStoreTable.newRead();
            List<String> res =
                    getResult(read, splits, row -> rowToStringWithStruct(row, table.rowType()));
            LOG.info("Result for {}: {} row(s)", format, res.size());
            assertThat(table.rowType().getFieldTypes().get(4)).isEqualTo(DataTypes.TIMESTAMP());
            assertThat(table.rowType().getFieldTypes().get(5))
                    .isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
            assertThat(table.rowType().getFieldTypes().get(6)).isInstanceOf(RowType.class);
            RowType metadataType = (RowType) table.rowType().getFieldTypes().get(6);
            assertThat(metadataType.getFieldTypes().get(2)).isInstanceOf(RowType.class);
            assertThat(res)
                    .containsExactlyInAnyOrder(
                            "+I[1, Apple, Fruit, 1.5, 1970-01-01T00:16:40, 1970-01-01T00:33:20, (store1, 1001, (Beijing, China))]",
                            "+I[2, Banana, Fruit, 0.8, 1970-01-01T00:16:40.001, 1970-01-01T00:33:20.001, (store1, 1002, (Shanghai, China))]",
                            "+I[3, Carrot, Vegetable, 0.6, 1970-01-01T00:16:40.002, 1970-01-01T00:33:20.002, (store2, 1003, (Tokyo, Japan))]",
                            "+I[4, Broccoli, Vegetable, 1.2, 1970-01-01T00:16:40.003, 1970-01-01T00:33:20.003, (store2, 1004, (Seoul, Korea))]",
                            "+I[5, Chicken, Meat, 5.0, 1970-01-01T00:16:40.004, 1970-01-01T00:33:20.004, (store3, 1005, (NewYork, USA))]",
                            "+I[6, Beef, Meat, 8.0, 1970-01-01T00:16:40.005, 1970-01-01T00:33:20.005, (store3, 1006, (London, UK))]");

            PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
            int[] ids = {1, 2, 3, 4, 5, 6};
            String[] names = {"Apple", "Banana", "Carrot", "Broccoli", "Chicken", "Beef"};
            for (int i = 0; i < ids.length; i++) {
                int id = ids[i];
                String expectedName = names[i];
                Predicate predicate = predicateBuilder.equal(0, id);
                ReadBuilder readBuilder = fileStoreTable.newReadBuilder().withFilter(predicate);
                List<String> byKey =
                        getResult(
                                readBuilder.newRead(),
                                readBuilder.newScan().plan().splits(),
                                row -> rowToStringWithStruct(row, table.rowType()));
                List<String> matching =
                        byKey.stream()
                                .filter(s -> s.trim().startsWith("+I[" + id + ", "))
                                .collect(Collectors.toList());
                assertThat(matching)
                        .as(
                                "Python wrote row id=%d; Java read with predicate id=%d should return it.",
                                id, id)
                        .hasSize(1);
                assertThat(matching.get(0)).contains(String.valueOf(id)).contains(expectedName);
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testBtreeIndexWrite() throws Exception {
        testBtreeIndexWriteString();
        testBtreeIndexWriteInt();
        testBtreeIndexWriteBigInt();
        testBtreeIndexWriteLarge();
        testBtreeIndexWriteNull();
    }

    private void testBtreeIndexWriteString() throws Exception {
        testBtreeIndexWriteGeneric(
                DataTypes.STRING(),
                "test_btree_index_string",
                BinaryString.fromString("k1"),
                BinaryString.fromString("k2"),
                BinaryString.fromString("k3"));
    }

    private void testBtreeIndexWriteInt() throws Exception {
        testBtreeIndexWriteGeneric(DataTypes.INT(), "test_btree_index_int", 100, 200, 300);
    }

    private void testBtreeIndexWriteBigInt() throws Exception {
        testBtreeIndexWriteGeneric(
                DataTypes.BIGINT(), "test_btree_index_bigint", 1000L, 2000L, 3000L);
    }

    private void testBtreeIndexWriteGeneric(
            DataType keyType, String tableName, Object key1, Object key2, Object key3)
            throws Exception {
        // create table
        RowType rowType =
                RowType.of(new DataType[] {keyType, DataTypes.STRING()}, new String[] {"k", "v"});
        Options options = new Options();
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // write data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(key1, BinaryString.fromString("v1")));
            write.write(GenericRow.of(key2, BinaryString.fromString("v2")));
            write.write(GenericRow.of(key3, BinaryString.fromString("v3")));
            commit.commit(write.prepareCommit());
        }

        // build index
        BTreeGlobalIndexBuilder builder =
                new BTreeGlobalIndexBuilder(table).withIndexType("btree").withIndexField("k");
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(builder.build(builder.scan(), IOManager.create(warehouse.toString())));
        }

        // assert index
        List<IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest);
        assertThat(indexEntries)
                .singleElement()
                .matches(entry -> entry.indexFile().rowCount() == 3);

        // read index
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(predicateBuilder.equal(0, key2));
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(r -> result.add(r.getString(1).toString()));
        assertThat(result).containsOnly("v2");
    }

    private void testBtreeIndexWriteLarge() throws Exception {
        // create table
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        Options options = new Options();
        Path tablePath = new Path(warehouse.toString() + "/default.db/test_btree_index_large");
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        options.set(BTREE_INDEX_COMPRESSION, "zstd");
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // write data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < 2000; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("k" + i),
                                BinaryString.fromString("v" + i)));
            }
            commit.commit(write.prepareCommit());
        }

        // build index
        BTreeGlobalIndexBuilder builder =
                new BTreeGlobalIndexBuilder(table).withIndexType("btree").withIndexField("k");
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(builder.build(builder.scan(), IOManager.create(warehouse.toString())));
        }

        // assert index
        List<IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest);
        assertThat(indexEntries)
                .singleElement()
                .matches(entry -> entry.indexFile().rowCount() == 2000);

        // read index
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withFilter(predicateBuilder.equal(0, BinaryString.fromString("k2")));
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(r -> result.add(r.getString(1).toString()));
        assertThat(result).containsOnly("v2");
    }

    private void testBtreeIndexWriteNull() throws Exception {
        // create table
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        Options options = new Options();
        Path tablePath = new Path(warehouse.toString() + "/default.db/test_btree_index_null");
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // write data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(BinaryString.fromString("k1"), BinaryString.fromString("v1")));
            write.write(
                    GenericRow.of(BinaryString.fromString("k2"), BinaryString.fromString("v2")));
            write.write(GenericRow.of(null, BinaryString.fromString("v3")));
            write.write(
                    GenericRow.of(BinaryString.fromString("k4"), BinaryString.fromString("v4")));
            write.write(GenericRow.of(null, BinaryString.fromString("v5")));
            commit.commit(write.prepareCommit());
        }

        // build index
        BTreeGlobalIndexBuilder builder =
                new BTreeGlobalIndexBuilder(table).withIndexType("btree").withIndexField("k");
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(builder.build(builder.scan(), IOManager.create(warehouse.toString())));
        }

        // assert index
        List<IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest);
        assertThat(indexEntries)
                .singleElement()
                .matches(entry -> entry.indexFile().rowCount() == 5);

        // read index is null
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicateBuilder.isNull(0));
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(r -> result.add(r.getString(1).toString()));
        assertThat(result).containsExactlyInAnyOrder("v3", "v5");
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
        options.set(PATH, tablePath.toString());
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

    private static InternalRow createRow7Cols(
            int id,
            String name,
            String category,
            double value,
            long ts,
            long tsLtz,
            String metadataSource,
            long metadataCreatedAt,
            String city,
            String country) {
        GenericRow locationRow =
                GenericRow.of(BinaryString.fromString(city), BinaryString.fromString(country));
        GenericRow metadataRow =
                GenericRow.of(
                        BinaryString.fromString(metadataSource), metadataCreatedAt, locationRow);
        return GenericRow.of(
                id,
                BinaryString.fromString(name),
                BinaryString.fromString(category),
                value,
                org.apache.paimon.data.Timestamp.fromEpochMillis(ts),
                org.apache.paimon.data.Timestamp.fromEpochMillis(tsLtz),
                metadataRow);
    }

    protected GenericRow createRow3Cols(Object... values) {
        return GenericRow.of(values[0], values[1], values[2]);
    }

    protected GenericRow createRow3ColsWithKind(RowKind rowKind, Object... values) {
        return GenericRow.ofKind(rowKind, values[0], values[1], values[2]);
    }

    private static String rowToStringWithStruct(InternalRow row, RowType type) {
        StringBuilder build = new StringBuilder();
        build.append(row.getRowKind().shortString()).append("[");
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i != 0) {
                build.append(", ");
            }
            if (row.isNullAt(i)) {
                build.append("NULL");
            } else {
                InternalRow.FieldGetter fieldGetter =
                        InternalRow.createFieldGetter(type.getTypeAt(i), i);
                Object field = fieldGetter.getFieldOrNull(row);
                build.append(DataFormatTestUtil.getDataFieldString(field, type.getTypeAt(i)));
            }
        }
        build.append("]");
        return build.toString();
    }
}
