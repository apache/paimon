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
import org.apache.paimon.format.lance.jni.LanceReader;
import org.apache.paimon.format.lance.jni.LanceWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;

/** Mixed language e2e test for Java and Python interoperability with Lance format. */
public class JavaPyLanceE2ETest {

    private static boolean tokioRuntimeInitialized = false;
    private static final Object TOKIO_INIT_LOCK = new Object();

    static {
    }

    @BeforeAll
    public static void initializeTokioRuntime() {
        synchronized (TOKIO_INIT_LOCK) {
            if (tokioRuntimeInitialized) {
                return;
            }
            try {
                // Try to initialize Tokio runtime by creating and reading a minimal Lance file
                // This is a workaround to ensure Tokio runtime is available before reading
                // Python-written files
                java.nio.file.Path tempInitDir =
                        Files.createTempDirectory("paimon-lance-tokio-init");
                String testFilePath = tempInitDir.resolve("tokio_init_test.lance").toString();

                // Create a minimal Lance file using Java writer
                RootAllocator allocator = new RootAllocator();
                try {
                    // Create a simple schema
                    Field intField =
                            new Field(
                                    "id", FieldType.nullable(Types.MinorType.INT.getType()), null);
                    Field strField =
                            new Field(
                                    "name",
                                    FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                    null);
                    org.apache.arrow.vector.types.pojo.Schema schema =
                            new org.apache.arrow.vector.types.pojo.Schema(
                                    Arrays.asList(intField, strField));

                    // Create vectors
                    IntVector intVector = new IntVector("id", allocator);
                    VarCharVector strVector = new VarCharVector("name", allocator);

                    intVector.allocateNew(1);
                    strVector.allocateNew(1);

                    intVector.set(0, 1);
                    strVector.set(0, "test".getBytes());

                    intVector.setValueCount(1);
                    strVector.setValueCount(1);

                    List<FieldVector> vectors = new ArrayList<>();
                    vectors.add(intVector);
                    vectors.add(strVector);

                    VectorSchemaRoot vsr = new VectorSchemaRoot(schema, vectors, 1);

                    // Write using LanceWriter
                    Map<String, String> storageOptions = new HashMap<>();
                    LanceWriter writer = new LanceWriter(testFilePath, storageOptions);
                    try {
                        writer.writeVsr(vsr);
                    } finally {
                        writer.close();
                        vsr.close();
                        intVector.close();
                        strVector.close();
                    }

                    // Try to read it back - this may initialize Tokio runtime
                    // Note: This might fail if Tokio is not initialized, but we'll catch and
                    // ignore
                    try {
                        // Use field names matching the written schema ("id" and "name")
                        org.apache.paimon.types.RowType rowType =
                                org.apache.paimon.types.RowType.of(
                                        new org.apache.paimon.types.DataType[] {
                                            org.apache.paimon.types.DataTypes.INT(),
                                            org.apache.paimon.types.DataTypes.STRING()
                                        },
                                        new String[] {"id", "name"});
                        LanceReader reader =
                                new LanceReader(testFilePath, rowType, null, 1024, storageOptions);
                        try {
                            // Try to read at least one batch
                            reader.readBatch();
                        } finally {
                            reader.close();
                        }
                    } catch (Exception e) {
                        // Ignore - this is expected if Tokio is not initialized
                    }

                    // Clean up
                    Files.deleteIfExists(Paths.get(testFilePath));
                    Files.deleteIfExists(tempInitDir);

                } finally {
                    allocator.close();
                }

                tokioRuntimeInitialized = true;
            } catch (Exception e) {
                // Don't fail the test - this is just an attempt to initialize Tokio
            }
        }
    }

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
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        // Use preferIO to avoid FileIO loader conflicts (OSS vs Jindo)
        CatalogContext context = CatalogContext.create(options, new TraceableFileIO.Loader(), null);
        catalog = CatalogFactory.createCatalog(context);

        // Create database if it doesn't exist
        try {
            catalog.createDatabase(database, false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // Database already exists, ignore
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteReadPkTableLance() throws Exception {
        Identifier identifier = identifier("mixed_test_pk_tablej_lance");
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
                        .option("file.format", "lance")
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
    public void testReadPkTableLance() throws Exception {
        try {
            // Known issue: Reading Python-written Lance files in Java causes JVM crash due to
            // missing Tokio runtime. The error is:
            // "there is no reactor running, must be called from the context of a Tokio 1.x runtime"
            //
            // This is a limitation of lance-core Java bindings. The Rust native library requires
            // Tokio runtime for certain operations when reading files written by Python (which may
            // use different encoding formats). Java-written files can be read successfully because
            // they use synchronous APIs that don't require Tokio.
            //
            // Workaround: Try to "warm up" Tokio runtime by reading a Java-written file first.
            // This may initialize the Tokio runtime if it's created on first use.
            try {
                Identifier warmupIdentifier = identifier("mixed_test_pk_tablej_lance");
                try {
                    Table warmupTable = catalog.getTable(warmupIdentifier);
                    FileStoreTable warmupFileStoreTable = (FileStoreTable) warmupTable;
                    List<Split> warmupSplits =
                            new ArrayList<>(
                                    warmupFileStoreTable.newSnapshotReader().read().dataSplits());
                    if (!warmupSplits.isEmpty()) {
                        TableRead warmupRead = warmupFileStoreTable.newRead();
                        // Try to read at least one batch to initialize Tokio runtime
                        getResult(
                                warmupRead,
                                warmupSplits.subList(0, Math.min(1, warmupSplits.size())),
                                row ->
                                        DataFormatTestUtil.toStringNoRowKind(
                                                row, warmupTable.rowType()));
                    }
                } catch (Catalog.TableNotExistException e) {
                    // Table doesn't exist, skip warm-up
                }
            } catch (Exception e) {
                // Ignore warm-up errors, continue with the actual test
            }

            Identifier identifier = identifier("mixed_test_pk_tablep_lance");
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
            System.out.println("Format: lance, Result: " + res);
            assertThat(res)
                    .containsExactlyInAnyOrder(
                            "1, Apple, Fruit, 1.5",
                            "2, Banana, Fruit, 0.8",
                            "3, Carrot, Vegetable, 0.6",
                            "4, Broccoli, Vegetable, 1.2",
                            "5, Chicken, Meat, 5.0",
                            "6, Beef, Meat, 8.0");
        } catch (Throwable t) {
            throw t;
        }
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
