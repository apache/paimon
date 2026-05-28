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
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;

/** E2E test for Java and Python interoperability with Vortex vector dedicated files. */
public class JavaPyVortexE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(JavaPyVortexE2ETest.class);

    java.nio.file.Path tempDir =
            Paths.get("../../paimon-python/pypaimon/tests/e2e").toAbsolutePath();

    protected final String commitUser = UUID.randomUUID().toString();
    protected Path warehouse;
    protected Catalog catalog;
    protected String database;

    @BeforeEach
    public void before() throws Exception {
        database = "default";

        if (!Files.exists(tempDir.resolve("warehouse"))) {
            Files.createDirectories(tempDir.resolve("warehouse"));
        }

        warehouse = new Path(tempDir.resolve("warehouse").toUri());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));

        try {
            catalog.createDatabase(database, false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // ignore
        }
    }

    private Identifier identifier(String tableName) {
        return new Identifier(database, tableName);
    }

    /**
     * Java writes a vector table with dedicated vector files (vector.file.format=vortex) for Python
     * to read.
     */
    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteVectorDedicatedFile() throws Exception {
        Identifier identifier = identifier("vector_dedicated_test");
        catalog.dropTable(identifier, true);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .column("label", DataTypes.STRING())
                        .option("file.format", "vortex")
                        .option("vector.file.format", "vortex")
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", "true")
                        .option("bucket", "-1")
                        .build();

        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}),
                            BinaryString.fromString("first")));
            write.write(
                    GenericRow.of(
                            2,
                            BinaryVector.fromPrimitiveArray(new float[] {4.0f, 5.0f, 6.0f}),
                            BinaryString.fromString("second")));
            write.write(
                    GenericRow.of(
                            3,
                            BinaryVector.fromPrimitiveArray(new float[] {-1.0f, 0.5f, 2.5f}),
                            BinaryString.fromString("third")));
            commit.commit(write.prepareCommit());
        }

        List<Split> splits = new ArrayList<>(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, [1.0, 2.0, 3.0], first",
                        "2, [4.0, 5.0, 6.0], second",
                        "3, [-1.0, 0.5, 2.5], third");
        LOG.info("testJavaWriteVectorDedicatedFile: wrote and read back {} rows", res.size());
    }

    /** Java reads a vector table with dedicated vector files written by Python. */
    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaReadVectorDedicatedFile() throws Exception {
        Identifier identifier = identifier("py_vector_dedicated_test");
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        assertThat(table.rowType().getFieldNames()).contains("embedding");
        int embIdx = table.rowType().getFieldIndex("embedding");
        assertThat(table.rowType().getTypeAt(embIdx))
                .isEqualTo(DataTypes.VECTOR(3, DataTypes.FLOAT()));

        List<Split> splits = new ArrayList<>(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, [1.0, 2.0, 3.0], first",
                        "2, [4.0, 5.0, 6.0], second",
                        "3, [-1.0, 0.5, 2.5], third");
        LOG.info(
                "testJavaReadVectorDedicatedFile: Java read {} rows with dedicated vector files written by Python",
                res.size());
    }

    /**
     * Java writes a vector table with multiple vector columns in a single .vector.vortex file for
     * Python to read.
     */
    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaWriteMultiVectorDedicatedFile() throws Exception {
        Identifier identifier = identifier("multi_vector_dedicated_test");
        catalog.dropTable(identifier, true);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embed1", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .column("embed2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .column("label", DataTypes.STRING())
                        .option("file.format", "vortex")
                        .option("vector.file.format", "vortex")
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", "true")
                        .option("bucket", "-1")
                        .build();

        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}),
                            BinaryVector.fromPrimitiveArray(new float[] {10.0f, 20.0f}),
                            BinaryString.fromString("first")));
            write.write(
                    GenericRow.of(
                            2,
                            BinaryVector.fromPrimitiveArray(new float[] {4.0f, 5.0f, 6.0f}),
                            BinaryVector.fromPrimitiveArray(new float[] {40.0f, 50.0f}),
                            BinaryString.fromString("second")));
            write.write(
                    GenericRow.of(
                            3,
                            BinaryVector.fromPrimitiveArray(new float[] {-1.0f, 0.5f, 2.5f}),
                            BinaryVector.fromPrimitiveArray(new float[] {-10.0f, 5.0f}),
                            BinaryString.fromString("third")));
            commit.commit(write.prepareCommit());
        }

        List<Split> splits = new ArrayList<>(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, [1.0, 2.0, 3.0], [10.0, 20.0], first",
                        "2, [4.0, 5.0, 6.0], [40.0, 50.0], second",
                        "3, [-1.0, 0.5, 2.5], [-10.0, 5.0], third");
        LOG.info("testJavaWriteMultiVectorDedicatedFile: wrote and read back {} rows", res.size());
    }

    /** Java reads a multi-vector-column table with dedicated vector files written by Python. */
    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testJavaReadMultiVectorDedicatedFile() throws Exception {
        Identifier identifier = identifier("py_multi_vector_dedicated_test");
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        assertThat(table.rowType().getFieldNames()).contains("embed1", "embed2");
        assertThat(table.rowType().getTypeAt(table.rowType().getFieldIndex("embed1")))
                .isEqualTo(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        assertThat(table.rowType().getTypeAt(table.rowType().getFieldIndex("embed2")))
                .isEqualTo(DataTypes.VECTOR(2, DataTypes.FLOAT()));

        List<Split> splits = new ArrayList<>(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        List<String> res =
                getResult(
                        read,
                        splits,
                        row -> DataFormatTestUtil.toStringNoRowKind(row, table.rowType()));
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "1, [1.0, 2.0, 3.0], [10.0, 20.0], first",
                        "2, [4.0, 5.0, 6.0], [40.0, 50.0], second",
                        "3, [-1.0, 0.5, 2.5], [-10.0, 5.0], third");
        LOG.info(
                "testJavaReadMultiVectorDedicatedFile: Java read {} rows written by Python",
                res.size());
    }
}
