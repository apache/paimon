/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.GenericArray;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.BooleanType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DoubleType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Base tests for spark read. */
public abstract class SparkReadTestBase {

    private static File warehouse = null;

    protected static SparkSession spark = null;

    protected static Path warehousePath = null;

    protected static Path tablePath1;

    protected static Path tablePath2;

    @BeforeAll
    public static void startMetastoreAndSpark() throws Exception {
        warehouse = Files.createTempFile("warehouse", null).toFile();
        assertThat(warehouse.delete()).isTrue();
        warehousePath = new Path("file:" + warehouse);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.tablestore", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.tablestore.warehouse", warehousePath.toString());

        // flink sink
        tablePath1 = new Path(warehousePath, "default.db/t1");
        SimpleTableTestHelper testHelper1 = new SimpleTableTestHelper(tablePath1, rowType1());
        testHelper1.write(GenericRow.of(1, 2L, BinaryString.fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, BinaryString.fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, BinaryString.fromString("3")));
        testHelper1.write(GenericRow.ofKind(RowKind.DELETE, 3, 4L, BinaryString.fromString("2")));
        testHelper1.commit();

        // a int not null
        // b array<varchar> not null
        // c row<row<double, array<boolean> not null> not null, bigint> not null
        tablePath2 = new Path(warehousePath, "default.db/t2");
        SimpleTableTestHelper testHelper2 = new SimpleTableTestHelper(tablePath2, rowType2());
        testHelper2.write(
                GenericRow.of(
                        1,
                        new GenericArray(
                                new BinaryString[] {
                                    BinaryString.fromString("AAA"), BinaryString.fromString("BBB")
                                }),
                        GenericRow.of(
                                GenericRow.of(1.0d, new GenericArray(new Boolean[] {null})), 1L)));
        testHelper2.write(
                GenericRow.of(
                        2,
                        new GenericArray(
                                new BinaryString[] {
                                    BinaryString.fromString("CCC"), BinaryString.fromString("DDD")
                                }),
                        GenericRow.of(
                                GenericRow.of(null, new GenericArray(new Boolean[] {true})),
                                null)));
        testHelper2.commit();

        testHelper2.write(
                GenericRow.of(
                        3,
                        new GenericArray(new BinaryString[] {null, null}),
                        GenericRow.of(
                                GenericRow.of(2.0d, new GenericArray(new boolean[] {true, false})),
                                2L)));

        testHelper2.write(
                GenericRow.of(
                        4,
                        new GenericArray(new BinaryString[] {null, BinaryString.fromString("EEE")}),
                        GenericRow.of(
                                GenericRow.of(
                                        3.0d, new GenericArray(new Boolean[] {true, false, true})),
                                3L)));
        testHelper2.commit();
    }

    protected static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType(false)),
                                new DataField(1, "b", new BigIntType()),
                                new DataField(2, "c", new VarCharType())));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    protected static SimpleTableTestHelper createTestHelperWithoutDDL(Path tablePath)
            throws Exception {
        return new SimpleTableTestHelper(tablePath);
    }

    private static RowType rowType1() {
        return new RowType(
                Arrays.asList(
                        new DataField(0, "a", new IntType(false)),
                        new DataField(1, "b", new BigIntType()),
                        new DataField(2, "c", new VarCharType())));
    }

    private static RowType rowType2() {
        return new RowType(
                Arrays.asList(
                        new DataField(0, "a", new IntType(false), "comment about a"),
                        new DataField(1, "b", new ArrayType(false, new VarCharType())),
                        new DataField(
                                2,
                                "c",
                                new RowType(
                                        false,
                                        Arrays.asList(
                                                new DataField(
                                                        3,
                                                        "c1",
                                                        new RowType(
                                                                false,
                                                                Arrays.asList(
                                                                        new DataField(
                                                                                4,
                                                                                "c11",
                                                                                new DoubleType()),
                                                                        new DataField(
                                                                                5,
                                                                                "c12",
                                                                                new ArrayType(
                                                                                        false,
                                                                                        new BooleanType()))))),
                                                new DataField(
                                                        6,
                                                        "c2",
                                                        new BigIntType(),
                                                        "comment about c2"))),
                                "comment about c")));
    }

    @AfterAll
    public static void stopMetastoreAndSpark() throws IOException {
        if (warehouse != null && warehouse.exists()) {
            FileUtils.deleteDirectory(warehouse);
        }
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    protected void innerTestSimpleType(Dataset<Row> dataset) {
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        results = dataset.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = dataset.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    protected TableSchema schema1() {
        return FileStoreTableFactory.create(tablePath1).schema();
    }

    protected TableSchema schema2() {
        return FileStoreTableFactory.create(tablePath2).schema();
    }

    protected boolean fieldIsNullable(DataField field) {
        return field.type().isNullable();
    }

    protected DataField getField(TableSchema schema, int index) {
        return schema.fields().get(index);
    }

    protected DataField getNestedField(DataField field, int index) {
        if (field.type() instanceof org.apache.flink.table.store.types.RowType) {
            org.apache.flink.table.store.types.RowType rowDataType =
                    (org.apache.flink.table.store.types.RowType) field.type();
            return rowDataType.getFields().get(index);
        }
        throw new IllegalArgumentException();
    }
}
