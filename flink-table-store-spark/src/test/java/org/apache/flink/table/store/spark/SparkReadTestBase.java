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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.RowDataType;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

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
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.write(GenericRowData.ofKind(RowKind.DELETE, 3, 4L, StringData.fromString("2")));
        testHelper1.commit();

        // a int not null
        // b array<varchar> not null
        // c row<row<double, array<boolean> not null> not null, bigint> not null
        tablePath2 = new Path(warehousePath, "default.db/t2");
        SimpleTableTestHelper testHelper2 = new SimpleTableTestHelper(tablePath2, rowType2());
        testHelper2.write(
                GenericRowData.of(
                        1,
                        new GenericArrayData(
                                new StringData[] {
                                    StringData.fromString("AAA"), StringData.fromString("BBB")
                                }),
                        GenericRowData.of(
                                GenericRowData.of(1.0d, new GenericArrayData(new Boolean[] {null})),
                                1L)));
        testHelper2.write(
                GenericRowData.of(
                        2,
                        new GenericArrayData(
                                new StringData[] {
                                    StringData.fromString("CCC"), StringData.fromString("DDD")
                                }),
                        GenericRowData.of(
                                GenericRowData.of(null, new GenericArrayData(new Boolean[] {true})),
                                null)));
        testHelper2.commit();

        testHelper2.write(
                GenericRowData.of(
                        3,
                        new GenericArrayData(new StringData[] {null, null}),
                        GenericRowData.of(
                                GenericRowData.of(
                                        2.0d, new GenericArrayData(new boolean[] {true, false})),
                                2L)));

        testHelper2.write(
                GenericRowData.of(
                        4,
                        new GenericArrayData(new StringData[] {null, StringData.fromString("EEE")}),
                        GenericRowData.of(
                                GenericRowData.of(
                                        3.0d,
                                        new GenericArrayData(new Boolean[] {true, false, true})),
                                3L)));
        testHelper2.commit();
    }

    protected static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new IntType(false)),
                                new RowType.RowField("b", new BigIntType()),
                                new RowType.RowField("c", new VarCharType())));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    protected static SimpleTableTestHelper createTestHelperWithoutDDL(Path tablePath)
            throws Exception {
        return new SimpleTableTestHelper(tablePath);
    }

    private static RowType rowType1() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("a", new IntType(false)),
                        new RowType.RowField("b", new BigIntType()),
                        new RowType.RowField("c", new VarCharType())));
    }

    private static RowType rowType2() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("a", new IntType(false), "comment about a"),
                        new RowType.RowField("b", new ArrayType(false, new VarCharType())),
                        new RowType.RowField(
                                "c",
                                new RowType(
                                        false,
                                        Arrays.asList(
                                                new RowType.RowField(
                                                        "c1",
                                                        new RowType(
                                                                false,
                                                                Arrays.asList(
                                                                        new RowType.RowField(
                                                                                "c11",
                                                                                new DoubleType()),
                                                                        new RowType.RowField(
                                                                                "c12",
                                                                                new ArrayType(
                                                                                        false,
                                                                                        new BooleanType()))))),
                                                new RowType.RowField(
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
        return field.type().logicalType().isNullable();
    }

    protected DataField getField(TableSchema schema, int index) {
        return schema.fields().get(index);
    }

    protected DataField getNestedField(DataField field, int index) {
        if (field.type() instanceof RowDataType) {
            RowDataType rowDataType = (RowDataType) field.type();
            return rowDataType.fields().get(index);
        }
        throw new IllegalArgumentException();
    }
}
