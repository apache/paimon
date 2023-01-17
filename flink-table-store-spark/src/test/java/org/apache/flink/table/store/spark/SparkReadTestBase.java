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
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
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
import org.apache.commons.lang3.StringUtils;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Base tests for spark read. */
public abstract class SparkReadTestBase {
    private static final String COMMIT_USER = "user";
    private static final AtomicLong COMMIT_IDENTIFIER = new AtomicLong(0);

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
        spark.sql("USE tablestore");
        spark.sql("CREATE NAMESPACE default");

        // flink sink
        tablePath1 = new Path(warehousePath, "default.db/t1");
        createTable("t1");
        writeTable(
                "t1",
                GenericRow.of(1, 2L, BinaryString.fromString("1")),
                GenericRow.of(3, 4L, BinaryString.fromString("2")),
                GenericRow.of(5, 6L, BinaryString.fromString("3")),
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, BinaryString.fromString("2")));

        // a int not null
        // b array<varchar> not null
        // c row<row<double, array<boolean> not null> not null, bigint> not null
        tablePath2 = new Path(warehousePath, "default.db/t2");
        spark.sql(
                "CREATE TABLE tablestore.default.t2 (a INT NOT NULL COMMENT 'comment about a', b ARRAY<STRING> NOT NULL, c STRUCT<c1: STRUCT<c11: DOUBLE, c12: ARRAY<BOOLEAN> NOT NULL> NOT NULL, c2: BIGINT COMMENT 'comment about c2'> NOT NULL COMMENT 'comment about c') TBLPROPERTIES ('file.format'='avro')");
        //        createTable1("t2");
        writeTable(
                "t2",
                "(1, array('AAA', 'BBB'), struct(struct(1.0d, array(null)), 1L))",
                "(2, array('CCC', 'DDD'), struct(struct(null, array(true)), null))");
        writeTable(
                "t2",
                "(3, array(null, null), struct(struct(2.0d, array(true, false)), 2L))",
                "(4, array(null, 'EEE'), struct(struct(3.0d, array(true, false, true)), 3L))");
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

    /**
     * Create table with fields: a->int not null, b->bigint, c->string. orc is shaded, can not find
     * shaded classes in ide, we use avro here.
     *
     * @param tableName the given table name
     */
    protected static void createTable(String tableName) {
        spark.sql(
                String.format(
                        "CREATE TABLE tablestore.default.%s (a INT NOT NULL, b BIGINT, c STRING) TBLPROPERTIES ('file.format'='avro')",
                        tableName));
    }

    private static void writeTable(String tableName, GenericRow... rows) throws Exception {
        FileStoreTable fileStoreTable =
                FileStoreTableFactory.create(
                        new Path(warehousePath, String.format("default.db/%s", tableName)));
        TableWrite writer = fileStoreTable.newWrite(COMMIT_USER);
        TableCommit commit = fileStoreTable.newCommit(COMMIT_USER);
        for (GenericRow row : rows) {
            writer.write(row);
        }
        long commitIdentifier = COMMIT_IDENTIFIER.getAndIncrement();
        commit.commit(commitIdentifier, writer.prepareCommit(true, commitIdentifier));
    }

    protected static void writeTable(String tableName, String... values) {
        spark.sql(
                String.format(
                        "INSERT INTO tablestore.default.%s VALUES %s",
                        tableName, StringUtils.join(values, ",")));
    }
}
