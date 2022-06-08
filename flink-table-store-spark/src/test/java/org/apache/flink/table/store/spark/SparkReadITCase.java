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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for spark reader. */
public class SparkReadITCase {

    private static SparkSession spark = null;

    @TempDir java.nio.file.Path tempDir;

    private Path path;

    private SimpleTableTestHelper testHelper;

    @BeforeEach
    public void beforeEach() throws Exception {
        this.path = new Path(tempDir.toUri().toString(), "my_table");
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new IntType()),
                                new RowType.RowField("b", new BigIntType()),
                                new RowType.RowField("c", new VarCharType())));
        testHelper = new SimpleTableTestHelper(path, rowType);
    }

    @BeforeAll
    public static void startMetastoreAndSpark() {
        spark = SparkSession.builder().master("local[2]").getOrCreate();
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testNormal() throws Exception {
        testHelper.write(ValueKind.ADD, GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper.write(ValueKind.ADD, GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper.write(ValueKind.ADD, GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper.write(ValueKind.DELETE, GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper.commit();

        Dataset<Row> dataset =
                spark.read().format("tablestore").option("path", path.toString()).load();

        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        results = dataset.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = dataset.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    @Test
    public void testFilterPushDown() throws Exception {
        testHelper.write(ValueKind.ADD, GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper.write(ValueKind.ADD, GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper.commit();

        testHelper.write(ValueKind.ADD, GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper.write(ValueKind.ADD, GenericRowData.of(7, 8L, StringData.fromString("4")));
        testHelper.commit();

        Dataset<Row> dataset =
                spark.read().format("tablestore").option("path", path.toString()).load();

        List<Row> results = dataset.filter("a < 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [3,2]]");
    }
}
