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

package org.apache.flink.table.store.connector;

import org.apache.flink.types.Row;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for partial update. */
public class AggregationITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {

        String ddl1 =
                "CREATE TABLE IF NOT EXISTS T3 ( "
                        + " a STRING, "
                        + " b BIGINT, "
                        + " c INT, "
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation' ,"
                        + " 'b.aggregate-function'='sum' ,"
                        + " 'c.aggregate-function'='min' "
                        + " );";
        String ddl2 =
                "CREATE TABLE IF NOT EXISTS T4 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " c INT,"
                        + " d BIGINT,"
                        + " e FLOAT,"
                        + " f DOUBLE,"
                        + " g DECIMAL(6,4),"
                        + " h TINYINT,"
                        + " i SMALLINT,"
                        + " PRIMARY KEY (a, b) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'c.aggregate-function' = 'sum',"
                        + " 'd.aggregate-function' = 'max',"
                        + " 'e.aggregate-function' = 'sum',"
                        + " 'f.aggregate-function' = 'avg',"
                        + " 'g.aggregate-function' = 'MaX',"
                        + " 'h.aggregate-function' = 'min',"
                        + " 'i.aggregate-function' = 'max' "
                        + " );";
        String ddl3 =
                "CREATE TABLE IF NOT EXISTS T5 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " c DOUBLE,"
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'b.aggregate-function' = 'sum'"
                        + " );";
        String ddl4 =
                "CREATE TABLE IF NOT EXISTS T6 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " c BOOLEAN,"
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'b.aggregate-function' = 'sum',"
                        + " 'c.aggregate-function' = 'max'"
                        + ");";
        String ddl5 =
                "CREATE TABLE IF NOT EXISTS T7 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'b.aggregate-function' = 'masasgadgasdgasdg'"
                        + ");";
        List<String> lists = new ArrayList<>();
        lists.add(ddl1);
        lists.add(ddl2);
        lists.add(ddl3);
        lists.add(ddl4);
        lists.add(ddl5);
        return lists;
    }

    @Test
    public void testCreateAggregateFunction() throws ExecutionException, InterruptedException {
        // T5 should set aggregate function for every column not part of primary key
        assertThatThrownBy(
                () -> {
                    bEnv.executeSql(
                                    "INSERT INTO T5 VALUES "
                                            + "('pk1',1, 2.0), "
                                            + "('pk1',1, 2.0)")
                            .await();
                });
        // T6 Unsupported column type(boolean)
        assertThatThrownBy(
                () -> {
                    bEnv.executeSql(
                                    "INSERT INTO T6 VALUES "
                                            + "('pk1',1, true), "
                                            + "('pk1',1, false)")
                            .await();
                });
        // T7 Unsupported aggregation kind
        assertThatThrownBy(
                () -> {
                    bEnv.executeSql("INSERT INTO T7 VALUES " + "('pk1',1), " + "('pk1',1)").await();
                });
    }

    @Test
    public void testMergeInMemory() throws ExecutionException, InterruptedException {
        List<Row> result;
        // T3
        bEnv.executeSql("INSERT INTO T3 VALUES " + "('pk1',1, 1), " + "('pk1',1, 2)").await();
        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 2L, 1));

        // T4
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES "
                                + "('pk1',1,3,100000,3.2,2.48, cast('12.3456' as decimal(6,4)),cast(2 as tinyint),cast(1 as smallint)), "
                                + "('pk1',1,4,100001,3.2,2.44, cast('12.5678' as decimal(6,4)),cast(4 as tinyint),cast(0 as smallint))")
                .await();
        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                "pk1",
                                1,
                                7,
                                100_001L,
                                Float.valueOf("6.4"),
                                Double.valueOf("2.46"),
                                new BigDecimal("12.5678"),
                                Byte.valueOf("2"),
                                Short.valueOf("1")));
    }

    @Test
    public void testMergeRead() throws ExecutionException, InterruptedException {
        List<Row> result;
        // T3
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 2)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 4)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',2, 0)").await();
        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 4L, 0));

        // T4
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',1,3,100000,3.2,2.48, cast('12.5678' as decimal(6,4)),cast(2 as tinyint),cast(13 as smallint))")
                .await();
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',1,6,112322,3.4,2.46, cast('12.3456' as decimal(6,4)),cast(1 as tinyint),cast(12 as smallint))")
                .await();
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',1,12,100000,3.6,2.44,cast('12.1234' as decimal(6,4)),cast(0 as tinyint),cast(10 as smallint))")
                .await();
        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                "pk1",
                                1,
                                21,
                                112322L,
                                Float.valueOf("10.2"),
                                Double.valueOf("2.46"),
                                new BigDecimal("12.5678"),
                                Byte.valueOf("0"),
                                Short.valueOf("13")));
    }

    @Test
    public void testMergeCompaction() throws ExecutionException, InterruptedException {
        List<Row> result;

        // T3
        // Wait compaction
        bEnv.executeSql("ALTER TABLE T3 SET ('commit.force-compact'='true')");

        // key pk1
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 3, 1)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 4, 5)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 4, 6)").await();

        // key pk2
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 6,7)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 9,0)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 4,4)").await();

        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 11L, 1), Row.of("pk2", 19L, 0));

        // T4
        // Wait compaction
        bEnv.executeSql("ALTER TABLE T4 SET ('commit.force-compact'='true')");

        // key pk1_3
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',1,3,100000,3.2,2.48, cast('12.3456' as decimal(6,4)),cast(2 as tinyint),cast(1 as smallint))")
                .await();
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',1,30,200000,1.2,1.24, cast('45.3456' as decimal(6,4)),cast(0 as tinyint),cast(5 as smallint))")
                .await();
        // key pk1_4
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',2,3,100000,3.2,2.48, cast('12.3456' as decimal(6,4)),cast(2 as tinyint),cast(1 as smallint))")
                .await();
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk1',2,30,200000,1.2,1.24, cast('45.3456' as decimal(6,4)),cast(0 as tinyint),cast(5 as smallint))")
                .await();
        // key pk2_2
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk2',2,3,100000,3.2,2.48, cast('12.3456' as decimal(6,4)),cast(2 as tinyint),cast(1 as smallint))")
                .await();
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk2',2,30,200000,1.2,1.24, cast('45.3456' as decimal(6,4)),cast(0 as tinyint),cast(5 as smallint))")
                .await();
        // key pk2_4
        bEnv.executeSql(
                        "INSERT INTO T4 VALUES ('pk2',4,3,100000,3.2,2.48, cast('12.3456' as decimal(6,4)),cast(2 as tinyint),cast(1 as smallint))")
                .await();

        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                "pk1",
                                1,
                                33,
                                200_000L,
                                Float.valueOf("4.4"),
                                Double.valueOf(" 1.86"),
                                new BigDecimal("45.3456"),
                                Byte.valueOf("0"),
                                Short.valueOf("5")),
                        Row.of(
                                "pk1",
                                2,
                                33,
                                200_000L,
                                Float.valueOf("4.4"),
                                Double.valueOf(" 1.86"),
                                new BigDecimal("45.3456"),
                                Byte.valueOf("0"),
                                Short.valueOf("5")),
                        Row.of(
                                "pk2",
                                2,
                                33,
                                200_000L,
                                Float.valueOf("4.4"),
                                Double.valueOf("1.86"),
                                new BigDecimal("45.3456"),
                                Byte.valueOf("0"),
                                Short.valueOf("5")),
                        Row.of(
                                "pk2",
                                4,
                                3,
                                100_000L,
                                Float.valueOf("3.2"),
                                Double.valueOf("2.48"),
                                new BigDecimal("12.3456"),
                                Byte.valueOf("2"),
                                Short.valueOf("1")));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T3").execute().print(),
                "Aggregation update continuous reading is not supported");
    }
}
