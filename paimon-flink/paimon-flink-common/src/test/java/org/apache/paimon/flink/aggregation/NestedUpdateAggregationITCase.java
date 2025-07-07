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

package org.apache.paimon.flink.aggregation;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FieldNestedUpdateAgg}. */
public class NestedUpdateAggregationITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        String ordersTable =
                "CREATE TABLE orders (\n"
                        + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                        + "  user_name STRING,\n"
                        + "  address STRING\n"
                        + ");";

        String subordersTable =
                "CREATE TABLE sub_orders (\n"
                        + "  order_id INT,\n"
                        + "  daily_id INT,\n"
                        + "  today STRING,\n"
                        + "  product_name STRING,\n"
                        + "  price BIGINT,\n"
                        + "  PRIMARY KEY (order_id, daily_id, today) NOT ENFORCED\n"
                        + ");";

        String wideTable =
                "CREATE TABLE order_wide (\n"
                        + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                        + "  user_name STRING,\n"
                        + "  address STRING,\n"
                        + "  sub_orders ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>\n"
                        + ") WITH (\n"
                        + "  'merge-engine' = 'aggregation',\n"
                        + "  'fields.sub_orders.aggregate-function' = 'nested_update',\n"
                        + "  'fields.sub_orders.nested-key' = 'daily_id,today',\n"
                        + "  'fields.sub_orders.ignore-retract' = 'true',"
                        + "  'fields.user_name.ignore-retract' = 'true',"
                        + "  'fields.address.ignore-retract' = 'true'"
                        + ")";

        String wideAppendTable =
                "CREATE TABLE order_append_wide (\n"
                        + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                        + "  user_name STRING,\n"
                        + "  address STRING,\n"
                        + "  sub_orders ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>\n"
                        + ") WITH (\n"
                        + "  'merge-engine' = 'aggregation',\n"
                        + "  'fields.sub_orders.aggregate-function' = 'nested_update',\n"
                        + "  'fields.sub_orders.ignore-retract' = 'true',"
                        + "  'fields.user_name.ignore-retract' = 'true',"
                        + "  'fields.address.ignore-retract' = 'true'"
                        + ")";

        return Arrays.asList(ordersTable, subordersTable, wideTable, wideAppendTable);
    }

    @Test
    public void testUseCase() {
        sql(
                "INSERT INTO orders VALUES "
                        + "(1, 'Wang', 'HangZhou'),"
                        + "(2, 'Zhao', 'ChengDu'),"
                        + "(3, 'Liu', 'NanJing')");

        sql(
                "INSERT INTO sub_orders VALUES "
                        + "(1, 1, '12-20', 'Apple', 8000),"
                        + "(1, 2, '12-20', 'Tesla', 400000),"
                        + "(1, 1, '12-21', 'Sangsung', 5000),"
                        + "(2, 1, '12-20', 'Tea', 40),"
                        + "(2, 2, '12-20', 'Pot', 60),"
                        + "(3, 1, '12-25', 'Bat', 15),"
                        + "(3, 1, '12-26', 'Cup', 30)");

        sql(widenSql());

        List<Row> result =
                sql("SELECT * FROM order_wide").stream()
                        .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                        .collect(Collectors.toList());

        assertThat(
                        checkOneRecord(
                                result.get(0),
                                1,
                                "Wang",
                                "HangZhou",
                                Row.of(1, "12-20", "Apple", 8000L),
                                Row.of(1, "12-21", "Sangsung", 5000L),
                                Row.of(2, "12-20", "Tesla", 400000L)))
                .isTrue();
        assertThat(
                        checkOneRecord(
                                result.get(1),
                                2,
                                "Zhao",
                                "ChengDu",
                                Row.of(1, "12-20", "Tea", 40L),
                                Row.of(2, "12-20", "Pot", 60L)))
                .isTrue();
        assertThat(
                        checkOneRecord(
                                result.get(2),
                                3,
                                "Liu",
                                "NanJing",
                                Row.of(1, "12-25", "Bat", 15L),
                                Row.of(1, "12-26", "Cup", 30L)))
                .isTrue();

        // query using UNNEST
        List<Row> unnested =
                sql(
                        "SELECT order_id, user_name, address, daily_id, today, product_name, price "
                                + "FROM order_wide, UNNEST(sub_orders) AS so(daily_id, today, product_name, price)");

        assertThat(unnested)
                .containsExactlyInAnyOrder(
                        Row.of(1, "Wang", "HangZhou", 1, "12-20", "Apple", 8000L),
                        Row.of(1, "Wang", "HangZhou", 2, "12-20", "Tesla", 400000L),
                        Row.of(1, "Wang", "HangZhou", 1, "12-21", "Sangsung", 5000L),
                        Row.of(2, "Zhao", "ChengDu", 1, "12-20", "Tea", 40L),
                        Row.of(2, "Zhao", "ChengDu", 2, "12-20", "Pot", 60L),
                        Row.of(3, "Liu", "NanJing", 1, "12-25", "Bat", 15L),
                        Row.of(3, "Liu", "NanJing", 1, "12-26", "Cup", 30L));
    }

    @Test
    public void testUseCaseWithNullValue() {
        sql(
                "INSERT INTO order_wide\n"
                        + "SELECT 6, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                        + "ARRAY[cast(null as ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>)]");

        List<Row> result =
                sql("SELECT * FROM order_wide").stream()
                        .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                        .collect(Collectors.toList());

        assertThat(checkOneRecord(result.get(0), 6, null, null, (Row) null)).isTrue();

        sql(
                "INSERT INTO order_wide\n"
                        + "SELECT 6, 'Sun', CAST (NULL AS STRING), "
                        + "ARRAY[ROW(1, '01-01','Apple', 6999)]");

        result =
                sql("SELECT * FROM order_wide").stream()
                        .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                        .collect(Collectors.toList());
        assertThat(
                        checkOneRecord(
                                result.get(0), 6, "Sun", null, Row.of(1, "01-01", "Apple", 6999L)))
                .isTrue();
    }

    @Test
    public void testUseCaseAppend() {
        sql(
                "INSERT INTO orders VALUES "
                        + "(1, 'Wang', 'HangZhou'),"
                        + "(2, 'Zhao', 'ChengDu'),"
                        + "(3, 'Liu', 'NanJing')");

        sql(
                "INSERT INTO sub_orders VALUES "
                        + "(1, 1, '12-20', 'Apple', 8000),"
                        + "(2, 1, '12-20', 'Tesla', 400000),"
                        + "(3, 1, '12-25', 'Bat', 15),"
                        + "(3, 1, '12-26', 'Cup', 30)");

        sql(widenAppendSql());

        // query using UNNEST
        List<Row> unnested =
                sql(
                        "SELECT order_id, user_name, address, daily_id, today, product_name, price "
                                + "FROM order_append_wide, UNNEST(sub_orders) AS so(daily_id, today, product_name, price)");

        assertThat(unnested)
                .containsExactlyInAnyOrder(
                        Row.of(1, "Wang", "HangZhou", 1, "12-20", "Apple", 8000L),
                        Row.of(2, "Zhao", "ChengDu", 1, "12-20", "Tesla", 400000L),
                        Row.of(3, "Liu", "NanJing", 1, "12-25", "Bat", 15L),
                        Row.of(3, "Liu", "NanJing", 1, "12-26", "Cup", 30L));
    }

    @Test
    @Timeout(60)
    public void testUpdateWithIgnoreRetract() throws Exception {
        sEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                        ExecutionConfigOptions.UpsertMaterialize.NONE);

        sql("INSERT INTO orders VALUES (1, 'Wang', 'HangZhou')");

        sql(
                "INSERT INTO sub_orders VALUES "
                        + "(1, 1, '12-20', 'Apple', 8000),"
                        + "(1, 2, '12-20', 'Tesla', 400000),"
                        + "(1, 1, '12-21', 'Sangsung', 5000)");

        sEnv.executeSql(widenSql());

        boolean checkResult;
        List<Row> result;

        do {
            Thread.sleep(500);
            result = sql("SELECT * FROM order_wide");
            checkResult =
                    !result.isEmpty()
                            && checkOneRecord(
                                    result.get(0),
                                    1,
                                    "Wang",
                                    "HangZhou",
                                    Row.of(1, "12-20", "Apple", 8000L),
                                    Row.of(1, "12-21", "Sangsung", 5000L),
                                    Row.of(2, "12-20", "Tesla", 400000L));
        } while (!checkResult);

        sql("INSERT INTO sub_orders VALUES (1, 2, '12-20', 'Benz', 380000)");

        do {
            Thread.sleep(500);
            result = sql("SELECT * FROM order_wide");
            checkResult =
                    !result.isEmpty()
                            && checkOneRecord(
                                    result.get(0),
                                    1,
                                    "Wang",
                                    "HangZhou",
                                    Row.of(1, "12-20", "Apple", 8000L),
                                    Row.of(1, "12-21", "Sangsung", 5000L),
                                    Row.of(2, "12-20", "Benz", 380000L));
        } while (!checkResult);
    }

    private String widenSql() {
        return "INSERT INTO order_wide\n"
                + "SELECT order_id, user_name, address, "
                + "CAST (NULL AS ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>) FROM orders\n"
                + "UNION ALL\n"
                + "SELECT order_id, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                + "ARRAY[ROW(daily_id, today, product_name, price)] FROM sub_orders";
    }

    private String widenAppendSql() {
        return "INSERT INTO order_append_wide\n"
                + "SELECT order_id, user_name, address, "
                + "CAST (NULL AS ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>) FROM orders\n"
                + "UNION ALL\n"
                + "SELECT order_id, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                + "ARRAY[ROW(daily_id, today, product_name, price)] FROM sub_orders";
    }

    private boolean checkOneRecord(
            Row record, int orderId, String userName, String address, Row... subOrders) {
        if ((int) record.getField(0) != orderId) {
            return false;
        }
        if (!Objects.equals(record.getFieldAs(1), userName)) {
            return false;
        }
        if (!Objects.equals(record.getFieldAs(2), address)) {
            return false;
        }

        return checkNestedTable(record.getFieldAs(3), subOrders);
    }

    private boolean checkNestedTable(Row[] nestedTable, Row... subOrders) {
        if (nestedTable.length != subOrders.length) {
            return false;
        }

        Comparator<Row> comparator =
                (Comparator)
                        Comparator.comparingInt(r -> ((Row) r).getFieldAs(0))
                                .thenComparing(r -> (String) ((Row) r).getField(1));

        List<Row> sortedActual =
                Arrays.stream(nestedTable).sorted(comparator).collect(Collectors.toList());
        List<Row> sortedExpected =
                Arrays.stream(subOrders).sorted(comparator).collect(Collectors.toList());

        for (int i = 0; i < sortedActual.size(); i++) {
            if (!Objects.equals(sortedActual.get(i), sortedExpected.get(i))) {
                return false;
            }
        }

        return true;
    }
}
