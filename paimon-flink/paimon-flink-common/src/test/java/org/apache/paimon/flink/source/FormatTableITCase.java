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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.flink.RESTCatalogITCaseBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.rest.RESTToken;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for format table. */
public class FormatTableITCase extends RESTCatalogITCaseBase {

    @Test
    public void testDiffFormat() {
        String bigDecimalStr = "10.001";
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr), 8, 3);
        for (String format : new String[] {"parquet", "csv", "json"}) {
            String tableName = "format_table_parquet_" + format.toLowerCase();
            sql(
                    "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) WITH ('file.format'='%s', 'type'='format-table')",
                    tableName, format);
            setDataToken(tableName);
            sql("INSERT INTO %s VALUES (%s, 1, 1), (%s, 2, 2)", tableName, decimal, decimal);
            assertThat(sql("SELECT a, b FROM %s", tableName))
                    .containsExactlyInAnyOrder(
                            Row.of(new BigDecimal(bigDecimalStr), 1),
                            Row.of(new BigDecimal(bigDecimalStr), 2));
            sql("Drop TABLE %s", tableName);
        }
    }

    @Test
    public void testPartitionedTableInsertOverwrite() {

        String ptTableName = "format_table_overwrite";
        sql(
                "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) PARTITIONED BY (c) WITH ('file.format'='parquet', 'type'='format-table')",
                ptTableName);
        setDataToken(ptTableName);

        String ptBigDecimalStr1 = "10.001";
        String ptBigDecimalStr2 = "12.345";
        Decimal ptDecimal1 = Decimal.fromBigDecimal(new BigDecimal(ptBigDecimalStr1), 8, 3);
        Decimal ptDecimal2 = Decimal.fromBigDecimal(new BigDecimal(ptBigDecimalStr2), 8, 3);

        sql(
                "INSERT INTO %s PARTITION (c = 1) VALUES (%s, 10), (%s, 20)",
                ptTableName, ptDecimal1, ptDecimal1);
        sql("INSERT INTO %s PARTITION (c = 2) VALUES (%s, 30)", ptTableName, ptDecimal1);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr1), 10, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 20, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 30, 2));

        sql(
                "INSERT OVERWRITE %s PARTITION (c = 1) VALUES (%s, 100), (%s, 200)",
                ptTableName, ptDecimal2, ptDecimal2);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr2), 100, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr2), 200, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 30, 2));

        sql(
                "INSERT OVERWRITE %s VALUES (%s, 100, 1), (%s, 200, 2)",
                ptTableName, ptDecimal1, ptDecimal2);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr1), 100, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr2), 200, 2));

        sql("Drop TABLE %s", ptTableName);
    }

    @Test
    public void testUnPartitionedTableInsertOverwrite() {
        String tableName = "format_table_overwrite_test";
        String bigDecimalStr1 = "10.001";
        String bigDecimalStr2 = "12.345";
        Decimal decimal1 = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr1), 8, 3);
        Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr2), 8, 3);

        sql(
                "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        setDataToken(tableName);

        sql("INSERT INTO %s VALUES (%s, 1, 1), (%s, 2, 2)", tableName, decimal1, decimal1);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(bigDecimalStr1), 1),
                        Row.of(new BigDecimal(bigDecimalStr1), 2));

        sql("INSERT OVERWRITE %s VALUES (%s, 3, 3), (%s, 4, 4)", tableName, decimal2, decimal2);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(bigDecimalStr2), 3),
                        Row.of(new BigDecimal(bigDecimalStr2), 4));

        sql("Drop TABLE %s", tableName);
    }

    @Test
    public void testTruncateTable() {
        String tableName = "format_table_truncate";
        sql(
                "CREATE TABLE %s (a INT, b INT) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        setDataToken(tableName);

        sql("INSERT INTO %s VALUES (1, 11), (2, 22)", tableName);
        assertThat(sql("SELECT * FROM %s", tableName))
                .containsExactlyInAnyOrder(Row.of(1, 11), Row.of(2, 22));

        assertThat(sql("TRUNCATE TABLE %s", tableName))
                .containsExactly(Row.ofKind(RowKind.INSERT, "OK"));
        assertThat(sql("SELECT * FROM %s", tableName)).isEmpty();

        // The table is empty, not gone: it takes writes again.
        sql("INSERT INTO %s VALUES (3, 33)", tableName);
        assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(Row.of(3, 33));

        sql("DROP TABLE %s", tableName);
    }

    @Test
    public void testTruncatePartitionedTable() throws Exception {
        String tableName = "format_table_truncate_partitioned";
        sql(
                "CREATE TABLE %s (a INT, b INT) PARTITIONED BY (b) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        setDataToken(tableName);

        sql("INSERT INTO %s PARTITION (b = 1) VALUES (10)", tableName);
        sql("INSERT INTO %s PARTITION (b = 2) VALUES (20)", tableName);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(Row.of(10, 1), Row.of(20, 2));

        sql("TRUNCATE TABLE %s", tableName);
        assertThat(sql("SELECT a, b FROM %s", tableName)).isEmpty();

        // Emptied, not dropped: the partition directory of the one nothing was written back to
        // is still there, and all that is left in it is a writer's staging tree.
        Path emptied = new Path(dataPath, "default.db/" + tableName + "/b=2");
        LocalFileIO fileIO = LocalFileIO.create();
        assertThat(fileIO.exists(emptied)).isTrue();
        assertThat(fileIO.listStatus(emptied))
                .extracting(status -> status.getPath().getName())
                .allMatch(name -> name.startsWith("_"));

        // Every partition was emptied, and each of them takes writes again.
        sql("INSERT INTO %s PARTITION (b = 1) VALUES (100)", tableName);
        assertThat(sql("SELECT a, b FROM %s", tableName)).containsExactly(Row.of(100, 1));

        sql("DROP TABLE %s", tableName);
    }

    private void setDataToken(String tableName) {
        restCatalogServer.setDataToken(
                Identifier.create("default", tableName),
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000));
    }
}
