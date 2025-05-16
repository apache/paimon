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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** ITCase for REST catalog. */
class RESTCatalogITCase extends RESTCatalogITCaseBase {

    @Test
    void testCreateTable() {
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", DATABASE_NAME, TABLE_NAME));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a` VARCHAR(2147483647),\n"
                                        + "  `b` DOUBLE",
                                DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testAlterTable() {
        sql(String.format("ALTER TABLE %s.%s ADD e INT AFTER b", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s DROP b", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s RENAME a TO a1", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s MODIFY e DOUBLE", DATABASE_NAME, TABLE_NAME));
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", DATABASE_NAME, TABLE_NAME));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a1` VARCHAR(2147483647),\n"
                                        + "  `e` DOUBLE",
                                DATABASE_NAME, TABLE_NAME));
    }

    @Test
    public void testWriteAndRead() {
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        assertThat(batchSql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, TABLE_NAME)))
                .containsExactlyInAnyOrder(Row.of("1", 11.0D), Row.of("2", 22.0D));
    }

    @Test
    public void testExpiredDataToken() {
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() - 100_000);
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        assertThrows(
                RuntimeException.class,
                () ->
                        batchSql(
                                String.format(
                                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                                        DATABASE_NAME, TABLE_NAME)));
        // update token and retry
        RESTToken dataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 100_000);
        restCatalogServer.setDataToken(identifier, dataToken);
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        assertThat(batchSql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, TABLE_NAME)))
                .containsExactlyInAnyOrder(Row.of("1", 11.0D), Row.of("2", 22.0D));
    }

    @ParameterizedTest
    // TODO add other file types back
    @ValueSource(strings = {"avro"})
    public void testIcebergRESTCatalog(String format) throws Exception {
        String initToken = "init_token";
        String dataPath = tempFile.toUri().toString();
        String restWarehouse = UUID.randomUUID().toString();
        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                RESTTokenFileIO.DATA_TOKEN_ENABLED.key(),
                                "true",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse),
                        ImmutableMap.of());
        AuthProvider authProvider = new BearTokenAuthProvider(initToken);
        RESTCatalogServer restCatalogServer =
                new RESTCatalogServer(dataPath, authProvider, config, restWarehouse);
        restCatalogServer.start();
        String serverUrl = restCatalogServer.getUrl();

        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE paimon.`default`.T (\n"
                        + "  k INT,\n"
                        + "  v MAP<INT, ARRAY<ROW(f1 STRING, f2 INT)>>,\n"
                        + "  v2 BIGINT\n"
                        + ") WITH (\n"
                        + "  'metadata.iceberg.storage' = 'rest-catalog',\n"
                        + "  'metadata.iceberg.uri' = '"
                        + serverUrl
                        + "',\n"
                        + "  'metadata.iceberg.rest-warehouse' = '"
                        + restWarehouse
                        + "',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, MAP[10, ARRAY[ROW('apple', 100), ROW('banana', 101)], 20, ARRAY[ROW('cat', 102), ROW('dog', 103)]], 1000), "
                                + "(2, MAP[10, ARRAY[ROW('cherry', 200), ROW('pear', 201)], 20, ARRAY[ROW('tiger', 202), ROW('wolf', 203)]], 2000)")
                .await();

        tEnv.executeSql(
                "CREATE CATALOG iceberg WITH (\n"
                        + "  'type' = 'iceberg',\n"
                        + "  'catalog-type' = 'rest',\n"
                        + "  'uri' = '"
                        + serverUrl
                        + "',\n"
                        + "  'cache-enabled' = 'false'\n"
                        + ")");
        assertThat(collect(tEnv.executeSql("SELECT k, v[10], v2 FROM iceberg.`default`.T")))
                .containsExactlyInAnyOrder(
                        Row.of(1, new Row[] {Row.of("apple", 100), Row.of("banana", 101)}, 1000L),
                        Row.of(2, new Row[] {Row.of("cherry", 200), Row.of("pear", 201)}, 2000L));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(3, MAP[10, ARRAY[ROW('mango', 300), ROW('watermelon', 301)], 20, ARRAY[ROW('rabbit', 302), ROW('lion', 303)]], 3000)")
                .await();
        assertThat(
                        collect(
                                tEnv.executeSql(
                                        "SELECT k, v[10][2].f1, v2 FROM iceberg.`default`.T WHERE v[20][1].f2 > 200")))
                .containsExactlyInAnyOrder(
                        Row.of(2, "pear", 2000L), Row.of(3, "watermelon", 3000L));
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }
}
