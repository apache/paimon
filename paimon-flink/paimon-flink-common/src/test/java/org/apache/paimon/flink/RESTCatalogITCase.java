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

import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for REST catalog. */
public class RESTCatalogITCase extends CatalogITCaseBase {

    private static final String databaseName = "mydb";
    private static final String tableName = "t1";

    RESTCatalogServer restCatalogServer;
    private String serverUrl;
    protected String warehouse;
    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    public void before() throws IOException {
        String initToken = "init_token";
        warehouse = tempFile.toUri().toString();
        restCatalogServer = new RESTCatalogServer(warehouse, initToken);
        restCatalogServer.start();
        serverUrl = restCatalogServer.getUrl();
        super.before();
        sql(String.format("CREATE DATABASE %s", databaseName));
        sql(String.format("CREATE TABLE %s.%s (a STRING, b DOUBLE)", databaseName, tableName));
    }

    @AfterEach()
    public void after() throws IOException {
        sql(String.format("DROP TABLE  %s.%s", databaseName, tableName));
        sql(String.format("DROP DATABASE %s", databaseName));
        restCatalogServer.shutdown();
    }

    @Test
    public void testCreateTable() {
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", databaseName, tableName));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a` VARCHAR(2147483647),\n"
                                        + "  `b` DOUBLE",
                                databaseName, tableName));
    }

    @Test
    public void testAlterTable() {
        sql(String.format("ALTER TABLE %s.%s ADD e INT AFTER b", databaseName, tableName));
        sql(String.format("ALTER TABLE %s.%s DROP b", databaseName, tableName));
        sql(String.format("ALTER TABLE %s.%s RENAME a TO a1", databaseName, tableName));
        sql(String.format("ALTER TABLE %s.%s MODIFY e DOUBLE", databaseName, tableName));
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", databaseName, tableName));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a1` VARCHAR(2147483647),\n"
                                        + "  `e` DOUBLE",
                                databaseName, tableName));
    }

    @Override
    protected Map<String, String> catalogOptions() {
        String initToken = "init_token";
        Map<String, String> options = new HashMap<>();
        options.put("metastore", "rest");
        options.put(RESTCatalogOptions.URI.key(), serverUrl);
        options.put(RESTCatalogOptions.TOKEN.key(), initToken);
        options.put(RESTCatalogOptions.THREAD_POOL_SIZE.key(), "" + 1);
        return options;
    }

    @Override
    protected String getTempDirPath() {
        return this.warehouse;
    }

    @Override
    protected boolean supportDefineWarehouse() {
        return false;
    }
}
