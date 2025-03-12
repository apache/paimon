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

package org.apache.paimon.spark;

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for spark read from Rest catalog. */
public class SparkCatalogWithRestTest {

    private RESTCatalogServer restCatalogServer;
    private String serverUrl;
    private String dataPath;
    private String warehouse;
    @TempDir java.nio.file.Path tempFile;
    private String initToken = "init_token";

    @BeforeEach
    public void before() throws IOException {
        dataPath = tempFile.toUri().toString();
        warehouse = UUID.randomUUID().toString();
        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                CatalogOptions.WAREHOUSE.key(),
                                warehouse),
                        ImmutableMap.of());
        AuthProvider authProvider = new BearTokenAuthProvider(initToken);
        restCatalogServer = new RESTCatalogServer(dataPath, authProvider, config, warehouse);
        restCatalogServer.start();
        serverUrl = restCatalogServer.getUrl();
    }

    @AfterEach()
    public void after() throws Exception {
        restCatalogServer.shutdown();
    }

    @Test
    public void testTable() {
        SparkSession spark =
                SparkSession.builder()
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.metastore", "rest")
                        .config("spark.sql.catalog.paimon.uri", serverUrl)
                        .config("spark.sql.catalog.paimon.token", initToken)
                        .config("spark.sql.catalog.paimon.warehouse", warehouse)
                        .config(
                                "spark.sql.catalog.paimon.token.provider",
                                AuthProviderEnum.BEAR.identifier())
                        .master("local[2]")
                        .getOrCreate();

        spark.sql("CREATE DATABASE paimon.db2");
        spark.sql("USE paimon.db2");
        spark.sql(
                "CREATE TABLE t1 (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'bucket'='4', 'file.format'='avro')");
        assertThat(
                        spark.sql("SHOW TABLES").collectAsList().stream()
                                .map(s -> s.get(1))
                                .map(Object::toString))
                .containsExactlyInAnyOrder("t1");
        spark.sql("DROP TABLE t1");
        assertThat(spark.sql("SHOW TABLES").collectAsList().size() == 0);
        spark.close();
    }
}
