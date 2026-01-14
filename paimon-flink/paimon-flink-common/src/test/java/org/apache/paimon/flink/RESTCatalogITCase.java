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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.RESTToken;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testFunction() throws Exception {
        Catalog catalog = tEnv.getCatalog("PAIMON").get();
        String functionName = "test_str2";
        String identifier = "com.streaming.flink.udf.StrUdf";
        String jarResourcePath = "xxxx.jar";
        String jarResourcePath2 = "xxxx-yyyyy.jar";
        CatalogFunctionImpl function =
                new CatalogFunctionImpl(
                        identifier,
                        FunctionLanguage.JAVA,
                        ImmutableList.of(
                                new ResourceUri(ResourceType.JAR, jarResourcePath),
                                new ResourceUri(ResourceType.JAR, jarResourcePath2)));
        sql(
                String.format(
                        "CREATE FUNCTION %s.%s AS '%s' LANGUAGE %s USING %s '%s', %s '%s'",
                        DATABASE_NAME,
                        functionName,
                        function.getClassName(),
                        function.getFunctionLanguage(),
                        ResourceType.JAR,
                        jarResourcePath,
                        ResourceType.JAR,
                        jarResourcePath2));
        assertThat(batchSql(String.format("SHOW FUNCTIONS in %s", DATABASE_NAME)))
                .contains(Row.of(functionName));
        ObjectPath functionObjectPath = new ObjectPath(DATABASE_NAME, functionName);
        CatalogFunction getFunction = catalog.getFunction(functionObjectPath);
        assertThat(getFunction).isEqualTo(function);
        identifier = "com.streaming.flink.udf.StrUdf2";
        sql(
                String.format(
                        "ALTER FUNCTION PAIMON.%s.%s AS '%s' LANGUAGE %s",
                        DATABASE_NAME, functionName, identifier, function.getFunctionLanguage()));
        getFunction = catalog.getFunction(functionObjectPath);
        assertThat(getFunction.getClassName()).isEqualTo(identifier);
        sql(String.format("DROP FUNCTION %s.%s", DATABASE_NAME, functionName));
        assertThat(catalog.functionExists(functionObjectPath)).isFalse();
    }

    @Test
    public void testTotalBucketsStatistics() throws Exception {
        String fixedBucketTableName = "fixed_bucket_tbl";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (a INT, b INT, p INT) PARTITIONED BY (p) WITH ('bucket'='2', 'bucket-key'='a')",
                        DATABASE_NAME, fixedBucketTableName));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 10, 1), (2, 20, 1)",
                        DATABASE_NAME, fixedBucketTableName));
        validateTotalBuckets(DATABASE_NAME, fixedBucketTableName, 2);

        String dynamicBucketTableName = "dynamic_bucket_tbl";
        sql(
                String.format(
                        "CREATE TABLE %s.%s (a INT, b INT, p INT) PARTITIONED BY (p) WITH ('bucket'='-1')",
                        DATABASE_NAME, dynamicBucketTableName));
        sql(
                String.format(
                        "INSERT INTO %s.dynamic_bucket_tbl VALUES (1, 10, 1), (2, 20, 1)",
                        DATABASE_NAME));
        validateTotalBuckets(DATABASE_NAME, "dynamic_bucket_tbl", -1);

        String postponeBucketTableName = "postpone_bucket_tbl";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (a INT, b INT, p INT, PRIMARY KEY (p, a) NOT ENFORCED) PARTITIONED BY (p) WITH ('bucket'='-2')",
                        DATABASE_NAME, postponeBucketTableName));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 10, 1), (2, 20, 1)",
                        DATABASE_NAME, postponeBucketTableName));
        validateTotalBuckets(DATABASE_NAME, postponeBucketTableName, 1);
    }

    private void validateTotalBuckets(
            String databaseName, String tableName, Integer expectedTotalBuckets) throws Exception {
        Catalog flinkCatalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        try (org.apache.paimon.catalog.Catalog catalog = ((FlinkCatalog) flinkCatalog).catalog()) {
            List<Partition> partitions =
                    catalog.listPartitions(Identifier.create(databaseName, tableName));
            assertThat(partitions).isNotEmpty();
            assertThat(partitions.get(0).totalBuckets()).isEqualTo(expectedTotalBuckets);
        }
    }
}
