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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.PredicateJsonSerde;

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

import java.util.Collections;
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
    public void testRowFilter() {
        String rowFilterTable = "row_filter_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (col1 INT) WITH ('query-auth.enabled' = 'true')",
                        DATABASE_NAME, rowFilterTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1), (2), (3), (4)",
                        DATABASE_NAME, rowFilterTable));

        // Only allow rows with col1 > 2
        TransformPredicate rowFilterPredicate =
                TransformPredicate.of(
                        new FieldTransform(new FieldRef(0, "col1", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(2));
        restCatalogServer.addTableFilter(
                Identifier.create(DATABASE_NAME, rowFilterTable),
                PredicateJsonSerde.toJsonString(rowFilterPredicate));

        assertThat(batchSql(String.format("SELECT col1 FROM %s.%s", DATABASE_NAME, rowFilterTable)))
                .containsExactlyInAnyOrder(Row.of(3), Row.of(4));
    }

    @Test
    public void testColumnMasking() {
        String maskingTable = "column_masking_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, secret STRING) WITH ('query-auth.enabled' = 'true')",
                        DATABASE_NAME, maskingTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 's1'), (2, 's2')",
                        DATABASE_NAME, maskingTable));

        Transform maskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("****")));
        restCatalogServer.addTableColumnMasking(
                Identifier.create(DATABASE_NAME, maskingTable),
                ImmutableMap.of("secret", maskTransform));

        assertThat(batchSql(String.format("SELECT secret FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("****"), Row.of("****"));
        assertThat(batchSql(String.format("SELECT id FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
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
}
