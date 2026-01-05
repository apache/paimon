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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.PredicateJsonSerde;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private SparkSession spark;

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
        spark =
                SparkSession.builder()
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.metastore", "rest")
                        .config("spark.sql.catalog.paimon.uri", serverUrl)
                        .config("spark.sql.catalog.paimon.token", initToken)
                        .config("spark.sql.catalog.paimon.warehouse", warehouse)
                        .config(
                                "spark.sql.catalog.paimon.token.provider",
                                AuthProviderEnum.BEAR.identifier())
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                        .master("local[2]")
                        .getOrCreate();
        spark.sql("CREATE DATABASE paimon.db2");
        spark.sql("USE paimon.db2");
    }

    @AfterEach()
    public void after() throws Exception {
        restCatalogServer.shutdown();
        spark.close();
    }

    @Test
    public void testTable() {
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
    }

    @Test
    public void testFunction() throws Exception {
        List<DataField> inputParams = new ArrayList<>();
        Catalog paimonCatalog = getPaimonCatalog();
        inputParams.add(new DataField(0, "length", DataTypes.INT()));
        inputParams.add(new DataField(1, "width", DataTypes.INT()));
        List<DataField> returnParams = new ArrayList<>();
        returnParams.add(new DataField(0, "area", DataTypes.BIGINT()));
        String functionName = "area_func";
        FunctionDefinition definition =
                FunctionDefinition.lambda(
                        "(Integer length, Integer width) -> { return (long) length * width; }",
                        "JAVA");
        Identifier identifier = Identifier.create("db2", functionName);
        Function function =
                new FunctionImpl(
                        identifier,
                        inputParams,
                        returnParams,
                        false,
                        ImmutableMap.of(SparkCatalog.FUNCTION_DEFINITION_NAME, definition),
                        null,
                        null);
        paimonCatalog.createFunction(identifier, function, false);
        assertThat(
                        spark.sql(String.format("select paimon.db2.%s(1, 2)", functionName))
                                .collectAsList()
                                .get(0)
                                .toString())
                .isEqualTo("[2]");
        definition =
                FunctionDefinition.lambda(
                        "(Integer length, Integer width) -> { return length * width + 1L; }",
                        "JAVA");
        paimonCatalog.alterFunction(
                identifier,
                ImmutableList.of(
                        FunctionChange.updateDefinition(
                                SparkCatalog.FUNCTION_DEFINITION_NAME, definition)),
                false);
        assertThat(
                        spark.sql(String.format("select paimon.db2.%s(1, 2)", functionName))
                                .collectAsList()
                                .get(0)
                                .toString())
                .isEqualTo("[3]");
        assertThat(spark.sql("show user functions").collectAsList().toString())
                .contains("[paimon.db2.area_func]");

        paimonCatalog.dropFunction(identifier, false);
        cleanFunction(functionName);
    }

    @Test
    public void testArrayFunction() throws Exception {
        List<DataField> inputParams = new ArrayList<>();
        Catalog paimonCatalog = getPaimonCatalog();
        inputParams.add(new DataField(0, "x", DataTypes.ARRAY(DataTypes.INT())));
        List<DataField> returnParams = new ArrayList<>();
        returnParams.add(new DataField(0, "y", DataTypes.INT()));
        String functionName = "test";
        Identifier identifier = Identifier.create("db2", functionName);
        FunctionDefinition definition =
                FunctionDefinition.lambda(
                        "(java.util.List<java.util.List<Integer>> x) -> x.size()", "JAVA");
        Function function =
                new FunctionImpl(
                        identifier,
                        inputParams,
                        returnParams,
                        false,
                        ImmutableMap.of(SparkCatalog.FUNCTION_DEFINITION_NAME, definition),
                        null,
                        null);
        paimonCatalog.createFunction(identifier, function, false);
        assertThat(
                        spark.sql(
                                        String.format(
                                                "select paimon.db2.%s(array(array(1, 2, 3), array(1, 2, 3)))",
                                                functionName))
                                .collectAsList()
                                .get(0)
                                .toString())
                .isEqualTo("[2]");
        cleanFunction(functionName);
    }

    @Test
    public void testMapFunction() throws Exception {
        List<DataField> inputParams = new ArrayList<>();
        Catalog paimonCatalog = getPaimonCatalog();
        inputParams.add(new DataField(0, "x", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())));
        List<DataField> returnParams = new ArrayList<>();
        returnParams.add(new DataField(0, "y", DataTypes.INT()));
        String functionName = "test";
        Identifier identifier = Identifier.create("db2", functionName);
        FunctionDefinition definition =
                FunctionDefinition.lambda(
                        "(java.util.Map<Integer, Integer> x) -> x.size()", "JAVA");
        Function function =
                new FunctionImpl(
                        identifier,
                        inputParams,
                        returnParams,
                        false,
                        ImmutableMap.of(SparkCatalog.FUNCTION_DEFINITION_NAME, definition),
                        null,
                        null);
        paimonCatalog.createFunction(identifier, function, false);
        assertThat(
                        spark.sql(String.format("select paimon.db2.%s(map(1, 2))", functionName))
                                .collectAsList()
                                .get(0)
                                .toString())
                .isEqualTo("[1]");
        cleanFunction(functionName);
    }

    @Test
    public void testRowFilter() {
        spark.sql(
                "CREATE TABLE t_row_filter (col1 INT) TBLPROPERTIES"
                        + " ('bucket'='1', 'bucket-key'='col1', 'file.format'='avro', 'query-auth.enabled'='true')");
        spark.sql("INSERT INTO t_row_filter VALUES (1), (2), (3), (4)");

        // Only allow rows with col1 > 2
        TransformPredicate rowFilterPredicate =
                TransformPredicate.of(
                        new FieldTransform(new FieldRef(0, "col1", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(2));
        restCatalogServer.addTableFilter(
                Identifier.create("db2", "t_row_filter"),
                PredicateJsonSerde.toJsonString(rowFilterPredicate));

        assertThat(spark.sql("SELECT col1 FROM t_row_filter").collectAsList().toString())
                .isEqualTo("[[3], [4]]");
    }

    @Test
    public void testColumnMasking() {
        spark.sql(
                "CREATE TABLE t_column_masking (id INT, secret STRING) TBLPROPERTIES"
                        + " ('bucket'='1', 'bucket-key'='id', 'file.format'='avro', 'query-auth.enabled'='true')");
        spark.sql("INSERT INTO t_column_masking VALUES (1, 's1'), (2, 's2')");

        Transform maskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("****")));
        restCatalogServer.addTableColumnMasking(
                Identifier.create("db2", "t_column_masking"),
                ImmutableMap.of("secret", maskTransform));

        assertThat(spark.sql("SELECT secret FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[****], [****]]");
        assertThat(spark.sql("SELECT id FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[1], [2]]");
    }

    private Catalog getPaimonCatalog() {
        CatalogManager catalogManager = spark.sessionState().catalogManager();
        WithPaimonCatalog withPaimonCatalog = (WithPaimonCatalog) catalogManager.currentCatalog();
        return withPaimonCatalog.paimonCatalog();
    }

    private void cleanFunction(String functionName) {
        String functionFilePath =
                SparkCatalogWithRestTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath()
                        .toString()
                        .replaceAll(
                                "target/test-classes/",
                                JavaLambdaStringToMethodConverter.getSourceFileName(functionName));
        File file = new File(functionFilePath);
        file.delete();
    }
}
