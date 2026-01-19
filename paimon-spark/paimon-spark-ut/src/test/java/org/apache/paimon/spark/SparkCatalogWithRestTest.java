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
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testColumnMasking() {
        spark.sql(
                "CREATE TABLE t_column_masking (id INT, secret STRING, email STRING, phone STRING) TBLPROPERTIES"
                        + " ('query-auth.enabled'='true')");
        spark.sql(
                "INSERT INTO t_column_masking VALUES (1, 's1', 'user1@example.com', '12345678901'), (2, 's2', 'user2@example.com', '12345678902')");

        // Test single column masking
        Transform maskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("****")));

        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("secret", maskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_column_masking"), columnMasking);

        assertThat(spark.sql("SELECT secret FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[****], [****]]");
        assertThat(spark.sql("SELECT id FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[1], [2]]");

        // Test multiple columns masking
        Transform emailMaskTransform =
                new ConcatTransform(
                        Collections.singletonList(BinaryString.fromString("***@***.com")));
        Transform phoneMaskTransform =
                new ConcatTransform(
                        Collections.singletonList(BinaryString.fromString("***********")));

        columnMasking.put("email", emailMaskTransform);
        columnMasking.put("phone", phoneMaskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_column_masking"), columnMasking);

        assertThat(spark.sql("SELECT email FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[***@***.com], [***@***.com]]");
        assertThat(spark.sql("SELECT phone FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[***********], [***********]]");

        // Test SELECT * with column masking
        List<Row> allRows = spark.sql("SELECT * FROM t_column_masking").collectAsList();
        assertThat(allRows.size()).isEqualTo(2);
        assertThat(allRows.get(0).getString(1)).isEqualTo("****");
        assertThat(allRows.get(0).getString(2)).isEqualTo("***@***.com");
        assertThat(allRows.get(0).getString(3)).isEqualTo("***********");

        // Test WHERE clause with masked column
        assertThat(
                        spark.sql("SELECT id FROM t_column_masking WHERE id = 1")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1]]");

        // Test aggregation with masked columns
        assertThat(spark.sql("SELECT COUNT(*) FROM t_column_masking").collectAsList().toString())
                .isEqualTo("[[2]]");

        // Test UpperTransform
        Transform upperTransform =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(1, "secret", DataTypes.STRING())));
        columnMasking.clear();
        columnMasking.put("secret", upperTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_column_masking"), columnMasking);

        assertThat(
                        spark.sql("SELECT secret FROM t_column_masking ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[S1], [S2]]");

        // Test ConcatWsTransform
        Transform concatWsTransform =
                new ConcatWsTransform(
                        Arrays.asList(
                                BinaryString.fromString("-"),
                                new FieldRef(1, "secret", DataTypes.STRING()),
                                BinaryString.fromString("masked")));
        columnMasking.clear();
        columnMasking.put("secret", concatWsTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_column_masking"), columnMasking);

        assertThat(
                        spark.sql("SELECT secret FROM t_column_masking ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[s1-masked], [s2-masked]]");

        // Clear masking and verify original data
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_column_masking"), new HashMap<>());
        assertThat(
                        spark.sql("SELECT secret FROM t_column_masking ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[s1], [s2]]");
        assertThat(
                        spark.sql("SELECT email FROM t_column_masking ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[user1@example.com], [user2@example.com]]");
    }

    @Test
    public void testRowFilter() {
        spark.sql(
                "CREATE TABLE t_row_filter (id INT, name STRING, age INT, department STRING) TBLPROPERTIES"
                        + " ('query-auth.enabled'='true')");
        spark.sql(
                "INSERT INTO t_row_filter VALUES (1, 'Alice', 25, 'IT'), (2, 'Bob', 30, 'HR'), (3, 'Charlie', 35, 'IT'), (4, 'David', 28, 'Finance')");

        // Test single condition row filter (age > 28)
        Predicate agePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(28));
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"), Collections.singletonList(agePredicate));

        assertThat(spark.sql("SELECT * FROM t_row_filter ORDER BY id").collectAsList().toString())
                .isEqualTo("[[2,Bob,30,HR], [3,Charlie,35,IT]]");

        // Test string condition row filter (department = 'IT')
        Predicate deptPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("IT")));
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"), Collections.singletonList(deptPredicate));

        assertThat(spark.sql("SELECT * FROM t_row_filter ORDER BY id").collectAsList().toString())
                .isEqualTo("[[1,Alice,25,IT], [3,Charlie,35,IT]]");

        // Test combined conditions (age >= 30 AND department = 'IT')
        Predicate ageGePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        Predicate combinedPredicate = PredicateBuilder.and(ageGePredicate, deptPredicate);
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"),
                Collections.singletonList(combinedPredicate));

        assertThat(spark.sql("SELECT * FROM t_row_filter ORDER BY id").collectAsList().toString())
                .isEqualTo("[[3,Charlie,35,IT]]");

        // Test OR condition (age < 27 OR department = 'Finance')
        Predicate ageLtPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        LessThan.INSTANCE,
                        Collections.singletonList(27));
        Predicate financePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("Finance")));
        Predicate orPredicate = PredicateBuilder.or(ageLtPredicate, financePredicate);
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"), Collections.singletonList(orPredicate));

        assertThat(spark.sql("SELECT * FROM t_row_filter ORDER BY id").collectAsList().toString())
                .isEqualTo("[[1,Alice,25,IT], [4,David,28,Finance]]");

        // Test WHERE clause combined with row filter
        Predicate ageGt25Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(25));
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"),
                Collections.singletonList(ageGt25Predicate));

        assertThat(
                        spark.sql("SELECT * FROM t_row_filter WHERE department = 'IT' ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[3,Charlie,35,IT]]");

        // Test JOIN with row filter
        spark.sql("CREATE TABLE t_join2 (id INT, salary DOUBLE)");
        spark.sql(
                "INSERT INTO t_join2 VALUES (1, 50000.0), (2, 60000.0), (3, 70000.0), (4, 55000.0)");

        Predicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_row_filter"),
                Collections.singletonList(ageGe30Predicate));

        List<Row> joinResult =
                spark.sql(
                                "SELECT t1.id, t1.name, t1.age, t2.salary FROM t_row_filter t1 JOIN t_join2 t2 ON t1.id = t2.id ORDER BY t1.id")
                        .collectAsList();
        assertThat(joinResult.size()).isEqualTo(2);
        assertThat(joinResult.get(0).toString()).isEqualTo("[2,Bob,30,60000.0]");
        assertThat(joinResult.get(1).toString()).isEqualTo("[3,Charlie,35,70000.0]");

        // Clear row filter and verify original data
        restCatalogServer.setRowFilterAuth(Identifier.create("db2", "t_row_filter"), null);

        assertThat(spark.sql("SELECT COUNT(*) FROM t_row_filter").collectAsList().toString())
                .isEqualTo("[[4]]");
    }

    @Test
    public void testColumnMaskingAndRowFilter() {
        spark.sql(
                "CREATE TABLE t_combined (id INT, name STRING, salary STRING, age INT, department STRING) TBLPROPERTIES"
                        + " ('query-auth.enabled'='true')");
        spark.sql(
                "INSERT INTO t_combined VALUES (1, 'Alice', '50000.0', 25, 'IT'), (2, 'Bob', '60000.0', 30, 'HR'), (3, 'Charlie', '70000.0', 35, 'IT'), (4, 'David', '55000.0', 28, 'Finance')");

        Transform salaryMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        Map<String, Transform> columnMasking = new HashMap<>();
        Predicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));

        // Test both column masking and row filter together
        columnMasking.put("salary", salaryMaskTransform);
        Transform nameMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        columnMasking.put("name", nameMaskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_combined"), columnMasking);
        Predicate deptPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(4, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("IT")));
        restCatalogServer.setRowFilterAuth(
                Identifier.create("db2", "t_combined"), Collections.singletonList(deptPredicate));

        List<Row> combinedResult =
                spark.sql("SELECT * FROM t_combined ORDER BY id").collectAsList();
        assertThat(combinedResult.size()).isEqualTo(2);
        assertThat(combinedResult.get(0).getString(1)).isEqualTo("***"); // name masked
        assertThat(combinedResult.get(0).getString(2)).isEqualTo("***"); // salary masked
        assertThat(combinedResult.get(0).getInt(3)).isEqualTo(25); // age not masked
        assertThat(combinedResult.get(0).getString(4)).isEqualTo("IT"); // department not masked

        // Test must read with row filter columns
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                                "SELECT id, name FROM t_combined WHERE age > 30 ORDER BY id")
                                        .collectAsList())
                .hasMessageContaining("Unable to read data without column department");

        // Test WHERE clause with both features
        assertThat(
                        spark.sql(
                                        "SELECT id, name, department FROM t_combined WHERE age > 30 ORDER BY id")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[3,***,IT]]");

        // Clear both column masking and row filter
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create("db2", "t_combined"), new HashMap<>());
        restCatalogServer.setRowFilterAuth(Identifier.create("db2", "t_combined"), null);

        assertThat(spark.sql("SELECT COUNT(*) FROM t_combined").collectAsList().toString())
                .isEqualTo("[[4]]");
        assertThat(spark.sql("SELECT name FROM t_combined WHERE id = 1").collectAsList().toString())
                .isEqualTo("[[Alice]]");
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
