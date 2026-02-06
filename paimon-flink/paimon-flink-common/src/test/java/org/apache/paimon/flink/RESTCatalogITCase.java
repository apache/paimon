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
import org.apache.paimon.partition.Partition;
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
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.types.DataTypes;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Test
    public void testColumnMasking() {
        String maskingTable = "column_masking_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, secret STRING, email STRING, phone STRING, salary STRING) WITH ('query-auth.enabled' = 'true')",
                        DATABASE_NAME, maskingTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 's1', 'user1@example.com', '12345678901', '50000.0'), (2, 's2', 'user2@example.com', '12345678902', '60000.0')",
                        DATABASE_NAME, maskingTable));

        // Test single column masking
        Transform maskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("****")));
        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("secret", maskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, maskingTable), columnMasking);

        assertThat(batchSql(String.format("SELECT secret FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("****"), Row.of("****"));
        assertThat(batchSql(String.format("SELECT id FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        // Test multiple columns masking
        Transform emailMaskTransform =
                new ConcatTransform(
                        Collections.singletonList(BinaryString.fromString("***@***.com")));
        Transform phoneMaskTransform =
                new ConcatTransform(
                        Collections.singletonList(BinaryString.fromString("***********")));
        Transform salaryMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("0.0")));

        columnMasking.put("email", emailMaskTransform);
        columnMasking.put("phone", phoneMaskTransform);
        columnMasking.put("salary", salaryMaskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, maskingTable), columnMasking);

        assertThat(batchSql(String.format("SELECT email FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("***@***.com"), Row.of("***@***.com"));
        assertThat(batchSql(String.format("SELECT phone FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("***********"), Row.of("***********"));
        assertThat(batchSql(String.format("SELECT salary FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("0.0"), Row.of("0.0"));

        // Test SELECT * with column masking
        List<Row> allRows =
                batchSql(
                        String.format(
                                "SELECT * FROM %s.%s ORDER BY id", DATABASE_NAME, maskingTable));
        assertThat(allRows.size()).isEqualTo(2);
        assertThat(allRows.get(0).getField(1)).isEqualTo("****");
        assertThat(allRows.get(0).getField(2)).isEqualTo("***@***.com");
        assertThat(allRows.get(0).getField(3)).isEqualTo("***********");
        assertThat(allRows.get(0).getField(4)).isEqualTo("0.0");

        // Test WHERE clause with masked column
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT id FROM %s.%s WHERE id = 1",
                                        DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of(1));

        // Test aggregation with masked columns
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s", DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of(2L));

        // Test JOIN with masked columns
        String joinTable = "join_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, name STRING)", DATABASE_NAME, joinTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 'Alice'), (2, 'Bob')",
                        DATABASE_NAME, joinTable));

        List<Row> joinResult =
                batchSql(
                        String.format(
                                "SELECT t1.id, t1.secret, t2.name FROM %s.%s t1 JOIN %s.%s t2 ON t1.id = t2.id ORDER BY t1.id",
                                DATABASE_NAME, maskingTable, DATABASE_NAME, joinTable));
        assertThat(joinResult.size()).isEqualTo(2);
        assertThat(joinResult.get(0).getField(1)).isEqualTo("****");
        assertThat(joinResult.get(0).getField(2)).isEqualTo("Alice");

        // Test UpperTransform
        Transform upperTransform =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(1, "secret", DataTypes.STRING())));
        columnMasking.clear();
        columnMasking.put("secret", upperTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, maskingTable), columnMasking);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT secret FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("S1"), Row.of("S2"));

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
                Identifier.create(DATABASE_NAME, maskingTable), columnMasking);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT secret FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("s1-masked"), Row.of("s2-masked"));

        // Clear masking and verify original data
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, maskingTable), new HashMap<>());
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT secret FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(Row.of("s1"), Row.of("s2"));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT email FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, maskingTable)))
                .containsExactlyInAnyOrder(
                        Row.of("user1@example.com"), Row.of("user2@example.com"));
    }

    @Test
    public void testRowFilter() {
        String filterTable = "row_filter_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, name STRING, age INT, department STRING) WITH ('query-auth.enabled' = 'true')",
                        DATABASE_NAME, filterTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 'Alice', 25, 'IT'), (2, 'Bob', 30, 'HR'), (3, 'Charlie', 35, 'IT'), (4, 'David', 28, 'Finance')",
                        DATABASE_NAME, filterTable));

        // Test single condition row filter (age > 28)
        Predicate agePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(28));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(agePredicate));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(
                        Row.of(2, "Bob", 30, "HR"), Row.of(3, "Charlie", 35, "IT"));

        // Test string condition row filter (department = 'IT')
        Predicate deptPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("IT")));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(deptPredicate));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(
                        Row.of(1, "Alice", 25, "IT"), Row.of(3, "Charlie", 35, "IT"));

        // Test combined conditions (age >= 30 AND department = 'IT')
        Predicate ageGePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        Predicate combinedPredicate = PredicateBuilder.and(ageGePredicate, deptPredicate);
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(combinedPredicate));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(3, "Charlie", 35, "IT"));

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
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(orPredicate));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s.%s ORDER BY id",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(
                        Row.of(1, "Alice", 25, "IT"), Row.of(4, "David", 28, "Finance"));

        // Test WHERE clause combined with row filter
        Predicate ageGt25Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(25));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(ageGt25Predicate));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s.%s WHERE department = 'IT' ORDER BY id",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(3, "Charlie", 35, "IT"));

        // Test JOIN with row filter
        String joinTable = "join_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, salary DOUBLE)", DATABASE_NAME, joinTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 50000.0), (2, 60000.0), (3, 70000.0), (4, 55000.0)",
                        DATABASE_NAME, joinTable));

        Predicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(ageGe30Predicate));

        List<Row> joinResult =
                batchSql(
                        String.format(
                                "SELECT t1.id, t1.name, t1.age, t2.salary FROM %s.%s t1 JOIN %s.%s t2 ON t1.id = t2.id ORDER BY t1.id",
                                DATABASE_NAME, filterTable, DATABASE_NAME, joinTable));
        assertThat(joinResult.size()).isEqualTo(2);
        assertThat(joinResult.get(0)).isEqualTo(Row.of(2, "Bob", 30, 60000.0));
        assertThat(joinResult.get(1)).isEqualTo(Row.of(3, "Charlie", 35, 70000.0));

        // Test column pruning with row filter
        Predicate ageGe28Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(28));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(ageGe28Predicate));

        // Query only id and name, but age column should be automatically included for filtering
        List<Row> pruneResult =
                batchSql(
                        String.format(
                                "SELECT id, name FROM %s.%s ORDER BY id",
                                DATABASE_NAME, filterTable));
        assertThat(pruneResult.size()).isEqualTo(3);
        assertThat(pruneResult.get(0).getField(0)).isEqualTo(2);
        assertThat(pruneResult.get(0).getField(1)).isEqualTo("Bob");
        assertThat(pruneResult.get(1).getField(0)).isEqualTo(3);
        assertThat(pruneResult.get(1).getField(1)).isEqualTo("Charlie");
        assertThat(pruneResult.get(2).getField(0)).isEqualTo(4);
        assertThat(pruneResult.get(2).getField(1)).isEqualTo("David");

        // Test with complex AND predicate - query only id
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(combinedPredicate));

        pruneResult =
                batchSql(
                        String.format(
                                "SELECT id FROM %s.%s ORDER BY id", DATABASE_NAME, filterTable));
        assertThat(pruneResult.size()).isEqualTo(1);
        assertThat(pruneResult.get(0).getField(0)).isEqualTo(3);

        // Test aggregate functions with row filter
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, filterTable),
                Collections.singletonList(ageGe30Predicate));

        // Test COUNT(*) with row filter
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s", DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(2L));

        // Test COUNT(column) with row filter
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(name) FROM %s.%s",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(2L));

        // Test GROUP BY with row filter
        List<Row> groupByResult =
                batchSql(
                        String.format(
                                "SELECT department, COUNT(*) FROM %s.%s GROUP BY department ORDER BY department",
                                DATABASE_NAME, filterTable));
        assertThat(groupByResult.size()).isEqualTo(2);
        assertThat(groupByResult.get(0).getField(0)).isEqualTo("HR");
        assertThat(groupByResult.get(0).getField(1)).isEqualTo(1L);
        assertThat(groupByResult.get(1).getField(0)).isEqualTo("IT");
        assertThat(groupByResult.get(1).getField(1)).isEqualTo(1L);

        // Test HAVING clause with row filter
        List<Row> havingResult =
                batchSql(
                        String.format(
                                "SELECT department, COUNT(*) as cnt FROM %s.%s GROUP BY department HAVING COUNT(*) >= 1 ORDER BY department",
                                DATABASE_NAME, filterTable));
        assertThat(havingResult.size()).isEqualTo(2);

        // Test COUNT DISTINCT with row filter
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(DISTINCT department) FROM %s.%s",
                                        DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(2L));

        // Clear row filter and verify original data
        restCatalogServer.setRowFilterAuth(Identifier.create(DATABASE_NAME, filterTable), null);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s", DATABASE_NAME, filterTable)))
                .containsExactlyInAnyOrder(Row.of(4L));
    }

    @Test
    public void testColumnMaskingAndRowFilter() {
        String combinedTable = "combined_auth_table";
        batchSql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, name STRING, salary STRING, age INT, department STRING) WITH ('query-auth.enabled' = 'true')",
                        DATABASE_NAME, combinedTable));
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 'Alice', '50000.0', 25, 'IT'), (2, 'Bob', '60000.0', 30, 'HR'), (3, 'Charlie', '70000.0', 35, 'IT'), (4, 'David', '55000.0', 28, 'Finance')",
                        DATABASE_NAME, combinedTable));
        Transform salaryMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("salary", salaryMaskTransform);
        Transform nameMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        columnMasking.put("name", nameMaskTransform);
        Predicate deptPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(4, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("IT")));
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, combinedTable), columnMasking);
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, combinedTable),
                Collections.singletonList(deptPredicate));

        // Test both column masking and row filter together
        List<Row> combinedResult =
                batchSql(
                        String.format(
                                "SELECT * FROM %s.%s ORDER BY id", DATABASE_NAME, combinedTable));
        assertThat(combinedResult.size()).isEqualTo(2);
        assertThat(combinedResult.get(0).getField(0)).isEqualTo(1); // id
        assertThat(combinedResult.get(0).getField(1)).isEqualTo("***"); // name masked
        assertThat(combinedResult.get(0).getField(2)).isEqualTo("***"); // salary masked
        assertThat(combinedResult.get(0).getField(3)).isEqualTo(25); // age not masked
        assertThat(combinedResult.get(0).getField(4)).isEqualTo("IT"); // department not masked

        // Test WHERE clause with both features
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT id, name, department FROM %s.%s WHERE age > 30 ORDER BY id",
                                        DATABASE_NAME, combinedTable)))
                .containsExactlyInAnyOrder(Row.of(3, "***", "IT"));

        // Test column pruning with both column masking and row filter
        Predicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        columnMasking.clear();
        columnMasking.put("salary", salaryMaskTransform);
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, combinedTable), columnMasking);
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, combinedTable),
                Collections.singletonList(ageGe30Predicate));

        // Query only id, name and salary (masked)
        List<Row> pruneResult =
                batchSql(
                        String.format(
                                "SELECT id, name, salary FROM %s.%s ORDER BY id",
                                DATABASE_NAME, combinedTable));
        assertThat(pruneResult.size()).isEqualTo(2);
        assertThat(pruneResult.get(0).getField(0)).isEqualTo(2);
        assertThat(pruneResult.get(0).getField(1)).isEqualTo("Bob");
        assertThat(pruneResult.get(0).getField(2)).isEqualTo("***"); // salary is masked
        assertThat(pruneResult.get(1).getField(0)).isEqualTo(3);
        assertThat(pruneResult.get(1).getField(1)).isEqualTo("Charlie");
        assertThat(pruneResult.get(1).getField(2)).isEqualTo("***"); // salary is masked

        // Test aggregate functions with column masking and row filter
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s",
                                        DATABASE_NAME, combinedTable)))
                .containsExactlyInAnyOrder(Row.of(2L));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(name) FROM %s.%s",
                                        DATABASE_NAME, combinedTable)))
                .containsExactlyInAnyOrder(Row.of(2L));

        // Test aggregation on non-masked columns with row filter
        List<Row> deptAggResult =
                batchSql(
                        String.format(
                                "SELECT department, COUNT(*) FROM %s.%s GROUP BY department ORDER BY department",
                                DATABASE_NAME, combinedTable));
        assertThat(deptAggResult.size()).isEqualTo(2);
        assertThat(deptAggResult.get(0).getField(0)).isEqualTo("HR");
        assertThat(deptAggResult.get(0).getField(1)).isEqualTo(1L);
        assertThat(deptAggResult.get(1).getField(0)).isEqualTo("IT");
        assertThat(deptAggResult.get(1).getField(1)).isEqualTo(1L);

        // Test with non-existent column as row filter
        Predicate nonExistentPredicate =
                LeafPredicate.of(
                        new FieldTransform(
                                new FieldRef(10, "non_existent_column", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("value")));
        restCatalogServer.setRowFilterAuth(
                Identifier.create(DATABASE_NAME, combinedTable),
                Collections.singletonList(nonExistentPredicate));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        String.format(
                                                "SELECT id, name FROM %s.%s WHERE age > 30 ORDER BY id",
                                                DATABASE_NAME, combinedTable)))
                .rootCause()
                .hasMessageContaining("Unable to read data without column non_existent_column");

        // Clear both column masking and row filter
        restCatalogServer.setColumnMaskingAuth(
                Identifier.create(DATABASE_NAME, combinedTable), new HashMap<>());
        restCatalogServer.setRowFilterAuth(Identifier.create(DATABASE_NAME, combinedTable), null);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s",
                                        DATABASE_NAME, combinedTable)))
                .containsExactlyInAnyOrder(Row.of(4L));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT name FROM %s.%s WHERE id = 1",
                                        DATABASE_NAME, combinedTable)))
                .containsExactlyInAnyOrder(Row.of("Alice"));
    }
}
