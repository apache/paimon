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

import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Splits;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Date;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests partition metadata projection and query authorization with real Spark reads. */
public class SparkPartitionMetadataAuthTest {

    @ParameterizedTest(name = "chain={0}, policy={1}")
    @CsvSource({
        "false,disabled", "false,no-rules", "false,row-filter", "false,constant", "false,field",
                "false,null",
        "true,disabled", "true,no-rules", "true,row-filter", "true,constant", "true,field",
                "true,null"
    })
    public void testPartitionMetadataWithAuthorization(
            boolean chain, String mode, @TempDir Path warehouse) throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, chain);
            setPolicy(spark, chain, mode, "id,dt,region");
            List<Row> expected = expectedRows(chain, mode);
            assertRows(spark, "SELECT id, dt FROM T", expected);
            assertRows(
                    spark,
                    "SELECT id, dt, __paimon_partition.dt FROM T",
                    expected.stream()
                            .map(row -> RowFactory.create(row.getInt(0), row.get(1), row.get(1)))
                            .collect(Collectors.toList()));
            assertRows(
                    spark,
                    "SELECT __paimon_partition.dt FROM T",
                    expected.stream()
                            .map(row -> RowFactory.create(row.get(1)))
                            .collect(Collectors.toList()));
            assertRows(
                    spark,
                    "SELECT id, __paimon_partition FROM T",
                    expected.stream()
                            .map(
                                    row ->
                                            RowFactory.create(
                                                    row.getInt(0),
                                                    RowFactory.create(
                                                            !chain && row.getInt(0) == 2
                                                                    ? "US"
                                                                    : "CN",
                                                            row.get(1))))
                            .collect(Collectors.toList()));

            if ("null".equals(mode)) {
                // Partition keys are NOT NULL in storage, but masks can null them. Spark must not
                // fold COUNT(column) or IS NULL using the storage schema's nullability.
                assertRows(
                        spark,
                        "SELECT COUNT(dt), COUNT(__paimon_partition.dt), COUNT(*) FROM T",
                        Collections.singletonList(
                                RowFactory.create(0L, 0L, (long) expected.size())));
                assertRows(
                        spark,
                        "SELECT id, CASE WHEN dt IS NULL THEN TRUE ELSE FALSE END, "
                                + "CASE WHEN __paimon_partition.dt IS NULL THEN TRUE ELSE FALSE END FROM T",
                        expected.stream()
                                .map(row -> RowFactory.create(row.getInt(0), true, true))
                                .collect(Collectors.toList()));
            }

            // Metadata predicates must apply to visible values before aggregation or limiting rows.
            String value =
                    "constant".equals(mode) ? "***" : "field".equals(mode) ? "new" : "20250811";
            String predicate = "null".equals(mode) ? " IS NULL" : " = '" + value + "'";
            List<Row> filtered =
                    expected.stream()
                            .filter(
                                    row ->
                                            Objects.equals(
                                                    row.get(1), "null".equals(mode) ? null : value))
                            .map(row -> RowFactory.create(row.getInt(0)))
                            .collect(Collectors.toList());
            String where = " WHERE __paimon_partition.dt" + predicate;
            assertRows(spark, "SELECT id FROM T" + where, filtered);
            assertRows(
                    spark,
                    "SELECT COUNT(*) FROM T" + where,
                    Collections.singletonList(RowFactory.create((long) filtered.size())));
            assertThat(spark.sql("SELECT id FROM T" + where + " LIMIT 1").collectAsList())
                    .hasSize(Math.min(1, filtered.size()))
                    .allSatisfy(row -> assertThat(filtered).contains(row));
            assertRows(
                    spark,
                    "SELECT id FROM T" + where + " ORDER BY id DESC LIMIT 1",
                    filtered.stream()
                            .sorted(
                                    (left, right) ->
                                            Integer.compare(right.getInt(0), left.getInt(0)))
                            .limit(1)
                            .collect(Collectors.toList()));
            assertRows(
                    spark,
                    "SELECT id FROM T WHERE __paimon_partition.dt = 'not-visible'",
                    Collections.emptyList());

            if (Arrays.asList("constant", "field", "null").contains(mode)) {
                assertThatThrownBy(
                                () ->
                                        spark.sql("SELECT id FROM T WHERE dt = '20250811'")
                                                .collectAsList())
                        .hasStackTraceContaining("masked partition key 'dt'");
            }

            if (!"disabled".equals(mode)) {
                // Accessing only dt must not require permission on region, the other partition key.
                // The payload column is a masking-rule dependency, not a user projection.
                setPolicy(spark, chain, mode, "id,dt");
                assertRows(spark, "SELECT id, __paimon_partition.dt FROM T", expected);
                assertRows(
                        spark,
                        "SELECT __paimon_partition.dt FROM T",
                        expected.stream()
                                .map(row -> RowFactory.create(row.get(1)))
                                .collect(Collectors.toList()));
                assertRows(spark, "SELECT id FROM T" + where, filtered);
                assertDenied(spark, "SELECT __paimon_partition FROM T");
                assertDenied(spark, "SELECT payload FROM T");

                setPolicy(spark, chain, mode, "id");
                assertRows(
                        spark,
                        "SELECT id FROM T",
                        expected.stream()
                                .map(row -> RowFactory.create(row.getInt(0)))
                                .collect(Collectors.toList()));
                assertDenied(spark, "SELECT __paimon_partition.dt FROM T");
                assertDenied(spark, "SELECT id FROM T WHERE __paimon_partition.dt = '20250811'");
            }
        }
    }

    @ParameterizedTest(name = "policy={0}")
    @ValueSource(strings = {"constant", "field", "null"})
    public void testJoinOnMaskedPartitionKey(String mode, @TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            // Isolate the static IS NOT NULL predicates inferred from the equality join.
            spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false");
            spark.conf().set("spark.sql.adaptive.enabled", "false");
            createTable(spark, false);
            setPolicy(spark, false, mode, "id,dt,region");
            assertRows(spark, "SELECT id, dt FROM spark_test.default.T", expectedRows(false, mode));

            List<Row> joined = new ArrayList<>();
            if (!"null".equals(mode)) {
                joined.add(RowFactory.create(1, 1));
                joined.add(RowFactory.create(2, 2));
                if ("constant".equals(mode)) {
                    joined.add(RowFactory.create(1, 2));
                    joined.add(RowFactory.create(2, 1));
                }
            }
            assertRows(
                    spark,
                    "SELECT l.id, r.id FROM spark_test.default.T l "
                            + "JOIN spark_test.default.T r ON l.dt = r.dt",
                    joined);

            // The residual must evaluate masked values, even without a join to reject NULLs.
            assertRows(
                    spark,
                    "SELECT id FROM spark_test.default.T WHERE dt IS NOT NULL",
                    "null".equals(mode)
                            ? Collections.emptyList()
                            : Arrays.asList(RowFactory.create(1), RowFactory.create(2)));
        }
    }

    @Test
    public void testJoinOnMaskedPartitionKeyWithoutBroadcastHint(@TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            // Keep AQE and the other DPP settings at their defaults, without a join hint.
            spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true");
            spark.sql(
                    "CREATE TABLE spark_test.default.T (id INT, dt STRING) "
                            + "USING paimon PARTITIONED BY (dt) TBLPROPERTIES "
                            + "('primary-key'='dt,id', 'bucket-key'='id', 'bucket'='1')");
            spark.sql(
                    "INSERT INTO spark_test.default.T VALUES "
                            + "(1,'20250810'),(2,'20250811'),(3,'20250810')");
            spark.sql(
                    "CREATE TABLE spark_test.default.D (dt STRING, k INT) "
                            + "USING paimon TBLPROPERTIES ('bucket'='-1')");
            spark.sql("INSERT INTO spark_test.default.D VALUES ('***',1),('other',0)");
            setPolicy(spark, false, "constant", "id,dt");

            assertThat(
                            spark.sql(
                                            "SELECT l.id FROM spark_test.default.T l "
                                                    + "JOIN spark_test.default.D r ON l.dt = r.dt "
                                                    + "WHERE r.k = 1 ORDER BY l.id")
                                    .collectAsList())
                    .containsExactly(RowFactory.create(1), RowFactory.create(2));
        }
    }

    @ParameterizedTest(name = "chain={0}, policy={1}")
    @CsvSource({
        "false,disabled", "false,no-rules", "false,row-filter", "false,constant", "false,field",
                "false,null", "false,other-column",
        "true,disabled", "true,no-rules", "true,row-filter", "true,constant", "true,field",
                "true,null", "true,other-column"
    })
    public void testDynamicPartitionPruningWithAuthorization(
            boolean chain, String mode, @TempDir Path warehouse) throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, chain);
            assertDynamicPartitionPruning(spark, chain, mode);
        }
    }

    @ParameterizedTest(name = "dataEvolution={0}, policy={1}")
    @CsvSource({
        "false,no-rules", "false,row-filter", "false,constant", "false,field", "false,null",
                "false,other-column",
        "true,no-rules", "true,row-filter", "true,constant", "true,field", "true,null",
                "true,other-column"
    })
    public void testDynamicPartitionPruningForAppendTables(
            boolean dataEvolution, String mode, @TempDir Path warehouse) throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            spark.sql(
                    "CREATE TABLE spark_test.default.T "
                            + "(id INT, payload STRING, seq BIGINT, region STRING, dt STRING) "
                            + "USING paimon PARTITIONED BY (region,dt) TBLPROPERTIES ('bucket'='-1'"
                            + (dataEvolution
                                    ? ", 'row-tracking.enabled'='true', 'data-evolution.enabled'='true'"
                                    : "")
                            + ")");
            spark.sql(
                    "INSERT INTO spark_test.default.T VALUES "
                            + "(1,'old',1,'CN','20250810'),(2,'new',1,'US','20250811'),(3,'hidden',1,'CN','20250810')");
            assertDynamicPartitionPruning(spark, false, mode);
        }
    }

    @Test
    public void testDynamicPartitionPruningWithPartiallyMaskedJoinKeys(@TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, false);
            spark.sql("INSERT INTO spark_test.default.T VALUES (1,'old',1,'EU','20250810')");
            setPolicy(spark, false, "constant", "id,dt,region");
            spark.sql(
                    "CREATE TABLE spark_test.default.D (region STRING, dt STRING, keep INT) "
                            + "USING paimon TBLPROPERTIES ('bucket'='-1')");
            spark.sql("INSERT INTO spark_test.default.D VALUES ('CN','***',1),('US','other',0)");
            String query =
                    "SELECT /*+ BROADCAST(r) */ l.id, l.dt FROM spark_test.default.T l "
                            + "JOIN spark_test.default.D r ON l.region = r.region AND l.dt = r.dt "
                            + "WHERE r.keep = 1 AND l.region <> 'EU'";
            for (boolean dpp : new boolean[] {false, true}) {
                spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", dpp);
                for (boolean adaptive : new boolean[] {false, true}) {
                    spark.conf().set("spark.sql.adaptive.enabled", adaptive);
                    Dataset<Row> result = spark.sql(query);
                    PaimonBaseScan scan = dataScan(result);
                    assertThat(scan.filterAttributes())
                            .extracting(reference -> reference.fieldNames()[0])
                            .containsExactly("region");
                    // The static predicate removes EU; runtime pruning can still remove US
                    // using region, while the join must compare the visible masked dt values.
                    assertThat(scan.inputSplits()).hasSize(2);
                    assertThat(result.collectAsList()).containsExactly(RowFactory.create(1, "***"));
                    assertThat(scan.inputSplits()).hasSize(dpp ? 1 : 2);
                    assertFileMetrics(scan, dpp ? 1 : 2, dpp ? 2 : 1);
                }
            }
        }
    }

    @Test
    public void testDynamicPartitionPruningWithBucketedScan(@TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, false);
            setPolicy(spark, false, "no-rules", "id,dt,region");
            spark.conf().set("spark.paimon.scan.preserve-data-grouping", "true");
            spark.conf().set("spark.sql.sources.v2.bucketing.enabled", "true");
            spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true");
            spark.sql(
                    "CREATE TABLE spark_test.default.D (dt STRING, keep INT) "
                            + "USING paimon TBLPROPERTIES ('bucket'='-1')");
            spark.sql("INSERT INTO spark_test.default.D VALUES ('20250810',1),('absent',2)");
            for (boolean adaptive : new boolean[] {false, true}) {
                spark.conf().set("spark.sql.adaptive.enabled", adaptive);
                for (int keep : new int[] {1, 2}) {
                    Dataset<Row> result =
                            spark.sql(
                                    "SELECT /*+ BROADCAST(r) */ l.id, l.dt FROM spark_test.default.T l "
                                            + "JOIN spark_test.default.D r ON l.dt = r.dt WHERE r.keep = "
                                            + keep);
                    PaimonBaseScan scan = dataScan(result);
                    assertThat(scan.inputSplits()).hasSize(2);
                    // Spark 3.2 does not support reporting grouped V2 partitions.
                    assertThat(scan.inputPartitions().head().bucketed())
                            .isEqualTo(!spark.version().startsWith("3.2."));
                    assertThat(result.collectAsList())
                            .containsExactlyInAnyOrderElementsOf(
                                    keep == 1
                                            ? Arrays.asList(
                                                    RowFactory.create(1, "20250810"),
                                                    RowFactory.create(3, "20250810"))
                                            : Collections.emptyList());
                    assertThat(scan.inputSplits()).hasSize(keep == 1 ? 1 : 0);
                    assertThat(scan.inputPartitions().size()).isEqualTo(keep == 1 ? 1 : 0);
                    assertFileMetrics(scan, keep == 1 ? 1 : 0, keep == 1 ? 1 : 2);
                }
            }
        }
    }

    private static void assertDynamicPartitionPruning(
            SparkSession spark, boolean chain, String mode) {
        setPolicy(spark, chain, mode, "id,dt,region");
        String joinKey =
                "constant".equals(mode) ? "***" : "field".equals(mode) ? "new" : "20250810";
        spark.sql(
                "CREATE TABLE spark_test.default.D (dt STRING, keep INT) "
                        + "USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO spark_test.default.D VALUES ('" + joinKey + "',1),('other',0)");
        String query =
                "SELECT /*+ BROADCAST(r) */ l.id, l.dt FROM spark_test.default.T l "
                        + "JOIN spark_test.default.D r ON l.dt = r.dt WHERE r.keep = 1";
        List<Row> expected =
                expectedRows(chain, mode).stream()
                        .filter(row -> Objects.equals(row.get(1), joinKey))
                        .collect(Collectors.toList());

        for (boolean dpp : new boolean[] {false, true}) {
            spark.conf()
                    .set(
                            "spark.sql.optimizer.dynamicPartitionPruning.enabled",
                            String.valueOf(dpp));
            for (boolean adaptive : new boolean[] {false, true}) {
                spark.conf().set("spark.sql.adaptive.enabled", String.valueOf(adaptive));
                Dataset<Row> result = spark.sql(query);
                PaimonBaseScan scan = dataScan(result);
                int plannedSplits = scan.inputSplits().length;
                assertThat(result.collectAsList())
                        .as("DPP=%s, AQE=%s", dpp, adaptive)
                        .containsExactlyInAnyOrderElementsOf(expected);
                boolean canPrune = !Arrays.asList("constant", "field", "null").contains(mode);
                if (dpp && canPrune) {
                    assertThat(scan.inputSplits().length)
                            .as("DPP must prune splits, AQE=%s, policy=%s", adaptive, mode)
                            .isLessThan(plannedSplits);
                    if (!"disabled".equals(mode)) {
                        // An append split can contain files from multiple writers.
                        long remainingFiles =
                                chain
                                        ? 1
                                        : Arrays.stream(scan.inputSplits())
                                                .map(Splits::underlying)
                                                .map(DataSplit.class::cast)
                                                .mapToLong(split -> split.dataFiles().size())
                                                .sum();
                        // The newer chain split also reads the older snapshot file.
                        assertFileMetrics(scan, remainingFiles, chain ? 2 : 1);
                    }
                }
                // Only a mask on the join key prevents runtime partition pruning.
                assertThat(
                                result.queryExecution()
                                        .optimizedPlan()
                                        .toString()
                                        .contains("dynamicpruning"))
                        .as("DPP=%s, AQE=%s", dpp, adaptive)
                        .isEqualTo(
                                dpp && !Arrays.asList("constant", "field", "null").contains(mode));
            }
        }

        if (!"disabled".equals(mode)) {
            setPolicy(spark, chain, mode, "id");
            assertDenied(spark, query);
        }
    }

    private static void assertFileMetrics(PaimonBaseScan scan, long resulted, long skipped) {
        // Reporting again must not count the runtime-pruned files a second time.
        for (int i = 0; i < 2; i++) {
            // JMX must already reflect pruning without depending on Spark's driver reporting.
            assertJmxFileMetric(scan.table().name(), "lastScanResultedTableFiles", resulted);
            assertJmxFileMetric(scan.table().name(), "lastScanSkippedTableFiles", skipped);
            Map<String, Long> metrics =
                    Arrays.stream(scan.reportDriverMetrics())
                            .collect(
                                    Collectors.toMap(
                                            metric -> metric.name(), metric -> metric.value()));
            assertThat(metrics)
                    .containsEntry("resultedTableFiles", resulted)
                    .containsEntry("skippedTableFiles", skipped);
        }
    }

    private static void assertJmxFileMetric(String table, String metric, long expected) {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            String path = "scan.table." + table + "." + metric;
            ObjectName name =
                    server.queryNames(new ObjectName("paimon:*"), null).stream()
                            .filter(candidate -> candidate.toString().contains(path))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Missing JMX metric: " + path));
            assertThat(server.getAttribute(name, "Value")).as(path).isEqualTo(expected);
        } catch (JMException e) {
            throw new AssertionError("Cannot read JMX file metric", e);
        }
    }

    private static PaimonBaseScan dataScan(Dataset<Row> result) {
        ArrayDeque<LogicalPlan> pending = new ArrayDeque<>();
        pending.add(result.queryExecution().optimizedPlan());
        while (!pending.isEmpty()) {
            LogicalPlan plan = pending.removeFirst();
            if (plan instanceof DataSourceV2ScanRelation) {
                org.apache.spark.sql.connector.read.Scan scan =
                        ((DataSourceV2ScanRelation) plan).scan();
                if (scan instanceof PaimonBaseScan
                        && "T".equals(((PaimonBaseScan) scan).table().name())) {
                    return (PaimonBaseScan) scan;
                }
            }
            scala.collection.Iterator<LogicalPlan> children = plan.children().iterator();
            while (children.hasNext()) {
                pending.add(children.next());
            }
        }
        throw new AssertionError(
                "Missing Paimon scan for T: " + result.queryExecution().optimizedPlan());
    }

    @ParameterizedTest(name = "chain={0}, nestedPruning={1}")
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testPartitionMetadataAuthorizationWithNestedSchemaPruning(
            boolean chain, boolean pruning, @TempDir Path warehouse) throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, chain);
            String pruningOption = "spark.sql.optimizer.nestedSchemaPruning.enabled";
            String originalPruning = spark.conf().get(pruningOption);
            try {
                spark.conf().set(pruningOption, Boolean.toString(pruning));
                for (String mode :
                        Arrays.asList(
                                "disabled",
                                "no-rules",
                                "row-filter",
                                "constant",
                                "field",
                                "null")) {
                    // Without nested pruning, Spark requests the complete partition struct.
                    // Reading only its dt subfield therefore also requires access to region.
                    setPolicy(spark, chain, mode, pruning ? "id,dt" : "id,dt,region");
                    List<Row> expected = expectedRows(chain, mode);
                    assertRows(
                            spark,
                            "SELECT id, __paimon_partition.dt FROM spark_test.default.T",
                            expected);
                    assertRows(
                            spark,
                            "SELECT __paimon_partition.dt FROM spark_test.default.T",
                            expected.stream()
                                    .map(row -> RowFactory.create(row.get(1)))
                                    .collect(Collectors.toList()));
                    assertRows(
                            spark,
                            "SELECT id, p.dt FROM (SELECT id, __paimon_partition AS p "
                                    + "FROM spark_test.default.T) s",
                            expected);

                    // Metadata-only predicates remain residual filters over visible values.
                    String value =
                            "constant".equals(mode)
                                    ? "***"
                                    : "field".equals(mode) ? "new" : "20250811";
                    String predicate = "null".equals(mode) ? " IS NULL" : " = '" + value + "'";
                    List<Row> filtered =
                            expected.stream()
                                    .filter(
                                            row ->
                                                    Objects.equals(
                                                            row.get(1),
                                                            "null".equals(mode) ? null : value))
                                    .map(row -> RowFactory.create(row.getInt(0)))
                                    .collect(Collectors.toList());
                    String where = " WHERE __paimon_partition.dt" + predicate;
                    assertRows(spark, "SELECT id FROM spark_test.default.T" + where, filtered);
                    assertRows(
                            spark,
                            "SELECT COUNT(*) FROM spark_test.default.T" + where,
                            Collections.singletonList(RowFactory.create((long) filtered.size())));
                    assertThat(
                                    spark.sql(
                                                    "SELECT id FROM spark_test.default.T"
                                                            + where
                                                            + " LIMIT 1")
                                            .collectAsList())
                            .hasSize(Math.min(1, filtered.size()))
                            .allSatisfy(row -> assertThat(filtered).contains(row));

                    if ("constant".equals(mode)) {
                        // Columns added only for metadata must not be advertised as ordinary
                        // runtime-filter attributes when Spark plans the join.
                        List<Row> joined = new ArrayList<>();
                        for (Row left : expected) {
                            for (Row right : expected) {
                                if (left.getInt(0) == right.getInt(0)) {
                                    joined.add(RowFactory.create(left.getInt(0), "***", "***"));
                                }
                            }
                        }
                        assertRows(
                                spark,
                                "SELECT l.id, l.__paimon_partition.dt, r.__paimon_partition.dt "
                                        + "FROM spark_test.default.T l JOIN spark_test.default.T r ON l.id = r.id",
                                joined);
                        assertRows(
                                spark,
                                "SELECT __paimon_partition.dt, COUNT(*) FROM spark_test.default.T "
                                        + "GROUP BY __paimon_partition.dt",
                                Collections.singletonList(
                                        RowFactory.create("***", (long) expected.size())));
                    }

                    if (!"disabled".equals(mode)) {
                        setPolicy(spark, chain, mode, "id,dt");
                        assertRows(spark, "SELECT id, dt FROM spark_test.default.T", expected);
                        // The complete struct and explicit references to region require its
                        // permission, regardless of the nested-pruning setting.
                        assertDenied(spark, "SELECT __paimon_partition FROM spark_test.default.T");
                        assertDenied(
                                spark,
                                "SELECT __paimon_partition, __paimon_partition.dt FROM spark_test.default.T");
                        assertDenied(
                                spark,
                                "SELECT id FROM spark_test.default.T WHERE __paimon_partition.region = 'CN'");
                        if (!pruning) {
                            // Spark also requests region for these dt-only metadata queries.
                            assertDenied(
                                    spark,
                                    "SELECT id, __paimon_partition.dt FROM spark_test.default.T");
                            assertDenied(
                                    spark,
                                    "SELECT __paimon_partition.dt FROM spark_test.default.T");
                            assertDenied(spark, "SELECT id FROM spark_test.default.T" + where);
                        }
                        if ("constant".equals(mode)) {
                            assertDenied(
                                    spark,
                                    "SELECT l.__paimon_partition.dt, r.__paimon_partition.region "
                                            + "FROM spark_test.default.T l JOIN spark_test.default.T r ON l.id = r.id");
                        }
                        setPolicy(spark, chain, mode, "id");
                        assertRows(
                                spark,
                                "SELECT id FROM spark_test.default.T",
                                expected.stream()
                                        .map(row -> RowFactory.create(row.getInt(0)))
                                        .collect(Collectors.toList()));
                        assertDenied(
                                spark, "SELECT __paimon_partition.dt FROM spark_test.default.T");
                        assertDenied(spark, "SELECT id FROM spark_test.default.T" + where);
                    }
                }
            } finally {
                spark.conf().set(pruningOption, originalPruning);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"scan.fallback-branch", "scan.primary-branch"})
    public void testFallbackPartitionMetadata(String option, @TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            createTable(spark, false);
            spark.sql("CALL spark_test.sys.create_branch('default.T', 'backup')");
            spark.sql(
                    "INSERT INTO `T$branch_backup` VALUES "
                            + "(1,'backup',1,'CN','20250810'),(2,'backkeep',1,'EU','20250812')");
            spark.sql("ALTER TABLE T SET TBLPROPERTIES ('" + option + "'='backup')");
            List<Row> expected =
                    new ArrayList<>(
                            Arrays.asList(
                                    RowFactory.create(1, "20250810"),
                                    RowFactory.create(2, "20250811"),
                                    RowFactory.create(2, "20250812")));
            if ("scan.fallback-branch".equals(option)) {
                expected.add(RowFactory.create(3, "20250810"));
            }
            assertRows(spark, "SELECT id, dt FROM T", expected);
            assertRows(spark, "SELECT id, __paimon_partition.dt FROM T", expected);
            for (String table : Arrays.asList("T", "T$branch_backup")) {
                spark.sql(
                        "ALTER TABLE `"
                                + table
                                + "` SET TBLPROPERTIES "
                                + "('query-auth.enabled'='true', 'test.auth-mode'='field', 'test.allowed'='id,dt')");
            }
            assertRows(
                    spark,
                    "SELECT id, __paimon_partition.dt FROM T",
                    Arrays.asList(
                            RowFactory.create(
                                    1, "scan.primary-branch".equals(option) ? "backup" : "old"),
                            RowFactory.create(2, "new"),
                            RowFactory.create(2, "backkeep")));
        }
    }

    @Test
    public void testAppendPartitionMetadataWithTypedAndNullValues(@TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            spark.sql(
                    "CREATE TABLE spark_test.default.append_metadata (id INT, p INT, d DATE) "
                            + "USING paimon PARTITIONED BY (d,p) TBLPROPERTIES ('bucket'='-1')");
            spark.sql(
                    "INSERT INTO spark_test.default.append_metadata VALUES "
                            + "(1,7,DATE '2026-10-27'),(2,NULL,NULL)");
            Date date = Date.valueOf("2026-10-27");

            // Metadata-only reads must preserve types and stored NULLs, even when the requested
            // field order differs from the partition order.
            assertRows(
                    spark,
                    "SELECT __paimon_partition.p, __paimon_partition.d "
                            + "FROM spark_test.default.append_metadata",
                    Arrays.asList(RowFactory.create(7, date), RowFactory.create(null, null)));
            assertRows(
                    spark,
                    "SELECT id, p, d, __paimon_partition FROM spark_test.default.append_metadata",
                    Arrays.asList(
                            RowFactory.create(1, 7, date, RowFactory.create(date, 7)),
                            RowFactory.create(2, null, null, RowFactory.create(null, null))));

            // Partition fields added to the read type must not shift file and row-index metadata
            // in the Spark output when those ordinary partition fields are not selected.
            assertRows(
                    spark,
                    "SELECT id, __paimon_partition.p, __paimon_partition.d, "
                            + "__paimon_file_path IS NOT NULL, __paimon_row_index >= 0 "
                            + "FROM spark_test.default.append_metadata",
                    Arrays.asList(
                            RowFactory.create(1, 7, date, true, true),
                            RowFactory.create(2, null, null, true, true)));
            assertRows(
                    spark,
                    "SELECT id FROM spark_test.default.append_metadata "
                            + "WHERE __paimon_partition.p IS NULL AND __paimon_partition.d IS NULL",
                    Collections.singletonList(RowFactory.create(2)));
        }
    }

    @Test
    public void testBlobDescriptorsWithPartitionMetadata(@TempDir Path warehouse)
            throws IOException {
        try (SparkSession spark = createSession(warehouse)) {
            spark.sql(
                    "CREATE TABLE blob_source (id INT, image BINARY COMMENT '__BLOB_FIELD', dt STRING) "
                            + "USING paimon PARTITIONED BY (dt) TBLPROPERTIES ('bucket'='-1', "
                            + "'row-tracking.enabled'='true', 'data-evolution.enabled'='true')");
            spark.sql(
                    "INSERT INTO blob_source VALUES (1, X'010203', '20250811'), (2, NULL, '20250812')");
            spark.sql(
                    "CREATE TABLE blob_target (id INT, image BINARY COMMENT '__BLOB_DESCRIPTOR_FIELD', dt STRING) "
                            + "USING paimon PARTITIONED BY (dt) TBLPROPERTIES ('bucket'='-1', "
                            + "'row-tracking.enabled'='true', 'data-evolution.enabled'='true')");
            spark.sql("ALTER TABLE blob_source SET TBLPROPERTIES ('blob-as-descriptor'='true')");
            spark.sql("INSERT INTO blob_target SELECT id, image, dt FROM blob_source");

            // Adding dt for metadata must preserve descriptor resolution and the output layout.
            assertRows(
                    spark,
                    "SELECT HEX(image), __paimon_partition.dt FROM blob_target",
                    Arrays.asList(
                            RowFactory.create("010203", "20250811"),
                            RowFactory.create(null, "20250812")));
            assertRows(
                    spark,
                    "SELECT id, HEX(image), dt, __paimon_partition.dt FROM blob_target",
                    Arrays.asList(
                            RowFactory.create(1, "010203", "20250811", "20250811"),
                            RowFactory.create(2, null, "20250812", "20250812")));
        }
    }

    private static List<Row> expectedRows(boolean chain, String mode) {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, visible(mode, "20250810", "old")));
        rows.add(
                RowFactory.create(
                        2, visible(mode, chain ? "20250810" : "20250811", chain ? "keep" : "new")));
        if (chain) {
            rows.add(RowFactory.create(1, visible(mode, "20250811", "new")));
            rows.add(RowFactory.create(2, visible(mode, "20250811", "keep")));
        }
        if ("disabled".equals(mode) || "no-rules".equals(mode)) {
            rows.add(RowFactory.create(3, "20250810"));
            if (chain) {
                rows.add(RowFactory.create(3, "20250811"));
            }
        }
        return rows;
    }

    private static String visible(String mode, String partition, String payload) {
        switch (mode) {
            case "constant":
                return "***";
            case "field":
                return payload;
            case "null":
                return null;
            default:
                return partition;
        }
    }

    private static void assertRows(SparkSession spark, String sql, List<Row> expected) {
        assertThat(spark.sql(sql).collectAsList())
                .as(sql)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void assertDenied(SparkSession spark, String sql) {
        assertThatThrownBy(() -> spark.sql(sql).collectAsList())
                .hasStackTraceContaining("Denied columns");
    }

    private static SparkSession createSession(Path warehouse) {
        return SparkSession.builder()
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.catalog.spark_test", PolicySparkCatalog.class.getName())
                .config("spark.sql.catalog.spark_test.warehouse", warehouse.toUri().toString())
                .config("spark.sql.defaultCatalog", "spark_test")
                .config(
                        "spark.sql.extensions",
                        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .getOrCreate();
    }

    private static void createTable(SparkSession spark, boolean chain) {
        spark.sql(
                "CREATE TABLE spark_test.default.T (id INT, payload STRING, seq BIGINT, region STRING, dt STRING) "
                        + "USING paimon PARTITIONED BY (region,dt) TBLPROPERTIES ("
                        + "'primary-key'='region,dt,id', 'bucket-key'='id', 'bucket'='1', 'sequence.field'='seq'"
                        + (chain
                                ? ", 'chain-table.enabled'='true', 'chain-table.chain-partition-keys'='dt', "
                                        + "'partition.timestamp-pattern'='$dt', 'partition.timestamp-formatter'='yyyyMMdd'"
                                : "")
                        + ")");
        if (chain) {
            spark.sql("CALL spark_test.sys.create_branch('default.T', 'snapshot')");
            spark.sql("CALL spark_test.sys.create_branch('default.T', 'delta')");
            for (String table : tableNames(true)) {
                spark.sql(
                        "ALTER TABLE `"
                                + table
                                + "` SET TBLPROPERTIES ("
                                + "'scan.fallback-snapshot-branch'='snapshot', 'scan.fallback-delta-branch'='delta')");
            }
            spark.sql(
                    "INSERT INTO `T$branch_snapshot` VALUES "
                            + "(1,'old',1,'CN','20250810'),(2,'keep',1,'CN','20250810'),(3,'hidden',1,'CN','20250810')");
            spark.sql("INSERT INTO `T$branch_delta` VALUES (1,'new',2,'CN','20250811')");
        } else {
            spark.sql(
                    "INSERT INTO T VALUES "
                            + "(1,'old',1,'CN','20250810'),(2,'new',1,'US','20250811'),(3,'hidden',1,'CN','20250810')");
        }
    }

    private static List<String> tableNames(boolean chain) {
        return chain
                ? Arrays.asList("T", "T$branch_snapshot", "T$branch_delta")
                : Collections.singletonList("T");
    }

    private static void setPolicy(SparkSession spark, boolean chain, String mode, String allowed) {
        for (String table : tableNames(chain)) {
            spark.sql(
                    "ALTER TABLE `"
                            + table
                            + "` SET TBLPROPERTIES ('query-auth.enabled'='"
                            + !"disabled".equals(mode)
                            + "', 'test.auth-mode'='"
                            + mode
                            + "', 'test.allowed'='"
                            + allowed
                            + "')");
        }
    }

    /** Provides policies while preserving real table planning, merge reads and Spark execution. */
    public static class PolicySparkCatalog extends SparkCatalog {
        @Override
        protected Table loadSparkTable(Identifier identifier, Map<String, String> extraOptions)
                throws NoSuchTableException {
            SparkTable sparkTable = (SparkTable) super.loadSparkTable(identifier, extraOptions);
            FileStoreTable table = (FileStoreTable) sparkTable.table();
            if (!table.coreOptions().queryAuthEnabled()) {
                return sparkTable;
            }
            String mode = table.options().get("test.auth-mode");
            List<String> allowed = Arrays.asList(table.options().get("test.allowed").split(","));
            List<String> filters =
                    "no-rules".equals(mode)
                            ? null
                            : Collections.singletonList(
                                    JsonSerdeUtil.toFlatJson(
                                            new PredicateBuilder(table.rowType()).lessThan(0, 3)));
            Transform mask = null;
            if ("field".equals(mode)) {
                mask = new FieldTransform(new FieldRef(1, "payload", DataTypes.STRING()));
            } else if ("constant".equals(mode)
                    || "null".equals(mode)
                    || "other-column".equals(mode)) {
                mask =
                        new ConcatTransform(
                                Collections.singletonList(
                                        "null".equals(mode)
                                                ? null
                                                : BinaryString.fromString("***")));
            }
            TableQueryAuthResult rules =
                    new TableQueryAuthResult(
                            filters,
                            mask == null
                                    ? null
                                    : Collections.singletonMap(
                                            "other-column".equals(mode) ? "payload" : "dt",
                                            JsonSerdeUtil.toFlatJson(mask)));
            CatalogEnvironment env = table.catalogEnvironment();
            CatalogEnvironment authorized =
                    new CatalogEnvironment(
                            env.identifier(),
                            env.uuid(),
                            authLoader(env.catalogLoader(), rules, allowed),
                            env.lockFactory(),
                            env.lockContext(),
                            env.catalogContext(),
                            false,
                            false);
            return SparkTable$.MODULE$.of(
                    FileStoreTableFactory.create(
                            table.fileIO(),
                            table.location(),
                            table.schema(),
                            new Options(table.options()),
                            authorized));
        }
    }

    private static CatalogLoader authLoader(
            CatalogLoader loader, TableQueryAuthResult rules, List<String> allowed) {
        return () ->
                new DelegateCatalog(loader.load()) {
                    @Override
                    public TableQueryAuthResult authTableQuery(
                            org.apache.paimon.catalog.Identifier identifier, List<String> select) {
                        if (select == null || !allowed.containsAll(select)) {
                            throw new SecurityException(
                                    "Denied columns: " + select + "; allowed: " + allowed);
                        }
                        return rules;
                    }

                    @Override
                    public CatalogLoader catalogLoader() {
                        return authLoader(loader, rules, allowed);
                    }
                };
    }
}
