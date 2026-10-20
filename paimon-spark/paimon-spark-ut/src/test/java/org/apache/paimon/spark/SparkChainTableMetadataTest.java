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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests bucket metadata when Spark reads chain splits, including query authorization. */
public class SparkChainTableMetadataTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBucketMetadata(boolean queryAuth, @TempDir Path warehouse) throws Exception {
        try (SparkSession spark = createSparkSession(warehouse, queryAuth)) {
            spark.sql("CREATE DATABASE spark_test.chain_metadata");
            spark.sql("USE spark_test.chain_metadata");
            spark.sql(
                    "CREATE TABLE T (id INT, payload STRING, seq BIGINT, dt STRING) "
                            + "USING paimon PARTITIONED BY (dt) TBLPROPERTIES ("
                            + "'primary-key'='dt,id', 'bucket-key'='id', 'bucket'='2', "
                            + "'sequence.field'='seq', 'chain-table.enabled'='true', "
                            + "'partition.timestamp-pattern'='$dt', "
                            + "'partition.timestamp-formatter'='yyyyMMdd')");
            spark.sql("CALL spark_test.sys.create_branch('chain_metadata.T', 'snapshot')");
            spark.sql("CALL spark_test.sys.create_branch('chain_metadata.T', 'delta')");
            for (String table : Arrays.asList("T", "T$branch_snapshot", "T$branch_delta")) {
                spark.sql(
                        "ALTER TABLE `"
                                + table
                                + "` SET TBLPROPERTIES ("
                                + "'scan.fallback-snapshot-branch'='snapshot', "
                                + "'scan.fallback-delta-branch'='delta')");
            }
            spark.sql(
                    "INSERT INTO `T$branch_snapshot` "
                            + "SELECT CAST(id AS INT), 'old', 1, '20250810' FROM range(16)");
            spark.sql(
                    "INSERT INTO `T$branch_delta` "
                            + "SELECT CAST(id AS INT), 'new', 2, '20250811' FROM range(4)");

            List<Row> bucketRows =
                    spark.sql("SELECT id, __paimon_bucket FROM `T$branch_snapshot`")
                            .collectAsList();
            assertThat(bucketRows).hasSize(16);
            assertThat(bucketRows.stream().map(row -> row.getInt(1)).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(0, 1);

            if (queryAuth) {
                // Filter on a non-key field after merging: updated rows must not reappear with
                // their old snapshot values, even when payload is absent from the projection.
                for (String name : Arrays.asList("T", "T$branch_snapshot", "T$branch_delta")) {
                    spark.sql(
                            "ALTER TABLE `"
                                    + name
                                    + "` SET TBLPROPERTIES ('query-auth.enabled'='true')");
                }
            }

            // Cover both snapshot-only ChainSplit.from and snapshot + delta chain merging.
            for (String dt : Arrays.asList("20250810", "20250811")) {
                if (queryAuth) {
                    assertQueryAuthChainSplits(spark, dt);
                }
                List<Row> visibleBucketRows =
                        bucketRows.stream()
                                .filter(
                                        row ->
                                                !queryAuth
                                                        || dt.equals("20250810")
                                                        || row.getInt(0) >= 4)
                                .collect(Collectors.toList());
                String predicate = " WHERE dt = '" + dt + "'";
                assertThat(
                                spark.sql("SELECT id, __paimon_bucket FROM T" + predicate)
                                        .collectAsList())
                        .containsExactlyInAnyOrderElementsOf(visibleBucketRows);
                // Partition metadata on chain and authorized splits is a separate concern.
                for (String query :
                        Arrays.asList(
                                "SELECT id, __paimon_partition, __paimon_bucket FROM T" + predicate,
                                "SELECT id FROM T"
                                        + predicate
                                        + " AND __paimon_partition.dt = '"
                                        + dt
                                        + "'")) {
                    assertThatThrownBy(() -> spark.sql(query).collectAsList())
                            .rootCause()
                            .isInstanceOf(UnsupportedOperationException.class)
                            .hasMessageContaining(
                                    "Partition metadata is only supported for DataSplit");
                }

                for (int bucket = 0; bucket < 2; bucket++) {
                    final int selectedBucket = bucket;
                    List<Row> expected =
                            visibleBucketRows.stream()
                                    .filter(row -> row.getInt(1) == selectedBucket)
                                    .map(
                                            row ->
                                                    RowFactory.create(
                                                            row.getInt(0),
                                                            dt.equals("20250811")
                                                                            && row.getInt(0) < 4
                                                                    ? "new"
                                                                    : "old"))
                                    .collect(Collectors.toList());
                    assertThat(
                                    spark.sql(
                                                    "SELECT id, payload FROM T"
                                                            + predicate
                                                            + " AND __paimon_bucket = "
                                                            + bucket)
                                            .collectAsList())
                            .containsExactlyInAnyOrderElementsOf(expected);
                }
            }
            spark.sql("DROP TABLE T");
            spark.sql("USE spark_test.default");
            spark.sql("DROP DATABASE spark_test.chain_metadata");
        }
    }

    private SparkSession createSparkSession(Path warehouse, boolean queryAuth) {
        return SparkSession.builder()
                .master("local[2]")
                .config(
                        "spark.sql.catalog.spark_test",
                        (queryAuth ? RowFilterSparkCatalog.class : SparkCatalog.class).getName())
                .config("spark.sql.catalog.spark_test.warehouse", warehouse.toUri().toString())
                .config("spark.sql.defaultCatalog", "spark_test")
                .config(
                        "spark.sql.extensions",
                        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .getOrCreate();
    }

    private FileStoreTable loadTable(SparkSession spark) throws Exception {
        SparkCatalog catalog =
                (SparkCatalog) spark.sessionState().catalogManager().currentCatalog();
        SparkTable table =
                (SparkTable) catalog.loadTable(Identifier.of(new String[] {"chain_metadata"}, "T"));
        return (FileStoreTable) table.table();
    }

    private void assertQueryAuthChainSplits(SparkSession spark, String dt) throws Exception {
        List<Split> splits =
                loadTable(spark)
                        .newReadBuilder()
                        .withPartitionFilter(Collections.singletonMap("dt", dt))
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits)
                .isNotEmpty()
                .allSatisfy(
                        split -> {
                            assertThat(split).isInstanceOf(FallbackSplit.class);
                            Split wrapped = ((FallbackSplit) split).wrapped();
                            assertThat(wrapped).isInstanceOf(QueryAuthSplit.class);
                            QueryAuthSplit authSplit = (QueryAuthSplit) wrapped;
                            assertThat(authSplit.authResult()).isNotNull();
                            assertThat(authSplit.authResult().hasRules()).isTrue();
                            assertThat(authSplit.split()).isInstanceOf(ChainSplit.class);
                        });
    }

    /** Supplies a row-filter policy while keeping the real filesystem catalog and table reads. */
    public static class RowFilterSparkCatalog extends SparkCatalog {

        @Override
        protected Table loadSparkTable(Identifier identifier, Map<String, String> extraOptions)
                throws NoSuchTableException {
            SparkTable sparkTable = (SparkTable) super.loadSparkTable(identifier, extraOptions);
            FileStoreTable table = (FileStoreTable) sparkTable.table();
            return table.coreOptions().queryAuthEnabled()
                    ? SparkTable$.MODULE$.of(withRowFilter(table))
                    : sparkTable;
        }
    }

    private static FileStoreTable withRowFilter(FileStoreTable table) {
        Predicate rowFilter =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("old"));
        TableQueryAuthResult authResult =
                new TableQueryAuthResult(
                        Collections.singletonList(JsonSerdeUtil.toFlatJson(rowFilter)), null);
        CatalogEnvironment environment = table.catalogEnvironment();
        CatalogLoader loader = environment.catalogLoader();
        CatalogEnvironment authorizedEnvironment =
                new CatalogEnvironment(
                        environment.identifier(),
                        environment.uuid(),
                        authCatalogLoader(loader, authResult),
                        environment.lockFactory(),
                        environment.lockContext(),
                        environment.catalogContext(),
                        false,
                        false);
        return FileStoreTableFactory.create(
                table.fileIO(),
                table.location(),
                table.schema(),
                new Options(table.options()),
                authorizedEnvironment);
    }

    private static CatalogLoader authCatalogLoader(
            CatalogLoader loader, TableQueryAuthResult authResult) {
        return () ->
                new DelegateCatalog(loader.load()) {
                    @Override
                    public TableQueryAuthResult authTableQuery(
                            org.apache.paimon.catalog.Identifier identifier, List<String> select) {
                        return authResult;
                    }

                    @Override
                    public CatalogLoader catalogLoader() {
                        return authCatalogLoader(loader, authResult);
                    }
                };
    }
}
