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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the lake-stream routing against a real Fluss cluster that Paimon reaches only through
 * Flink's {@code CatalogFactory} SPI. Paimon compiles against no Fluss class, so what this test
 * exercises is what a user gets from a published Fluss release.
 *
 * <p>It covers what the fake store in {@link LakeStreamRoutingITCase} cannot: that the discovered
 * Fluss catalog accepts the options forwarded under the {@code fluss.} prefix, that a missing
 * required store option surfaces as a configuration error, and that reads of the {@code $rt} name
 * are served by the Fluss source, query hints included.
 *
 * <p>A Fluss log table is unbounded, so the {@code $rt} reads pin a bounded scan window to keep the
 * assertions deterministic.
 */
public class LakeStreamFlussITCase {

    private static final String STORE_IDENTIFIER = "fluss";
    private static final String DATABASE = FlussClusterExtension.BUILTIN_DATABASE;

    /** Reads the log as it stands when the query starts, instead of tailing it forever. */
    private static final String BOUNDED_SCAN =
            "/*+ OPTIONS('scan.startup.mode' = 'earliest', "
                    + "'scan.bounded.mode' = 'latest-offset') */";

    @RegisterExtension
    static final FlussClusterExtension FLUSS =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @BeforeEach
    public void beforeEach() {
        catalog = newCatalog(withStoreConnection(baseOptions()));
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private Options baseOptions() {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        conf.setString(CatalogOptions.LAKE_STREAM_STORE.key(), STORE_IDENTIFIER);
        return conf;
    }

    private Options withStoreConnection(Options conf) {
        conf.setString(STORE_IDENTIFIER + ".bootstrap.servers", FLUSS.getBootstrapServers());
        return conf;
    }

    private Catalog newCatalog(Options conf) {
        return new FlinkCatalogFactory()
                .createCatalog(
                        new FactoryUtil.DefaultCatalogContext(
                                "paimon",
                                conf.toMap(),
                                new Configuration(),
                                getClass().getClassLoader()));
    }

    private StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerCatalog("paimon", catalog);
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG fluss_store WITH ('type' = '%s', 'bootstrap.servers' = '%s')",
                        STORE_IDENTIFIER, FLUSS.getBootstrapServers()));
        tEnv.useCatalog("paimon");
        tEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
        tEnv.useDatabase(DATABASE);
        return tEnv;
    }

    /** Creates the store-side table and the matching lake-stream table in Paimon. */
    private void createLakeStreamTable(StreamTableEnvironment tEnv, String name) {
        tEnv.executeSql(String.format("CREATE TABLE fluss_store.%s.%s (id INT)", DATABASE, name));
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (id INT) WITH ('lakestream.enabled' = 'true')", name));
    }

    private static List<Integer> query(StreamTableEnvironment tEnv, String sql) throws Exception {
        List<Integer> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add((Integer) it.next().getField(0));
            }
        }
        return result;
    }

    @Test
    public void testRegularWriteGoesToFlussAndTheRealTimeNameReadsItBack() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv();
        createLakeStreamTable(tEnv, "orders");

        tEnv.executeSql("INSERT INTO orders SELECT * FROM (VALUES (1), (2)) AS t(id)").await();

        // the regular write reached Fluss, and the '$rt' name reads it back from the store
        assertThat(query(tEnv, "SELECT id FROM orders$rt " + BOUNDED_SCAN))
                .containsExactlyInAnyOrder(1, 2);

        // the original name still reads Paimon only, which the regular write bypassed
        assertThat(query(tEnv, "SELECT id FROM orders")).isEmpty();
    }

    @Test
    public void testRealTimeReadInBatchModeNeedsABoundedScan() {
        StreamTableEnvironment tEnv = newTableEnv();
        createLakeStreamTable(tEnv, "orders");

        // the store serves '$rt' as an unbounded source, which a batch query cannot read
        assertThatThrownBy(() -> query(tEnv, "SELECT id FROM orders$rt"))
                .hasStackTraceContaining("unbounded");
    }

    @Test
    public void testMissingStoreOptionFailsWithConfigurationError() {
        catalog = newCatalog(baseOptions());
        StreamTableEnvironment tEnv = newTableEnv();
        tEnv.executeSql("CREATE TABLE broken (id INT) WITH ('lakestream.enabled' = 'true')");

        // the Fluss catalog factory requires 'bootstrap.servers', which was never forwarded
        assertThatThrownBy(() -> query(tEnv, "SELECT id FROM broken$rt " + BOUNDED_SCAN))
                .hasStackTraceContaining(STORE_IDENTIFIER)
                .hasStackTraceContaining("bootstrap.servers");
    }
}
