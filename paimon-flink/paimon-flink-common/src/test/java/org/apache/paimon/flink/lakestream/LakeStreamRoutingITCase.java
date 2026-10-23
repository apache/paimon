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
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for the Flink half of the PIP-39 test plan (L506-512), using the fake
 * lake-stream store:
 *
 * <ul>
 *   <li>regular writes on a lake-stream table are routed to the store sink;
 *   <li>{@code INSERT OVERWRITE} is rejected, because the store's sink cannot execute it;
 *   <li>reads of the original table name access Paimon only;
 *   <li>reads of the {@code $rt} name are routed to the store with the BASE identifier;
 *   <li>original-name reads succeed with no store configured;
 *   <li>non-lake-stream tables behave exactly as before.
 * </ul>
 *
 * <p>{@code execution.runtime-mode} can only be set when a table environment is instantiated, so
 * each mode gets its own environment over the same catalog instance.
 */
public class LakeStreamRoutingITCase {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @BeforeEach
    public void beforeEach() {
        FakeStoreCatalogFactory.reset();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private void createCatalog(boolean withStore) {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        if (withStore) {
            conf.setString(
                    CatalogOptions.LAKE_STREAM_STORE.key(), FakeStoreCatalogFactory.IDENTIFIER);
            conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        }
        catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "paimon",
                                        conf.toMap(),
                                        new Configuration(),
                                        getClass().getClassLoader()));
    }

    private StreamTableEnvironment newTableEnv(RuntimeExecutionMode mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRuntimeMode(mode);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerCatalog("paimon", catalog);
        tEnv.useCatalog("paimon");
        return tEnv;
    }

    private StreamTableEnvironment newBatchEnvWithLakeStreamTable(String name) {
        createCatalog(true);
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.BATCH);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (id INT) WITH ("
                                + "'bucket' = '-1', 'lakestream.enabled' = 'true')",
                        name));
        return tEnv;
    }

    private static List<Integer> queryIds(StreamTableEnvironment tEnv, String sql)
            throws Exception {
        List<Integer> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add((Integer) it.next().getField(0));
            }
        }
        return result;
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    public void testRegularWriteRoutesToStoreAndOriginalNameReadsPaimonOnly() throws Exception {
        StreamTableEnvironment batch = newBatchEnvWithLakeStreamTable("t1");
        StreamTableEnvironment streaming = newTableEnv(RuntimeExecutionMode.STREAMING);

        streaming.executeSql("INSERT INTO t1 SELECT * FROM (VALUES (1), (2)) AS t(id)").await();

        // the regular write went to the fake store, through the delegated sink context
        assertThat(FakeStoreTableFactory.sinkContexts()).hasSize(1);
        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sinkContexts().get(0);
        assertThat(delegated.getObjectIdentifier().getObjectName()).isEqualTo("t1");
        assertThat(FakeStoreData.rows("default.t1")).hasSize(2);

        // reading the ORIGINAL name accesses Paimon only, which received nothing
        assertThat(queryIds(batch, "SELECT id FROM t1")).isEmpty();
    }

    @Test
    public void testRealTimeReadRoutesToStoreWithBaseIdentifier() throws Exception {
        StreamTableEnvironment batch = newBatchEnvWithLakeStreamTable("t1");
        StreamTableEnvironment streaming = newTableEnv(RuntimeExecutionMode.STREAMING);

        streaming.executeSql("INSERT INTO t1 SELECT * FROM (VALUES (1), (2)) AS t(id)").await();

        // the '$rt' read is served by the store's union read
        assertThat(queryIds(batch, "SELECT id FROM t1$rt")).containsExactlyInAnyOrder(1, 2);

        // the store's source factory saw the BASE identifier, without the '$rt' suffix
        assertThat(FakeStoreTableFactory.sourceContexts()).isNotEmpty();
        for (DynamicTableFactory.Context ctx : FakeStoreTableFactory.sourceContexts()) {
            assertThat(ctx.getObjectIdentifier().getObjectName()).isEqualTo("t1");
            assertThat(ctx.getObjectIdentifier().getDatabaseName()).isEqualTo("default");
        }
    }

    @Test
    public void testInsertOverwriteIsRejected() throws Exception {
        StreamTableEnvironment batch = newBatchEnvWithLakeStreamTable("t1");

        // a write to a lake-stream table is served by the store, and the store's sink cannot
        // execute an overwrite, so the planner rejects the statement rather than let it run
        // against the wrong side
        assertThatThrownBy(
                        () ->
                                batch.executeSql(
                                        "INSERT OVERWRITE t1 SELECT * FROM (VALUES (5)) AS t(id)"))
                .hasStackTraceContaining("INSERT OVERWRITE");
        assertThat(FakeStoreData.rows("default.t1")).isEmpty();
    }

    @Test
    public void testOriginalNameReadSucceedsWithoutStoreConfigured() throws Exception {
        createCatalog(false);
        StreamTableEnvironment batch = newTableEnv(RuntimeExecutionMode.BATCH);
        batch.executeSql("CREATE TABLE t3 (id INT) WITH ('bucket' = '-1')");

        batch.executeSql("INSERT INTO t3 SELECT * FROM (VALUES (7)) AS t(id)").await();

        assertThat(queryIds(batch, "SELECT id FROM t3")).containsExactly(7);
        assertThat(FakeStoreCatalogFactory.createdCatalogs()).isEmpty();
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
    }

    @Test
    public void testRealTimeNameIsReadOnly() {
        StreamTableEnvironment streaming = newBatchEnvWithLakeStreamTable("t1");

        // the '$rt' name is read-only; a write has to name the table itself
        assertThatThrownBy(() -> streaming.executeSql("INSERT INTO `t1$rt` SELECT 1"))
                .hasStackTraceContaining("read-only view")
                .hasStackTraceContaining("paimon.default.t1");
    }

    @Test
    public void testQueryHintCannotMoveAWriteOffTheStore() throws Exception {
        StreamTableEnvironment batch = newBatchEnvWithLakeStreamTable("t4");
        StreamTableEnvironment streaming = newTableEnv(RuntimeExecutionMode.STREAMING);

        streaming
                .executeSql(
                        "INSERT INTO t4 /*+ OPTIONS('lakestream.enabled' = 'false') */ "
                                + "SELECT * FROM (VALUES (1)) AS t(id)")
                .await();

        // the persisted option decides the route; a per-statement hint must not move a table's
        // writes away from the store that holds its recent data
        assertThat(FakeStoreData.rows("default.t4")).hasSize(1);
        assertThat(queryIds(batch, "SELECT id FROM t4")).isEmpty();
    }

    @Test
    public void testQueryHintCannotMakeAPlainTableWriteToTheStore() throws Exception {
        createCatalog(true);
        StreamTableEnvironment batch = newTableEnv(RuntimeExecutionMode.BATCH);
        batch.executeSql("CREATE TABLE t5 (id INT) WITH ('bucket' = '-1')");
        StreamTableEnvironment streaming = newTableEnv(RuntimeExecutionMode.STREAMING);

        streaming
                .executeSql(
                        "INSERT INTO t5 /*+ OPTIONS('lakestream.enabled' = 'true') */ "
                                + "SELECT * FROM (VALUES (1)) AS t(id)")
                .await();

        assertThat(FakeStoreData.rows("default.t5")).isEmpty();
        assertThat(queryIds(batch, "SELECT id FROM t5")).containsExactly(1);
    }

    @Test
    public void testNonLakeStreamTableBehavesAsBefore() throws Exception {
        createCatalog(true);
        StreamTableEnvironment batch = newTableEnv(RuntimeExecutionMode.BATCH);
        batch.executeSql("CREATE TABLE t2 (id INT) WITH ('bucket' = '-1')");

        batch.executeSql("INSERT INTO t2 SELECT * FROM (VALUES (3)) AS t(id)").await();

        // the write went to Paimon, not to the store
        assertThat(queryIds(batch, "SELECT id FROM t2")).containsExactly(3);
        assertThat(FakeStoreData.rows("default.t2")).isEmpty();
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
    }
}
