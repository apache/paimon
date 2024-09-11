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

package org.apache.paimon.hive;

import org.apache.paimon.flink.FlinkGenericCatalog;
import org.apache.paimon.flink.FlinkGenericCatalogFactory;
import org.apache.paimon.hive.annotation.Minio;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.s3.MinioTestContainer;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** IT cases for using Flink {@code FlinkGenericCatalog}. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class FlinkGenericCatalogITCase extends AbstractTestBaseJUnit4 {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    protected TableEnvironment tEnv;

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    @Minio private static MinioTestContainer minioTestContainer;

    private static HiveCatalog createHiveCatalog(HiveConf hiveConf) {
        return new HiveCatalog(
                "testcatalog", null, hiveConf, HiveShimLoader.getHiveVersion(), true);
    }

    @Before
    public void before() throws Exception {
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
        hiveShell.execute("CREATE TABLE hive_table ( a INT, b STRING )");
        hiveShell.execute("INSERT INTO hive_table VALUES (100, 'Hive'), (200, 'Table')");
        hiveShell.executeQuery("SHOW TABLES");

        tEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance().inBatchMode().build());
        HiveCatalog hiveCatalog = createHiveCatalog(hiveShell.getHiveConf());
        FlinkGenericCatalog catalog =
                FlinkGenericCatalogFactory.createCatalog(
                        this.getClass().getClassLoader(),
                        new HashMap<>(),
                        hiveCatalog.getName(),
                        hiveCatalog);
        catalog.open();
        tEnv.registerCatalog(hiveCatalog.getName(), catalog);
        sql("USE CATALOG " + hiveCatalog.getName());
        sql("USE test_db");
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
        hiveShell.execute("DROP DATABASE IF EXISTS test_db2 CASCADE");
    }

    protected List<Row> sql(String query, Object... args) {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPaimonTableToBlackHole() {
        sql(
                "CREATE TABLE paimon_t ( "
                        + "f0 INT, "
                        + "f1 INT "
                        + ") WITH ('connector'='paimon', 'file.format' = 'avro' )");
        sql("INSERT INTO paimon_t VALUES (1, 1), (2, 2)");
        assertThat(sql("SELECT * FROM paimon_t"))
                .containsExactlyInAnyOrder(Row.of(1, 1), Row.of(2, 2));

        sql("CREATE TABLE bh (f0 INT, f1 INT) WITH ('connector'='blackhole')");
        sql("INSERT INTO bh SELECT * FROM paimon_t");
    }

    @Test
    public void testReadPaimonSystemTable() {
        sql(
                "CREATE TABLE paimon_t (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    PRIMARY KEY (dt, user_id) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt) "
                        + " WITH ('connector'='paimon', 'file.format' = 'avro' )");
        sql("INSERT INTO paimon_t VALUES (1, 2, 'click', '2023-11-01')");
        sql("INSERT INTO paimon_t VALUES (2, 3, 'click', '2023-11-02')");
        sql("INSERT INTO paimon_t VALUES (3, 4, 'click', '2023-11-03')");
        sql("INSERT INTO paimon_t VALUES (4, 5, 'click', '2023-11-04')");

        List<Row> result =
                sql("SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots");

        assertThat(result)
                .containsExactly(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"),
                        Row.of(4L, 0L, "APPEND"));

        // check leaf predicate query
        List<Row> result1 =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots where snapshot_id>0");

        assertThat(result1)
                .containsExactly(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"),
                        Row.of(4L, 0L, "APPEND"));

        // check leaf predicate query with exist snapshot_id
        List<Row> result2 =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots where snapshot_id=2");

        assertThat(result2).containsExactly(Row.of(2L, 0L, "APPEND"));

        // check leaf predicate query with exist snapshot_id
        assertThatThrownBy(
                        () ->
                                sql(
                                        "SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots where snapshot_id=6"))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(
                        "snapshot upper id:6 should not greater than latestSnapshotId:4");

        // check compound predicate query with right range
        List<Row> result3 =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots where snapshot_id>1 and snapshot_id<5");

        assertThat(result3)
                .containsExactly(
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"),
                        Row.of(4L, 0L, "APPEND"));

        // check with wrong range
        assertThatThrownBy(
                        () ->
                                sql(
                                        "SELECT snapshot_id, schema_id, commit_kind FROM paimon_t$snapshots where snapshot_id>9"))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(
                        "snapshot upper id:10 should not greater than latestSnapshotId:4");
    }

    @Test
    public void testReadPaimonAllProcedures() {
        List<Row> result = sql("SHOW PROCEDURES");

        assertThat(result)
                .contains(Row.of("compact"), Row.of("merge_into"), Row.of("migrate_table"));
    }
}
