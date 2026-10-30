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

package org.apache.paimon.hive.procedure;

import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CloneAction} with {@code copy_files} and {@code recursive_listing}. */
public class CloneHiveCopyFilesITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static int port;

    @BeforeAll
    public static void beforeAll() {
        TEST_HIVE_METASTORE.start(0);
        port = TEST_HIVE_METASTORE.getPort();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testCopyNestedFilesIntoTargetWithMoreFields() throws Exception {
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        TableEnvironment tEnv = hiveTableEnvironment(dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.t (id STRING, s STRUCT<a:INT>, tags ARRAY<STRUCT<a:INT>>, "
                        + "m MAP<STRING, STRUCT<a:INT>>) STORED AS orc",
                dbName);
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        sql(
                tEnv,
                "INSERT INTO %s.t VALUES ('a', ROW(1), ARRAY[ROW(10)], MAP['k', ROW(100)]), "
                        + "('b', ROW(2), ARRAY[ROW(20)], MAP['k', ROW(200)])",
                dbName);

        Path root = hiveTableDir(dbName, "t");
        Path nested = Files.createDirectories(root.resolve("sub/deep"));
        for (Path file : hiveDataFiles(root)) {
            Files.move(file, nested.resolve(file.getFileName()));
        }

        usePaimonCatalog(tEnv);
        sql(
                tEnv,
                "CREATE TABLE test.t (extra INT, id STRING, s ROW<b INT, a INT>, "
                        + "tags ARRAY<ROW<b INT, a INT>>, m MAP<STRING, ROW<b INT, a INT>>) "
                        + "WITH ('bucket' = '-1', 'file.format' = 'orc')");
        cloneWithCopyFiles(dbName, "t");

        assertThat(
                        sql(
                                tEnv,
                                "SELECT extra, id, s.b, s.a, tags[1].b, tags[1].a, m['k'].b, "
                                        + "m['k'].a FROM test.t"))
                .containsExactlyInAnyOrder(
                        Row.of(null, "a", null, 1, null, 10, null, 100),
                        Row.of(null, "b", null, 2, null, 20, null, 200));
    }

    @Test
    public void testCopyFilesIntoTargetWithOtherPartitionKeyOrder() throws Exception {
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        TableEnvironment tEnv = hiveTableEnvironment(dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.t (id STRING) PARTITIONED BY (p1 STRING, p2 STRING) STORED AS orc",
                dbName);
        sql(tEnv, "INSERT INTO %s.t PARTITION (p1 = 'A', p2 = 'B') VALUES ('a')", dbName);

        usePaimonCatalog(tEnv);
        sql(
                tEnv,
                "CREATE TABLE test.t (id STRING, p1 STRING, p2 STRING) PARTITIONED BY (p2, p1) "
                        + "WITH ('bucket' = '-1', 'file.format' = 'orc')");
        // the partition the source values land in when they are read in the wrong order
        sql(tEnv, "INSERT INTO test.t VALUES ('keep', 'B', 'A')");
        cloneWithCopyFiles(dbName, "t");

        assertThat(sql(tEnv, "SELECT * FROM test.t"))
                .containsExactlyInAnyOrder(Row.of("a", "A", "B"), Row.of("keep", "B", "A"));
    }

    @Test
    public void testCopyFilesSkipsUnregisteredPartitions() throws Exception {
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        TableEnvironment tEnv = hiveTableEnvironment(dbName);
        sql(tEnv, "CREATE TABLE %s.src (id STRING, id2 INT) STORED AS orc", dbName);
        sql(tEnv, "INSERT INTO %s.src VALUES ('a', 1)", dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.part (id STRING, id2 INT) PARTITIONED BY (dt STRING) STORED AS orc",
                dbName);

        // a partition directory which was never added to the metastore
        Path partitionDir = Files.createDirectories(hiveTableDir(dbName, "part").resolve("dt=1"));
        for (Path file : hiveDataFiles(hiveTableDir(dbName, "src"))) {
            Files.copy(file, partitionDir.resolve(file.getFileName()));
        }

        usePaimonCatalog(tEnv);
        cloneWithCopyFiles(dbName, "part");

        assertThat(sql(tEnv, "SELECT * FROM test.part")).isEmpty();
    }

    /**
     * Statistics gathering and reads through Hive's input formats fail with the ORC and Parquet
     * versions on the test classpath, so the tests use neither.
     */
    private TableEnvironment hiveTableEnvironment(String dbName) {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.getConfig().set("table.exec.hive.sink.statistic-auto-gather.enable", "false");
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        return tEnv;
    }

    private void usePaimonCatalog(TableEnvironment tEnv) throws Exception {
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");
    }

    private void cloneWithCopyFiles(String dbName, String tableName) throws Exception {
        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        dbName,
                        "--table",
                        tableName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + port,
                        "--target_database",
                        "test",
                        "--target_table",
                        tableName,
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--copy_files",
                        "true",
                        "--recursive_listing",
                        "true")
                .run();
    }

    private static Path hiveTableDir(String dbName, String tableName) throws Exception {
        String warehouseDir = System.getProperty("hive.metastore.warehouse.dir");
        return Paths.get(
                        warehouseDir.startsWith("file:")
                                ? new URI(warehouseDir).getPath()
                                : warehouseDir)
                .resolve(dbName + ".db")
                .resolve(tableName);
    }

    private static List<Path> hiveDataFiles(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .filter(p -> !p.getFileName().toString().startsWith("_"))
                    .collect(Collectors.toList());
        }
    }

    private static List<Row> sql(TableEnvironment tEnv, String query, Object... args)
            throws Exception {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        }
    }
}
