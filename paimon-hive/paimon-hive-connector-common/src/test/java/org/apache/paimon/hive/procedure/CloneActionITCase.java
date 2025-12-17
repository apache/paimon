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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link CloneAction}. */
@Disabled // TODO fix unstable cases
public class CloneActionITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9088;

    @BeforeAll
    public static void beforeAll() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testClonePKTableFromPaimon() throws Exception {
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();
        String warehouse1 = getTempDirPath();
        String warehouse2 = getTempDirPath();
        sql(tEnv, "CREATE CATALOG catalog1 WITH ('type'='paimon', 'warehouse' = '%s')", warehouse1);
        sql(tEnv, "CREATE CATALOG catalog2 WITH ('type'='paimon', 'warehouse' = '%s')", warehouse2);

        sql(
                tEnv,
                "CREATE TABLE catalog1.`default`.src (a INT, b INT, PRIMARY KEY (a) NOT ENFORCED)");
        sql(tEnv, "INSERT INTO catalog1.`default`.src VALUES (1, 1), (2, 2)");
        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        "default",
                        "--table",
                        "src",
                        "--catalog_conf",
                        "warehouse=" + warehouse1,
                        "--target_database",
                        "default",
                        "--target_table",
                        "target",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse2,
                        "--clone_from",
                        "paimon")
                .run();

        sql(tEnv, "CALL catalog2.sys.compact(`table` => 'default.target')");
        List<Row> result = sql(tEnv, "SELECT * FROM catalog2.`default`.target");
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1), Row.of(2, 2));
        List<Row> show = sql(tEnv, "SHOW CREATE TABLE catalog2.`default`.target");
        assertThat(show.toString()).contains("PRIMARY KEY");
    }

    @Test
    public void testCloneBucketedAppendFromPaimon() throws Exception {
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();
        String warehouse1 = getTempDirPath();
        String warehouse2 = getTempDirPath();
        sql(tEnv, "CREATE CATALOG catalog1 WITH ('type'='paimon', 'warehouse' = '%s')", warehouse1);
        sql(tEnv, "CREATE CATALOG catalog2 WITH ('type'='paimon', 'warehouse' = '%s')", warehouse2);

        sql(
                tEnv,
                "CREATE TABLE catalog1.`default`.src (a INT, b INT) WITH ('bucket' = '2', 'bucket-key' = 'b')");
        sql(tEnv, "INSERT INTO catalog1.`default`.src VALUES (1, 1), (2, 2)");
        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        "default",
                        "--table",
                        "src",
                        "--catalog_conf",
                        "warehouse=" + warehouse1,
                        "--target_database",
                        "default",
                        "--target_table",
                        "target",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse2,
                        "--clone_from",
                        "paimon")
                .run();

        sql(tEnv, "CALL catalog2.sys.compact(`table` => 'default.target')");
        List<Row> result = sql(tEnv, "SELECT * FROM catalog2.`default`.target");
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1), Row.of(2, 2));
        List<Row> show = sql(tEnv, "SHOW CREATE TABLE catalog2.`default`.target");
        assertThat(show.toString()).contains("'bucket' = '2'");
        assertThat(show.toString()).contains("'bucket-key' = 'b'");
    }

    @Test
    public void testMigrateOneNonPartitionedTable() throws Exception {
        String format = randomFormat();
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

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
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse)
                .run();

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);

        List<Row> files = sql(tEnv, "SELECT file_path FROM test.`test_table$files`");
        assertThat(files).hasSize(1);

        // file name should be start with data-, which is generated by uuid
        assertThat(new Path(files.get(0).getField(0).toString()).getName()).startsWith("data-");
    }

    @Test
    public void testCloneWithTimestamp() throws Exception {
        String format = "orc";
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (`a` int COMMENT 'The a field',`ts` timestamp COMMENT 'The ts field') STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES (1, '2025-06-03 16:00:00')", dbName, tableName);

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

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
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse)
                .run();

        assertThatCode(() -> sql(tEnv, "SELECT * FROM test.test_table")).doesNotThrowAnyException();

        List<Row> files = sql(tEnv, "SELECT file_path FROM test.`test_table$files`");
        assertThat(files).hasSize(1);

        // file name should be start with data-, which is generated by uuid
        assertThat(new Path(files.get(0).getField(0).toString()).getName()).startsWith("data-");
    }

    @Test
    public void testMigrateOnePartitionedTable() throws Exception {
        testMigrateOnePartitionedTableImpl(false);
    }

    @Test
    public void testMigrateOnePartitionedTableWithFilter() throws Exception {
        testMigrateOnePartitionedTableImpl(true);
    }

    public void testMigrateOnePartitionedTableImpl(boolean specificFilter) throws Exception {
        String format = randomFormat();
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 =
                sql(
                        tEnv,
                        "SELECT * FROM %s.%s %s",
                        dbName,
                        tableName,
                        specificFilter ? "WHERE id2 = 1 OR id3 = 1" : "");

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse));
        if (specificFilter) {
            args.add("--where");
            args.add("id2 = 1 OR id3 = 1");
        }

        createAction(CloneAction.class, args).run();
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

        assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

        // first run, validate clone
        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);

        if (specificFilter) {
            // test other partitions
            // drop where
            args = new ArrayList<>(args.subList(0, args.size() - 1));
            args.add("id2 <> 1 AND id3 <> 1");
            createAction(CloneAction.class, args).run();

            // assert not file deleted
            Snapshot snapshot = paimonTable.latestSnapshot().get();
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
            List<ManifestFileMeta> manifests =
                    paimonTable.manifestListReader().read(snapshot.deltaManifestList());
            assertThat(manifests).noneMatch(manifest -> manifest.numDeletedFiles() > 0);

            // expect all
            r1 = sql(tEnv, "SELECT * FROM PAIMON_GE.%s.%s", dbName, tableName);
            r2 = sql(tEnv, "SELECT * FROM test.test_table");
            assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        } else {
            // run again, validate overwrite
            createAction(CloneAction.class, args).run();
            r2 = sql(tEnv, "SELECT * FROM test.test_table");
            assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        }
    }

    @Test
    public void testMigrateOnePartitionedTableAndFilterNoPartition() throws Exception {
        String format = randomFormat();
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                Arrays.asList(
                        "clone",
                        "--database",
                        dbName,
                        "--table",
                        tableName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--where",
                        // the data won't < 0
                        "id2 < 0");

        createAction(CloneAction.class, args).run();

        // table exists but no data
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));
        assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");
        assertThat(paimonTable.snapshotManager().earliestSnapshot()).isNull();
    }

    @Test
    public void testMigrateWholeDatabase() throws Exception {
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName1 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName2 = "hivetable2" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName1,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName, tableName1, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName2,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName, tableName2, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName1);
        List<Row> r2 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName2);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        dbName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse)
                .run();

        List<Row> actualR1 = sql(tEnv, "SELECT * FROM test.%s", tableName1);
        List<Row> actualR2 = sql(tEnv, "SELECT * FROM test.%s", tableName2);

        assertThatList(actualR1).containsExactlyInAnyOrderElementsOf(r1);
        assertThatList(actualR2).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateWholeDatabaseWithFilter() throws Exception {
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName1 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName2 = "hivetable1" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName1,
                randomFormat());
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName1, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName2,
                randomFormat());
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName1, data(100));
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName2, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s WHERE id2=1 OR id3=1", dbName, tableName1);
        List<Row> r2 = sql(tEnv, "SELECT * FROM %s.%s WHERE id2=1 OR id3=1", dbName, tableName2);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        dbName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--where",
                        "id2=1 OR id3=1")
                .run();

        List<Row> actualR1 = sql(tEnv, "SELECT * FROM test.%s", tableName1);
        List<Row> actualR2 = sql(tEnv, "SELECT * FROM test.%s", tableName2);

        assertThatList(actualR1).containsExactlyInAnyOrderElementsOf(r1);
        assertThatList(actualR2).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testCloneWithExistedTable() throws Exception {
        String format = randomFormat();
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");
        // create a paimon table with the same name
        int ddlIndex = ThreadLocalRandom.current().nextInt(0, 4);
        tEnv.executeSql(ddls(format)[ddlIndex]);

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse));

        if (ddlIndex < 4) {
            assertThatThrownBy(() -> createAction(CloneAction.class, args).run())
                    .rootCause()
                    .hasMessageContaining(exceptionMsg()[ddlIndex]);
        } else {
            createAction(CloneAction.class, args).run();
            FileStoreTable paimonTable =
                    paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

            assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

            List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
            assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        }
    }

    @Test
    public void testCloneWithNotExistedDatabase() throws Exception {
        String format = randomFormat();
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse));

        createAction(CloneAction.class, args).run();
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

        assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateWholeCatalogWithExcludedTables() throws Exception {
        String dbName1 = "hivedb" + StringUtils.randomNumericString(10);
        String tableName1 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName2 = "hivetable2" + StringUtils.randomNumericString(10);

        String dbName2 = "hivedb" + StringUtils.randomNumericString(10);
        String tableName3 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName4 = "hivetable2" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName1);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName1,
                tableName1,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName1, tableName1, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName1,
                tableName2,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName1, tableName2, data(100));

        tEnv.executeSql("CREATE DATABASE " + dbName2);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName2,
                tableName3,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName2, tableName3, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName2,
                tableName4,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName2, tableName4, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<String> db1Tables = ImmutableList.of(tableName2);
        List<String> db2Tables = ImmutableList.of(tableName4);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--excluded_tables",
                        dbName1 + "." + tableName1 + "," + dbName2 + "." + tableName3)
                .run();

        List<String> actualDB1Tables =
                sql(tEnv, "show tables from %s", dbName1).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        List<String> actualDB2Tables =
                sql(tEnv, "show tables from %s", dbName2).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThatList(actualDB1Tables).containsExactlyInAnyOrderElementsOf(db1Tables);
        assertThatList(actualDB2Tables).containsExactlyInAnyOrderElementsOf(db2Tables);
    }

    @Test
    public void testMigrateWholeCatalogWithIncludedTables() throws Exception {
        String dbName1 = "hivedb" + StringUtils.randomNumericString(10);
        String tableName1 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName2 = "hivetable2" + StringUtils.randomNumericString(10);

        String dbName2 = "hivedb" + StringUtils.randomNumericString(10);
        String tableName3 = "hivetable1" + StringUtils.randomNumericString(10);
        String tableName4 = "hivetable2" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName1);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName1,
                tableName1,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName1, tableName1, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName1,
                tableName2,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName1, tableName2, data(100));

        tEnv.executeSql("CREATE DATABASE " + dbName2);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING, id2 INT, id3 INT) STORED AS %s",
                dbName2,
                tableName3,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName2, tableName3, data(100));
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) STORED AS %s",
                dbName2,
                tableName4,
                randomFormat());
        sql(tEnv, "INSERT INTO TABLE %s.%s VALUES %s", dbName2, tableName4, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<String> db1Tables = ImmutableList.of(tableName1);
        List<String> db2Tables = ImmutableList.of(tableName3);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--included_tables",
                        dbName1 + "." + tableName1 + "," + dbName2 + "." + tableName3)
                .run();

        List<String> actualDB1Tables =
                sql(tEnv, "show tables from %s", dbName1).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        List<String> actualDB2Tables =
                sql(tEnv, "show tables from %s", dbName2).stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());

        assertThatList(actualDB1Tables).containsExactlyInAnyOrderElementsOf(db1Tables);
        assertThatList(actualDB2Tables).containsExactlyInAnyOrderElementsOf(db2Tables);
    }

    @Test
    public void testMigrateCsvTable() throws Exception {
        String format = "textfile";
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT) "
                        + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS %s ",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse));

        createAction(CloneAction.class, args).run();
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

        assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateJsonTable() throws Exception {
        String format = "textfile";
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT)"
                        + "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS %s ",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse));

        createAction(CloneAction.class, args).run();
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

        assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateWithPreferFileFormat() throws Exception {
        String format = "orc";
        String preferFileFormat = "parquet";
        String dbName = "hivedb" + StringUtils.randomNumericString(10);
        String tableName = "hivetable" + StringUtils.randomNumericString(10);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + dbName);
        sql(
                tEnv,
                "CREATE TABLE %s.%s (id STRING) PARTITIONED BY (id2 INT, id3 INT)"
                        + "STORED AS %s ",
                dbName,
                tableName,
                format);
        sql(tEnv, "INSERT INTO %s.%s VALUES %s", dbName, tableName, data(100));

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 = sql(tEnv, "SELECT * FROM %s.%s", dbName, tableName);

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone",
                                "--database",
                                dbName,
                                "--table",
                                tableName,
                                "--catalog_conf",
                                "metastore=hive",
                                "--catalog_conf",
                                "uri=thrift://localhost:" + PORT,
                                "--target_database",
                                "test",
                                "--target_table",
                                "test_table",
                                "--target_catalog_conf",
                                "warehouse=" + warehouse,
                                "--prefer_file_format",
                                preferFileFormat));

        createAction(CloneAction.class, args).run();
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));
        assertEquals(paimonTable.options().get(CoreOptions.FILE_FORMAT.key()), preferFileFormat);

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    private String[] ddls(String format) {
        // has primary key
        String ddl0 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int, PRIMARY KEY (id, id2, id3) NOT ENFORCED) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1', 'file.format' = '"
                        + format
                        + "');";
        // has different partition keys
        String ddl1 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int) "
                        + "PARTITIONED BY (id, id3) with ('bucket' = '-1', 'file.format' = '"
                        + format
                        + "');";
        // size of fields is different
        String ddl2 =
                "CREATE TABLE test.test_table (id2 int, id3 int) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1', 'file.format' = '"
                        + format
                        + "');";

        // different format
        String ddl3 =
                "CREATE TABLE test.test_table (id2 int, id3 int) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1', 'file.format' = '"
                        + randomFormat(format)
                        + "');";

        // normal
        String ddl4 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1', 'file.format' = '"
                        + format
                        + "');";
        return new String[] {ddl0, ddl1, ddl2, ddl3, ddl4};
    }

    private String[] exceptionMsg() {
        return new String[] {
            "Can not clone data to existed paimon table which has primary keys",
            "source table partition keys is not compatible with existed paimon table partition keys.",
            "source table partition keys is not compatible with existed paimon table partition keys.",
            "source table format is not compatible with existed paimon table format."
        };
    }

    private static String data(int i) {
        Random random = new Random();
        StringBuilder stringBuilder = new StringBuilder();
        for (int m = 0; m < i; m++) {
            stringBuilder.append("(");
            stringBuilder.append("\"");
            stringBuilder.append('a' + m);
            stringBuilder.append("\",");
            stringBuilder.append(random.nextInt(10));
            stringBuilder.append(",");
            stringBuilder.append(random.nextInt(10));
            stringBuilder.append(")");
            if (m != i - 1) {
                stringBuilder.append(",");
            }
        }
        return stringBuilder.toString();
    }

    private String randomFormat() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int i = random.nextInt(3);
        String[] formats = new String[] {"orc", "parquet", "avro", "textfile"};
        return formats[i];
    }

    private String randomFormat(String excludedFormat) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int i = random.nextInt(3);
        String[] formats = new String[] {"orc", "parquet", "avro", "textfile"};
        if (Objects.equals(excludedFormat, formats[i])) {
            return formats[(i + 1) % 3];
        }
        return formats[i];
    }

    private FileStoreTable paimonTable(TableEnvironment tEnv, String catalogName, Identifier table)
            throws org.apache.paimon.catalog.Catalog.TableNotExistException {
        FlinkCatalog flinkCatalog = (FlinkCatalog) tEnv.getCatalog(catalogName).get();
        Catalog catalog = flinkCatalog.catalog();
        return (FileStoreTable) catalog.getTable(table);
    }

    private List<Row> sql(TableEnvironment tEnv, String query, Object... args) {
        String formattedQuery = String.format(query, args);
        Exception lastException = null;

        for (int attempt = 1; attempt <= 5; attempt++) {
            try (CloseableIterator<Row> iter = tEnv.executeSql(formattedQuery).collect()) {
                return ImmutableList.copyOf(iter);
            } catch (Exception e) {
                lastException = e;
                if (attempt < 5) {
                    try {
                        Thread.sleep(60_000); // 1 minute between attempts
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw new RuntimeException(lastException);
    }
}
