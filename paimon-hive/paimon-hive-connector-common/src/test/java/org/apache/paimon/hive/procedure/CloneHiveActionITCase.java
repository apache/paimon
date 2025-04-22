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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.CloneHiveAction;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link CloneHiveAction}. */
public class CloneHiveActionITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9088;

    @BeforeEach
    public void beforeEach() throws IOException {
        super.before();
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterEach
    public void afterEach() throws Exception {
        super.after();
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testMigrateOneNonPartitionedTable() throws Exception {
        String format = randomFormat();

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE TABLE hivetable (id string, id2 int, id3 int) STORED AS " + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivetable").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneHiveAction.class,
                        "clone_hive",
                        "--database",
                        "default",
                        "--table",
                        "hivetable",
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

        List<Row> r2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.test_table").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateOnePartitionedTable() throws Exception {
        testMigrateOnePartitionedTableImpl(null);
    }

    @Test
    public void testMigrateOnePartitionedTableWithFilter() throws Exception {
        testMigrateOnePartitionedTableImpl("id2 = 1 OR id3 = 1");
    }

    public void testMigrateOnePartitionedTableImpl(@Nullable String whereSql) throws Exception {
        String format = randomFormat();

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        String query = "SELECT * FROM hivetable " + (whereSql == null ? "" : "WHERE " + whereSql);
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql(query).collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone_hive",
                                "--database",
                                "default",
                                "--table",
                                "hivetable",
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
        if (whereSql != null) {
            args.add("--where");
            args.add(whereSql);
        }

        createAction(CloneHiveAction.class, args).run();

        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

        Assertions.assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

        List<Row> r2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.test_table").collect());

        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateOnePartitionedTableAndFilterNoPartition() throws Exception {
        String format = randomFormat();

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        List<String> args =
                Arrays.asList(
                        "clone_hive",
                        "--database",
                        "default",
                        "--table",
                        "hivetable",
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

        createAction(CloneHiveAction.class, args).run();

        // table exists but no data
        FileStoreTable paimonTable =
                paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));
        Assertions.assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");
        Assertions.assertThat(paimonTable.snapshotManager().earliestSnapshot()).isNull();
    }

    @Test
    public void testMigrateWholeDatabase() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE hivedb");
        tEnv.executeSql(
                "CREATE TABLE hivedb.hivetable1 (id string, id2 int, id3 int) STORED AS "
                        + randomFormat());
        tEnv.executeSql("INSERT INTO hivedb.hivetable1 VALUES" + data(100)).await();
        tEnv.executeSql(
                "CREATE TABLE hivedb.hivetable2 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + randomFormat());
        tEnv.executeSql("INSERT INTO hivedb.hivetable2 VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");
        List<Row> r1 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivedb.hivetable1").collect());
        List<Row> r2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hivedb.hivetable2").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneHiveAction.class,
                        "clone_hive",
                        "--database",
                        "hivedb",
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse)
                .run();

        List<Row> actualR1 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.hivetable1").collect());
        List<Row> actualR2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.hivetable2").collect());

        Assertions.assertThatList(actualR1).containsExactlyInAnyOrderElementsOf(r1);
        Assertions.assertThatList(actualR2).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateWholeDatabaseWithFilter() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE hivedb");
        tEnv.executeSql(
                "CREATE TABLE hivedb.hivetable1 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + randomFormat());
        tEnv.executeSql("INSERT INTO hivedb.hivetable1 VALUES" + data(100)).await();
        tEnv.executeSql(
                "CREATE TABLE hivedb.hivetable2 (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + randomFormat());
        tEnv.executeSql("INSERT INTO hivedb.hivetable2 VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        List<Row> r1 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM hivedb.hivetable1 WHERE id2=1 OR id3=1")
                                .collect());
        List<Row> r2 =
                ImmutableList.copyOf(
                        tEnv.executeSql("SELECT * FROM hivedb.hivetable2 WHERE id2=1 OR id3=1")
                                .collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneHiveAction.class,
                        "clone_hive",
                        "--database",
                        "hivedb",
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

        List<Row> actualR1 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.hivetable1").collect());
        List<Row> actualR2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.hivetable2").collect());

        Assertions.assertThatList(actualR1).containsExactlyInAnyOrderElementsOf(r1);
        Assertions.assertThatList(actualR2).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testCloneWithExistedTable() throws Exception {
        String format = "avro";

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE hivetable (id string) PARTITIONED BY (id2 int, id3 int) STORED AS "
                        + format);
        tEnv.executeSql("INSERT INTO hivetable VALUES" + data(100)).await();

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("CREATE CATALOG PAIMON_GE WITH ('type'='paimon-generic')");
        tEnv.useCatalog("PAIMON_GE");

        String query = "SELECT * FROM hivetable";
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql(query).collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");
        // create a paimon table with the same name
        //        int ddlIndex = ThreadLocalRandom.current().nextInt(0, 4);
        int ddlIndex = 3;
        tEnv.executeSql(ddls()[ddlIndex]);

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "clone_hive",
                                "--database",
                                "default",
                                "--table",
                                "hivetable",
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

        if (ddlIndex < 3) {
            assertThatThrownBy(() -> createAction(CloneHiveAction.class, args).run())
                    .rootCause()
                    .hasMessageContaining(exceptionMsg()[ddlIndex]);
        } else {
            createAction(CloneHiveAction.class, args).run();
            FileStoreTable paimonTable =
                    paimonTable(tEnv, "PAIMON", Identifier.create("test", "test_table"));

            Assertions.assertThat(paimonTable.partitionKeys()).containsExactly("id2", "id3");

            List<Row> r2 =
                    ImmutableList.copyOf(
                            tEnv.executeSql("SELECT * FROM test.test_table").collect());

            Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
        }
    }

    private String[] ddls() {
        // has primary key
        String ddl0 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int, PRIMARY KEY (id, id2, id3) NOT ENFORCED) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1');";
        // has different partition keys
        String ddl1 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int) "
                        + "PARTITIONED BY (id, id3) with ('bucket' = '-1');";
        // size of fields is different
        String ddl2 =
                "CREATE TABLE test.test_table (id2 int, id3 int) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1');";
        // normal
        String ddl3 =
                "CREATE TABLE test.test_table (id string, id2 int, id3 int) "
                        + "PARTITIONED BY (id2, id3) with ('bucket' = '-1');";
        return new String[] {ddl0, ddl1, ddl2, ddl3};
    }

    private String[] exceptionMsg() {
        return new String[] {
            "Can not clone data to existed paimon table which has primary keys",
            "source table partition keys is not compatible with existed paimon table partition keys.",
            "source table partition keys is not compatible with existed paimon table partition keys."
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
        if (i == 0) {
            return "orc";
        } else if (i == 1) {
            return "parquet";
        } else {
            return "avro";
        }
    }

    protected FileStoreTable paimonTable(
            TableEnvironment tEnv, String catalogName, Identifier table)
            throws org.apache.paimon.catalog.Catalog.TableNotExistException {
        FlinkCatalog flinkCatalog = (FlinkCatalog) tEnv.getCatalog(catalogName).get();
        Catalog catalog = flinkCatalog.catalog();
        return (FileStoreTable) catalog.getTable(table);
    }
}
