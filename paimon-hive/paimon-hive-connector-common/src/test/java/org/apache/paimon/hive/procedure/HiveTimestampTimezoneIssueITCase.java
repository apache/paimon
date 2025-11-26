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
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the local timezone conversion for Parquet files. */
public class HiveTimestampTimezoneIssueITCase extends ActionITCaseBase {

    private static final TestHiveMetastore HMS = new TestHiveMetastore();
    private static final int PORT = findFreePort();

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException("Could not find a free port", e);
        }
    }

    private static final TimeZone ORIGINAL_TZ = TimeZone.getDefault();
    private static final ZoneId TEST_ZONE = ZoneId.of("Asia/Shanghai");

    @TempDir Path warehouse;

    @BeforeAll
    public static void beforeAll() {
        HMS.start(PORT);
        TimeZone.setDefault(TimeZone.getTimeZone(TEST_ZONE));
        // set JVM timezone to Asia/Shanghai for this test run
    }

    @AfterAll
    public static void afterAll() throws Exception {
        HMS.stop();
        TimeZone.setDefault(ORIGINAL_TZ);
    }

    private static List<Row> sql(TableEnvironment env, String q, Object... args) {
        try (CloseableIterator<Row> it = env.executeSql(String.format(q, args)).collect()) {
            return ImmutableList.copyOf(it);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LocalDateTime parseTs(String s) {
        String iso = s.replace(' ', 'T');
        int p = Math.max(iso.indexOf('+'), Math.max(iso.indexOf('-', 19), iso.indexOf('Z')));
        if (p > 19) {
            iso = iso.substring(0, p);
        }
        return LocalDateTime.parse(iso);
    }

    private static class TableIds {
        final String db = "hivedb" + StringUtils.randomNumericString(10);
        final String tbl = "hivetable" + StringUtils.randomNumericString(10);
    }

    private TableEnvironment freshEnv() {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.getConfig().setLocalTimeZone(TEST_ZONE);

        tEnv.executeSql("CREATE CATALOG HIVE WITH ('type'='hive')");
        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse'='" + warehouse + "')");
        return tEnv;
    }

    @Test
    public void clonePreservesTimestamp_parquetHiveToPaimon() throws Exception {
        TableEnvironment tEnv = freshEnv();
        TableIds s = new TableIds();
        tEnv.useCatalog("HIVE");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql("CREATE DATABASE " + s.db);
        sql(tEnv, "CREATE TABLE %s.%s (`a` INT, `ts` TIMESTAMP) STORED AS PARQUET", s.db, s.tbl);
        sql(tEnv, "INSERT INTO %s.%s VALUES (1, '2025-06-03 16:00:00')", s.db, s.tbl);

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        List<Row> hiveRows =
                sql(tEnv, "SELECT a, CAST(ts AS STRING) AS ts_str FROM HIVE.%s.%s", s.db, s.tbl);
        assertThat(hiveRows).hasSize(1);

        assertThat(hiveRows.get(0).getField(0)).isEqualTo(1);
        String hiveTsStr = hiveRows.get(0).getField(1).toString();
        LocalDateTime hiveTs = parseTs(hiveTsStr);

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        s.db,
                        "--table",
                        s.tbl,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse.toString())
                .run();
        tEnv.useCatalog("PAIMON");
        List<Row> paimonRows =
                sql(tEnv, "SELECT a, CAST(ts AS STRING) AS ts_str FROM test.test_table");
        assertThat(paimonRows).hasSize(1);
        assertThat(paimonRows.get(0).getField(0)).isEqualTo(1);
        String paimonTsStr = paimonRows.get(0).getField(1).toString();

        LocalDateTime paimonTs = parseTs(paimonTsStr);

        assertThat(paimonTs).isEqualTo(hiveTs);
    }
}
