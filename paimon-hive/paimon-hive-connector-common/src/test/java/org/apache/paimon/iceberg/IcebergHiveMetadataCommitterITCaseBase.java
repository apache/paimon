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

package org.apache.paimon.iceberg;

import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** IT cases for {@link IcebergHiveMetadataCommitter}. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public abstract class IcebergHiveMetadataCommitterITCaseBase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    private String path;

    @Before
    public void before() throws Exception {
        path = folder.newFolder().toURI().toString();
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    @Test
    public void testPrimaryKeyTable() throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE CATALOG my_paimon WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + path
                        + "' )");
        tEnv.executeSql("CREATE DATABASE my_paimon.test_db");
        tEnv.executeSql(
                "CREATE TABLE my_paimon.test_db.t ( pt INT, id INT, data STRING, PRIMARY KEY (pt, id) NOT ENFORCED ) "
                        + "PARTITIONED BY (pt) WITH "
                        + "( 'metadata.iceberg.storage' = 'hive-catalog', 'metadata.iceberg.uri' = '', 'file.format' = 'avro', "
                        // make sure all changes are visible in iceberg metadata
                        + " 'full-compaction.delta-commits' = '1' )");
        tEnv.executeSql(
                        "INSERT INTO my_paimon.test_db.t VALUES "
                                + "(1, 1, 'apple'), (1, 2, 'pear'), (2, 1, 'cat'), (2, 2, 'dog')")
                .await();

        tEnv.executeSql(
                "CREATE CATALOG my_iceberg WITH "
                        + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = '', 'warehouse' = '"
                        + path
                        + "', 'cache-enabled' = 'false' )");
        Assert.assertEquals(
                Arrays.asList(Row.of("pear", 2, 1), Row.of("dog", 2, 2)),
                collect(
                        tEnv.executeSql(
                                "SELECT data, id, pt FROM my_iceberg.test_db.t WHERE id = 2 ORDER BY pt, id")));

        tEnv.executeSql(
                        "INSERT INTO my_paimon.test_db.t VALUES "
                                + "(1, 1, 'cherry'), (2, 2, 'elephant')")
                .await();
        Assert.assertEquals(
                Arrays.asList(
                        Row.of(1, 1, "cherry"),
                        Row.of(1, 2, "pear"),
                        Row.of(2, 1, "cat"),
                        Row.of(2, 2, "elephant")),
                collect(tEnv.executeSql("SELECT * FROM my_iceberg.test_db.t ORDER BY pt, id")));
    }

    @Test
    public void testAppendOnlyTable() throws Exception {
        TableEnvironment tEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE CATALOG my_paimon WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + path
                        + "' )");
        tEnv.executeSql("CREATE DATABASE my_paimon.test_db");
        tEnv.executeSql(
                "CREATE TABLE my_paimon.test_db.t ( pt INT, id INT, data STRING ) PARTITIONED BY (pt) WITH "
                        + "( 'metadata.iceberg.storage' = 'hive-catalog', 'metadata.iceberg.uri' = '', 'file.format' = 'avro' )");
        tEnv.executeSql(
                        "INSERT INTO my_paimon.test_db.t VALUES "
                                + "(1, 1, 'apple'), (1, 2, 'pear'), (2, 1, 'cat'), (2, 2, 'dog')")
                .await();

        tEnv.executeSql(
                "CREATE CATALOG my_iceberg WITH "
                        + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = '', 'warehouse' = '"
                        + path
                        + "', 'cache-enabled' = 'false' )");
        Assert.assertEquals(
                Arrays.asList(Row.of("pear", 2, 1), Row.of("dog", 2, 2)),
                collect(
                        tEnv.executeSql(
                                "SELECT data, id, pt FROM my_iceberg.test_db.t WHERE id = 2 ORDER BY pt, id")));

        tEnv.executeSql(
                        "INSERT INTO my_paimon.test_db.t VALUES "
                                + "(1, 3, 'cherry'), (2, 3, 'elephant')")
                .await();
        Assert.assertEquals(
                Arrays.asList(
                        Row.of("pear", 2, 1),
                        Row.of("cherry", 3, 1),
                        Row.of("dog", 2, 2),
                        Row.of("elephant", 3, 2)),
                collect(
                        tEnv.executeSql(
                                "SELECT data, id, pt FROM my_iceberg.test_db.t WHERE id > 1 ORDER BY pt, id")));
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }
}
