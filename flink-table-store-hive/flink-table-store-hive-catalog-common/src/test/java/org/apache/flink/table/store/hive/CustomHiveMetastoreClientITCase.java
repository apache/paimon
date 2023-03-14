/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.connectors.hive.FlinkEmbeddedHiveRunner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
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

/** IT cases for {@link HiveCatalog} with custom Hive metastore client. */
@RunWith(FlinkEmbeddedHiveRunner.class)
public class CustomHiveMetastoreClientITCase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private String path;
    private TableEnvironment tEnv;

    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    @Before
    public void before() throws Exception {
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    @Test
    public void testCustomMetastoreClient() throws Exception {
        path = folder.newFolder().toURI().toString();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironmentImpl.create(settings);
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive WITH (",
                                "  'type' = 'table-store',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'warehouse' = '" + path + "',",
                                "  'metastore.client.class' = '"
                                        + TestHiveMetaStoreClient.class.getName()
                                        + "'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive").await();
        Assert.assertEquals(
                Arrays.asList(
                        Row.of("default"),
                        Row.of("test_db"),
                        Row.of(TestHiveMetaStoreClient.MOCK_DATABASE)),
                collect("SHOW DATABASES"));
    }

    private List<Row> collect(String sql) throws Exception {
        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }
}
