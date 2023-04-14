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

import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.s3.MinioTestContainer;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.junit.Before;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.runner.RunWith;

import java.util.UUID;

/** ITCase for using S3 in Hive. */
@RunWith(PaimonEmbeddedHiveRunner.class)
@ExtendWith(ParameterizedTestExtension.class)
public class HiveS3ITCase extends HiveCatalogITCaseBase {

    @RegisterExtension
    public static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    @Before
    public void before() throws Exception {
        String path = MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        Path warehousePath = new Path(path);
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
        hiveShell.execute("CREATE TABLE hive_table ( a INT, b STRING )");
        hiveShell.execute("INSERT INTO hive_table VALUES (100, 'Hive'), (200, 'Table')");
        hiveShell.executeQuery("SHOW TABLES");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironmentImpl.create(settings);
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'hive-conf-dir' = '"
                                        + hiveShell.getBaseDir().getRoot().getPath()
                                        + HIVE_CONF
                                        + "',",
                                "  'warehouse' = '" + warehousePath + "',",
                                "  'lock.enabled' = 'true'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive").await();
        tEnv.executeSql("USE test_db").await();
    }
}
