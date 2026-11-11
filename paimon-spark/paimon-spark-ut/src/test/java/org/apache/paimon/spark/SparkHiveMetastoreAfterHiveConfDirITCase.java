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

package org.apache.paimon.spark;

import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.TestHiveMetastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Hive catalog created with 'hive-conf-dir' clears HiveConf's JVM-wide hive-site.xml location. A
 * {@link TestHiveMetastore} started later in the same JVM no longer reads the test hive-site.xml,
 * and Spark's DROP DATABASE must still work against it.
 *
 * <p>This is an ITCase so that it runs in its own JVM: the metastore's transaction handler sets up
 * its connection pool once per JVM, and an earlier test in a shared fork may already have set it up
 * with DBCP.
 */
public class SparkHiveMetastoreAfterHiveConfDirITCase {

    @Test
    public void testMetastoreStartedAfterHiveConfDirCatalog(@TempDir Path tempDir)
            throws Exception {
        URL hiveSiteLocation = HiveConf.getHiveSiteLocation();
        boolean loadMetastoreConfig = HiveConf.isLoadMetastoreConfig();
        boolean loadHiveServer2Config = HiveConf.isLoadHiveServer2Config();
        try {
            createHiveConfFromDir(tempDir.resolve("hive-conf"));

            TestHiveMetastore metastore = new TestHiveMetastore();
            metastore.start(0);
            try {
                SparkSession spark =
                        SparkSession.builder()
                                .config("spark.sql.warehouse.dir", "file:" + tempDir.resolve("wh"))
                                .config("spark.sql.catalogImplementation", "hive")
                                .config(
                                        "hive.metastore.uris",
                                        "thrift://localhost:" + metastore.getPort())
                                .master("local[2]")
                                .getOrCreate();
                try {
                    spark.sql("CREATE DATABASE conf_dir_db");
                    spark.sql("DROP DATABASE conf_dir_db");
                    assertThat(spark.catalog().databaseExists("conf_dir_db")).isFalse();
                } finally {
                    spark.stop();
                }
            } finally {
                metastore.stop();
            }
        } finally {
            HiveConf.setHiveSiteLocation(hiveSiteLocation);
            HiveConf.setLoadMetastoreConfig(loadMetastoreConfig);
            HiveConf.setLoadHiveServer2Config(loadHiveServer2Config);
        }
    }

    private static void createHiveConfFromDir(Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.write(
                dir.resolve("hive-site.xml"),
                "<configuration></configuration>".getBytes(StandardCharsets.UTF_8));
        HiveCatalog.createHiveConf(dir.toString(), null, new Configuration());
    }
}
