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

import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.utils.CommonTestUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HiveCatalog}. */
public class HiveCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        HiveConf hiveConf = new HiveConf();
        String jdoConnectionURL = "jdbc:derby:memory:" + UUID.randomUUID();
        hiveConf.setVar(METASTORECONNECTURLKEY, jdoConnectionURL + ";create=true");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
        try (MockedStatic<HiveCatalog> mocked = Mockito.mockStatic(HiveCatalog.class)) {
            mocked.when(() -> HiveCatalog.createClient(hiveConf, metastoreClientClass))
                    .thenReturn(metaStoreClient);
        }
        catalog = new HiveCatalog(fileIO, hiveConf, metastoreClientClass, warehouse);
    }

    @Test
    @Override
    public void testListDatabasesWhenNoDatabases() {
        // List databases returns an empty list when there are no databases
        List<String> databases = catalog.listDatabases();
        assertThat(databases).containsExactly("default");
    }

    @Test
    public void testCheckIdentifierUpperCase() throws Exception {
        catalog.createDatabase("test_db", false);
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("TEST_DB", "new_table"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage(
                        "Database name[TEST_DB] cannot contain upper case in hive catalog");

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("test_db", "NEW_TABLE"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage(
                        "Table name[NEW_TABLE] cannot contain upper case in hive catalog");
    }

    private static final String HADOOP_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hadoop-conf-dir").getPath();

    private static final String HIVE_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hive-conf-dir").getPath();

    @Test
    public void testHadoopConfDir() {
        HiveConf hiveConf = HiveCatalog.createHiveConf(null, HADOOP_CONF_DIR);
        assertThat(hiveConf.get("fs.defaultFS")).isEqualTo("dummy-fs");
    }

    @Test
    public void testHiveConfDir() {
        try {
            testHiveConfDirImpl();
        } finally {
            cleanUpHiveConfDir();
        }
    }

    private void testHiveConfDirImpl() {
        HiveConf hiveConf = HiveCatalog.createHiveConf(HIVE_CONF_DIR, null);
        assertThat(hiveConf.get("hive.metastore.uris")).isEqualTo("dummy-hms");
    }

    private void cleanUpHiveConfDir() {
        // reset back to default value
        HiveConf.setHiveSiteLocation(HiveConf.class.getClassLoader().getResource("hive-site.xml"));
    }

    @Test
    public void testHadoopConfDirFromEnv() {
        Map<String, String> newEnv = new HashMap<>(System.getenv());
        newEnv.put("HADOOP_CONF_DIR", HADOOP_CONF_DIR);
        // add HADOOP_CONF_DIR to system environment
        CommonTestUtils.setEnv(newEnv, false);

        HiveConf hiveConf = HiveCatalog.createHiveConf(null, null);
        assertThat(hiveConf.get("fs.defaultFS")).isEqualTo("dummy-fs");
    }

    @Test
    public void testHiveConfDirFromEnv() {
        try {
            testHiveConfDirFromEnvImpl();
        } finally {
            cleanUpHiveConfDir();
        }
    }

    private void testHiveConfDirFromEnvImpl() {
        Map<String, String> newEnv = new HashMap<>(System.getenv());
        newEnv.put("HIVE_CONF_DIR", HIVE_CONF_DIR);
        // add HIVE_CONF_DIR to system environment
        CommonTestUtils.setEnv(newEnv, false);

        HiveConf hiveConf = HiveCatalog.createHiveConf(null, null);
        assertThat(hiveConf.get("hive.metastore.uris")).isEqualTo("dummy-hms");
    }
}
