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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.paimon.hive.HiveCatalog.PAIMON_TABLE_IDENTIFIER;
import static org.apache.paimon.hive.HiveCatalog.TABLE_TYPE_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link HiveCatalog}. */
public class HiveCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        HiveConf hiveConf = new HiveConf();
        String jdoConnectionURL = "jdbc:derby:memory:" + UUID.randomUUID();
        hiveConf.setVar(METASTORECONNECTURLKEY, jdoConnectionURL + ";create=true");
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
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
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database name [TEST_DB] cannot contain upper case in the catalog.");

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("test_db", "NEW_TABLE"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table name [NEW_TABLE] cannot contain upper case in the catalog.");
    }

    private static final String HADOOP_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hadoop-conf-dir").getPath();

    private static final String HIVE_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hive-conf-dir").getPath();

    @Test
    public void testHadoopConfDir() {
        HiveConf hiveConf =
                HiveCatalog.createHiveConf(
                        null, HADOOP_CONF_DIR, HadoopUtils.getHadoopConfiguration(new Options()));
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
        HiveConf hiveConf =
                HiveCatalog.createHiveConf(
                        HIVE_CONF_DIR, null, HadoopUtils.getHadoopConfiguration(new Options()));
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

        HiveConf hiveConf =
                HiveCatalog.createHiveConf(
                        null, null, HadoopUtils.getHadoopConfiguration(new Options()));
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

        HiveConf hiveConf =
                HiveCatalog.createHiveConf(
                        null, null, HadoopUtils.getHadoopConfiguration(new Options()));
        assertThat(hiveConf.get("hive.metastore.uris")).isEqualTo("dummy-hms");
    }

    @Test
    public void testAlterDatabaseAddProperty() throws Exception {
        this.alterDatabaseAddPropertyWhenSupportAlter();
    }

    @Test
    public void testAlterDatabaseRemoveProperty() throws Exception {
        this.alterDatabaseRemovePropertyWhenSupportAlter();
    }

    @Test
    public void testAddHiveTableParameters() {
        try {
            // Create a new database for the test
            String databaseName = "test_db";
            catalog.createDatabase(databaseName, false);

            // Create a new table with Hive table parameters
            String tableName = "new_table";
            Map<String, String> options = new HashMap<>();
            options.put("hive.table.owner", "Jon");
            options.put("hive.storage.format", "ORC");
            options.put("snapshot.num-retained.min", "5");
            options.put("snapshot.time-retained", "1h");

            Schema addHiveTableParametersSchema =
                    new Schema(
                            Lists.newArrayList(
                                    new DataField(0, "pk", DataTypes.INT()),
                                    new DataField(1, "col1", DataTypes.STRING()),
                                    new DataField(2, "col2", DataTypes.STRING())),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            options,
                            "this is a hive table");

            catalog.createTable(
                    Identifier.create(databaseName, tableName),
                    addHiveTableParametersSchema,
                    false);

            Field clientField = HiveCatalog.class.getDeclaredField("clients");
            clientField.setAccessible(true);
            @SuppressWarnings("unchecked")
            ClientPool<IMetaStoreClient, TException> clients =
                    (ClientPool<IMetaStoreClient, TException>) clientField.get(catalog);
            Table table = clients.run(client -> client.getTable(databaseName, tableName));
            Map<String, String> tableProperties = table.getParameters();

            // Verify the transformed parameters
            assertThat(tableProperties).containsEntry("table.owner", "Jon");
            assertThat(tableProperties).containsEntry("storage.format", "ORC");
            assertThat(tableProperties).containsEntry("comment", "this is a hive table");
            assertThat(tableProperties)
                    .containsEntry(
                            TABLE_TYPE_PROP, PAIMON_TABLE_IDENTIFIER.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            fail("Test failed due to exception: " + e.getMessage());
        }
    }

    @Test
    public void testAlterHiveTableParameters() {
        try {
            // Create a new database for the test
            String databaseName = "test_db";
            catalog.createDatabase(databaseName, false);

            // Create a new table with Hive table parameters
            String tableName = "new_table";
            Map<String, String> options = new HashMap<>();
            options.put("hive.table.owner", "Jon");
            options.put("hive.storage.format", "ORC");

            Schema addHiveTableParametersSchema =
                    new Schema(
                            Lists.newArrayList(
                                    new DataField(0, "pk", DataTypes.INT()),
                                    new DataField(1, "col1", DataTypes.STRING()),
                                    new DataField(2, "col2", DataTypes.STRING())),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            options,
                            "");

            catalog.createTable(
                    Identifier.create(databaseName, tableName),
                    addHiveTableParametersSchema,
                    false);

            SchemaChange schemaChange1 = SchemaChange.setOption("hive.table.owner", "Hms");
            SchemaChange schemaChange2 =
                    SchemaChange.setOption("hive.table.create_time", "2024-01-22");
            catalog.alterTable(
                    Identifier.create(databaseName, tableName),
                    Arrays.asList(schemaChange1, schemaChange2),
                    false);

            Field clientField = HiveCatalog.class.getDeclaredField("clients");
            clientField.setAccessible(true);
            @SuppressWarnings("unchecked")
            ClientPool<IMetaStoreClient, TException> clients =
                    (ClientPool<IMetaStoreClient, TException>) clientField.get(catalog);
            Table table = clients.run(client -> client.getTable(databaseName, tableName));
            Map<String, String> tableProperties = table.getParameters();

            assertThat(tableProperties).containsEntry("table.owner", "Hms");
            assertThat(tableProperties).containsEntry("table.create_time", "2024-01-22");
        } catch (Exception e) {
            fail("Test failed due to exception: " + e.getMessage());
        }
    }

    @Test
    public void testListTablesLock() {
        try {
            String databaseName = "test_db";
            catalog.createDatabase(databaseName, false);

            Map<String, String> options = new HashMap<>();
            Schema addHiveTableParametersSchema =
                    new Schema(
                            Lists.newArrayList(
                                    new DataField(0, "pk", DataTypes.INT()),
                                    new DataField(1, "col1", DataTypes.STRING()),
                                    new DataField(2, "col2", DataTypes.STRING())),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            options,
                            "this is a hive table");

            for (int i = 0; i < 100; i++) {
                String tableName = "new_table" + i;
                catalog.createTable(
                        Identifier.create(databaseName, tableName),
                        addHiveTableParametersSchema,
                        false);
            }
            List<String> tables1 = new ArrayList<>();
            List<String> tables2 = new ArrayList<>();

            Thread thread1 =
                    new Thread(
                            () -> {
                                System.out.println(
                                        "First thread started at " + System.currentTimeMillis());
                                try {
                                    tables1.addAll(catalog.listTables(databaseName));
                                } catch (Catalog.DatabaseNotExistException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Thread thread2 =
                    new Thread(
                            () -> {
                                System.out.println(
                                        "Second thread started at " + System.currentTimeMillis());
                                try {
                                    tables2.addAll(catalog.listTables(databaseName));
                                } catch (Catalog.DatabaseNotExistException e) {
                                    throw new RuntimeException(e);
                                }
                            });

            thread1.start();
            thread2.start();

            long timeout = 5000;
            long startTime = System.currentTimeMillis();

            AtomicBoolean deadlockDetected = new AtomicBoolean(false);
            while (thread1.isAlive() || thread2.isAlive()) {
                if (System.currentTimeMillis() - startTime > timeout) {
                    deadlockDetected.set(true);
                    thread1.interrupt();
                    thread2.interrupt();
                    break;
                }

                Thread.sleep(100);
            }

            assertThat(deadlockDetected).isFalse();
            assertThat(tables1).size().isEqualTo(100);
            assertThat(tables1).containsAll(tables2);
            assertThat(tables2).containsAll(tables1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean supportsView() {
        return true;
    }

    @Override
    protected boolean supportsFormatTable() {
        return true;
    }

    @Test
    public void testCreateExternalTableWithLocation(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        HiveConf hiveConf = new HiveConf();
        String jdoConnectionURL = "jdbc:derby:memory:" + UUID.randomUUID();
        hiveConf.setVar(METASTORECONNECTURLKEY, jdoConnectionURL + ";create=true");
        hiveConf.set(CatalogOptions.TABLE_TYPE.key(), "external");
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
        HiveCatalog externalWarehouseCatalog =
                new HiveCatalog(fileIO, hiveConf, metastoreClientClass, warehouse);

        String externalTablePath = tempDir.toString();

        Schema schema =
                new Schema(
                        Lists.newArrayList(new DataField(0, "foo", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        ImmutableMap.of("path", externalTablePath),
                        "");

        Identifier identifier = Identifier.create("default", "my_table");
        externalWarehouseCatalog.createTable(identifier, schema, true);

        org.apache.paimon.table.Table table = externalWarehouseCatalog.getTable(identifier);
        assertThat(table.options())
                .extracting(CoreOptions.PATH.key())
                .isEqualTo("file:" + externalTablePath);

        externalWarehouseCatalog.close();
    }
}
