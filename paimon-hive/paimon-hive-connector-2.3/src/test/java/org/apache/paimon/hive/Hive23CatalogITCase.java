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

import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.schema.SchemaManager;

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TXN_MANAGER;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for using Paimon {@link HiveCatalog} together with Paimon Hive 2.3 connector. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class Hive23CatalogITCase extends HiveCatalogITCaseBase {

    @HiveRunnerSetup
    private static final HiveRunnerConfig CONFIG =
            new HiveRunnerConfig() {
                {
                    // catalog lock needs txn manager
                    // hive-3.x requires a proper txn manager to create ACID table
                    getHiveConfSystemOverride()
                            .put(HIVE_TXN_MANAGER.varname, DbTxnManager.class.getName());
                    getHiveConfSystemOverride().put(HIVE_SUPPORT_CONCURRENCY.varname, "true");
                    // tell TxnHandler to prepare txn DB
                    getHiveConfSystemOverride().put(HIVE_IN_TEST.varname, "true");
                }
            };

    @Test
    public void testCustomMetastoreClient() throws Exception {
        path = folder.newFolder().toURI().toString();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironmentImpl.create(settings);
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'default-database' = 'test_db',",
                                "  'warehouse' = '" + path + "',",
                                "  'metastore.client.class' = '"
                                        + TestHiveMetaStoreClient.class.getName()
                                        + "'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive").await();
        assertThat(collect("SHOW DATABASES"))
                .isEqualTo(
                        Arrays.asList(
                                Row.of("default"),
                                Row.of("test_db"),
                                Row.of(TestHiveMetaStoreClient.MOCK_DATABASE)));
    }

    @Test
    public void testCreateExistTableInHive() throws Exception {
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE CATALOG my_hive_custom_client WITH (",
                        "  'type' = 'paimon',",
                        "  'metastore' = 'hive',",
                        "  'uri' = '',",
                        "  'default-database' = 'test_db',",
                        "  'warehouse' = '" + path + "',",
                        "  'metastore.client.class' = '"
                                + CreateFailHiveMetaStoreClient.class.getName()
                                + "'",
                        ")"));
        tEnv.executeSql("USE CATALOG my_hive_custom_client");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE hive_table(a INT, b INT, c INT, d INT)")
                                        .await())
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Could not execute CreateTable in path `my_hive_custom_client`.`test_db`.`hive_table`");
        assertThat(
                        new SchemaManager(
                                        LocalFileIO.create(),
                                        new org.apache.paimon.fs.Path(
                                                path, "test_db.db/hive_table"))
                                .listAllIds())
                .isEmpty();
    }

    @Test
    public void testAlterTableFailedInHive() throws Exception {
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_alter_hive WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'default-database' = 'test_db',",
                                "  'warehouse' = '" + path + "',",
                                "  'metastore.client.class' = '"
                                        + AlterFailHiveMetaStoreClient.class.getName()
                                        + "'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_alter_hive").await();
        tEnv.executeSql("CREATE TABLE alter_failed_table(a INT, b STRING)").await();

        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE alter_failed_table SET ('aa'='bb')"))
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Could not execute AlterTable in path `my_alter_hive`.`test_db`.`alter_failed_table`"));

        assertThat(
                        new SchemaManager(
                                        LocalFileIO.create(),
                                        new org.apache.paimon.fs.Path(
                                                path, "test_db.db/alter_failed_table"))
                                .latest()
                                .get()
                                .options())
                .isEmpty();
    }
}
