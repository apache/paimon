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

package org.apache.paimon.flink.action.args;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.*;

public class MySqlSyncTableActionArgs extends ActionArgs {

    @Parameter(
            names = {"--warehouse"},
            description = "warehouse is the path to Paimon warehouse.")
    private String warehouse;

    @Parameter(
            names = {"--database"},
            description = "database is the database name in Paimon catalog.")
    private String database;

    @Parameter(
            names = {"--table"},
            description = "table is the Paimon table name.",
            required = true)
    private String table;

    @Parameter(
            names = {"--partition-keys"},
            description =
                    "partition-keys are the partition keys for Paimon table. If there are multiple partition keys, connect them with comma, for example dt hh mm.")
    private List<String> partitionKeys;

    @Parameter(
            names = {"--primary-keys"},
            description =
                    "primary-keys are the primary keys for Paimon table. If there are multiple primary keys, connect them with comma, for example buyer_id seller_id.")
    private List<String> primaryKeys;

    @DynamicParameter(
            names = {"--mysql-conf"},
            description =
                    "mysql-conf is the configuration for Flink CDC MySQL table sources. Each configuration should be specified in the format key=value. hostname, username, password, database-name and table-name are required configurations, others are optional.",
            required = true)
    private Map<String, String> mysqlConfig;

    @DynamicParameter(
            names = {"--paimon-conf"},
            description =
                    "paimon-conf is the configuration for Paimon table sink. Each configuration should be specified in the format key=value.")
    private Map<String, String> paimonConf;

    @Override
    public Optional<Action> buildAction() {

        return Optional.of(
                new MySqlSyncTableAction(
                        mysqlConfig,
                        warehouse,
                        database,
                        table,
                        partitionKeys,
                        primaryKeys,
                        Collections.emptyMap(),
                        Collections.emptyMap()));
    }

    @Override
    public StringBuilder usage() {
        return MySqlSyncTableAction.printHelp();
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public Map<String, String> getMysqlConfig() {
        return mysqlConfig;
    }

    public void setMysqlConfig(Map<String, String> mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    public Map<String, String> getPaimonConf() {
        return paimonConf;
    }

    public void setPaimonConf(Map<String, String> paimonConf) {
        this.paimonConf = paimonConf;
    }
}
