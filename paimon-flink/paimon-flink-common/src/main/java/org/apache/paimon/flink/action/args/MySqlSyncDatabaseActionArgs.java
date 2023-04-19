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
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class MySqlSyncDatabaseActionArgs extends ActionArgs {

    @Parameter(
            names = {"--warehouse"},
            description = "warehouse is the path to Paimon warehouse.")
    private String warehouse;

    @Parameter(
            names = {"--database"},
            description = "database is the database name in Paimon catalog.")
    private String database;

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
                new MySqlSyncDatabaseAction(
                        mysqlConfig,
                        warehouse,
                        database,
                        Collections.emptyMap(),
                        Collections.emptyMap()));
    }

    @Override
    public StringBuilder usage() {
        return MySqlSyncDatabaseAction.printHelp();
    }
}
