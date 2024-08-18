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

package org.apache.paimon.flink.action;

import org.apache.paimon.utils.TimeUtils;

import java.util.Optional;

/** Factory to create {@link CompactDatabaseAction}. */
public class CompactDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact_database";

    private static final String INCLUDING_DATABASES = "including_databases";
    private static final String INCLUDING_TABLES = "including_tables";
    private static final String EXCLUDING_TABLES = "excluding_tables";
    private static final String MODE = "mode";
    private static final String PARTITION_IDLE_TIME = "partition_idle_time";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        CompactDatabaseAction action =
                new CompactDatabaseAction(
                        getRequiredValue(params, WAREHOUSE),
                        optionalConfigMap(params, CATALOG_CONF));

        action.includingDatabases(params.get(INCLUDING_DATABASES))
                .includingTables(params.get(INCLUDING_TABLES))
                .excludingTables(params.get(EXCLUDING_TABLES))
                .withDatabaseCompactMode(params.get(MODE))
                .withTableOptions(optionalConfigMap(params, TABLE_CONF));
        String partitionIdleTime = params.get(PARTITION_IDLE_TIME);
        if (partitionIdleTime != null) {
            action.withPartitionIdleTime(TimeUtils.parseDuration(partitionIdleTime));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"compact_database\" runs a dedicated job for compacting one or multiple database.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact_database --warehouse <warehouse_path> --including_databases <database-name|name-regular-expr> "
                        + "[--including_tables <paimon_table_name|name_regular_expr>] "
                        + "[--excluding_tables <paimon_table_name|name_regular_expr>] "
                        + "[--mode <compact_mode>]"
                        + "[--partition_idle_time <partition_idle_time>]");
        System.out.println(
                "  compact_database --warehouse s3://path/to/warehouse --including_databases <database-name|name-regular-expr> "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]]");
        System.out.println();

        System.out.println(
                "--including_databases is used to specify which databases are to be compacted. "
                        + "You must use '|' to separate multiple databases, Regular expression is supported.");

        System.out.println(
                "--including_tables is used to specify which source tables are to be compacted. "
                        + "You must use '|' to separate multiple tables, the format is `databaseName.tableName`, Regular expression is supported.");
        System.out.println(
                "--excluding_tables is used to specify which source tables are not to be compacted. "
                        + "The usage is same as --including_tables.");
        System.out.println(
                "--excluding_tables has higher priority than --including_tables if you specified both.");
        System.out.println(
                "--mode is used to specify compaction mode. Possible values: divided, combined.");
        System.out.println(
                "--partition_idle_time is used to do a full compaction for partition which had not receive any new data for 'partition_idle_time' time. And only these partitions will be compacted.");
        System.out.println("--partition_idle_time is only supported in batch mode. ");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  compact_database --warehouse hdfs:///path/to/warehouse --including_databases test_db");
        System.out.println(
                "  compact_database --warehouse s3:///path/to/warehouse "
                        + "--including_databases test_db "
                        + "--catalog_conf s3.endpoint=https://****.com "
                        + "--catalog_conf s3.access-key=***** "
                        + "--catalog_conf s3.secret-key=***** ");
    }
}
