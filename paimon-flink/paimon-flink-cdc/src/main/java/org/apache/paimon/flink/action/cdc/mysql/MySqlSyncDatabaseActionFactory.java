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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionFactoryBase;

import java.util.Arrays;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.METADATA_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MYSQL_CONF;

/** Factory to create {@link MySqlSyncDatabaseAction}. */
public class MySqlSyncDatabaseActionFactory
        extends SyncDatabaseActionFactoryBase<MySqlSyncDatabaseAction> {

    public static final String IDENTIFIER = "mysql_sync_database";

    private static final String IGNORE_INCOMPATIBLE = "ignore_incompatible";
    private static final String MERGE_SHARDS = "merge_shards";
    private static final String MODE = "mode";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    protected String cdcConfigIdentifier() {
        return MYSQL_CONF;
    }

    @Override
    public MySqlSyncDatabaseAction createAction() {
        return new MySqlSyncDatabaseAction(database, catalogConfig, cdcSourceConfig);
    }

    @Override
    protected void withParams(MultipleParameterToolAdapter params, MySqlSyncDatabaseAction action) {
        super.withParams(params, action);
        action.ignoreIncompatible(Boolean.parseBoolean(params.get(IGNORE_INCOMPATIBLE)))
                .mergeShards(
                        !params.has(MERGE_SHARDS) || Boolean.parseBoolean(params.get(MERGE_SHARDS)))
                .withMode(MultiTablesSinkMode.fromString(params.get(MODE)));
        if (params.has(METADATA_COLUMN)) {
            action.withMetadataColumns(Arrays.asList(params.get(METADATA_COLUMN).split(",")));
        }
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mysql_sync_database\" creates a streaming job "
                        + "with a Flink MySQL CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole MySQL database into one Paimon database.\n"
                        + "Only MySQL tables with primary keys will be considered. "
                        + "Newly created MySQL tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql_sync_database --warehouse <warehouse_path> --database <database_name> "
                        + "[--ignore_incompatible <true/false>] "
                        + "[--merge_shards <true/false>] "
                        + "[--table_prefix <paimon_table_prefix>] "
                        + "[--table_suffix <paimon_table_suffix>] "
                        + "[--including_tables <mysql_table_name|name_regular_expr>] "
                        + "[--excluding_tables <mysql_table_name|name_regular_expr>] "
                        + "[--mode <sync_mode>] "
                        + "[--metadata_column <metadata_column>] "
                        + "[--type_mapping <option1,option2...>] "
                        + "[--mysql_conf <mysql_cdc_source_conf> [--mysql_conf <mysql_cdc_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
        System.out.println();

        System.out.println(
                "--ignore_incompatible is default false, in this case, if MySQL table name exists in Paimon "
                        + "and their schema is incompatible, an exception will be thrown. "
                        + "You can specify it to true explicitly to ignore the incompatible tables and exception.");
        System.out.println();

        System.out.println(
                "--merge_shards is default true, in this case, if some tables in different databases have the same name, "
                        + "their schemas will be merged and their records will be synchronized into one Paimon table. "
                        + "Otherwise, each table's records will be synchronized to a corresponding Paimon table, "
                        + "and the Paimon table will be named to 'databaseName_tableName' to avoid potential name conflict.");
        System.out.println();

        System.out.println(
                "--table_prefix is the prefix of all Paimon tables to be synchronized. For example, if you want all "
                        + "synchronized tables to have \"ods_\" as prefix, you can specify `--table_prefix ods_`.");
        System.out.println("The usage of --table_suffix is same as `--table_prefix`");
        System.out.println();

        System.out.println(
                "--including_tables is used to specify which source tables are to be synchronized. "
                        + "You must use '|' to separate multiple tables. Regular expression is supported.");
        System.out.println(
                "--excluding_tables is used to specify which source tables are not to be synchronized. "
                        + "The usage is same as --including_tables.");
        System.out.println(
                "--excluding_tables has higher priority than --including_tables if you specified both.");
        System.out.println();

        System.out.println(
                "--mode is used to specify synchronization mode. You can specify two modes:");
        System.out.println(
                "  1. 'divided' (the default mode if you haven't specified one): "
                        + "start a sink for each table, the synchronization of the new table requires restarting the job;");
        System.out.println(
                "  2. 'combined': start a single combined sink for all tables, the new table will be automatically synchronized.");
        System.out.println();

        System.out.println(
                "--metadata_column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println(
                "--type_mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' and 'database-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the MySQL database you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println("All Paimon sink table will be applied the same set of configurations.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql_sync_database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --mysql_conf hostname=127.0.0.1 \\\n"
                        + "    --mysql_conf username=root \\\n"
                        + "    --mysql_conf password=123456 \\\n"
                        + "    --mysql_conf database-name=source_db \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
