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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.EXCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.INCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.METADATA_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.SQLSERVER_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_PREFIX;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_SUFFIX;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TYPE_MAPPING;

/** Factory to create {@link SqlServerSyncDatabaseAction}. */
public class SqlServerSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "sqlserver_sync_database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        checkRequiredArgument(params, SQLSERVER_CONF);

        SqlServerSyncDatabaseAction action =
                new SqlServerSyncDatabaseAction(
                        getRequiredValue(params, WAREHOUSE),
                        getRequiredValue(params, DATABASE),
                        optionalConfigMap(params, CATALOG_CONF),
                        optionalConfigMap(params, SQLSERVER_CONF));

        action.withTableConfig(optionalConfigMap(params, TABLE_CONF));

        action.ignoreIncompatible(Boolean.parseBoolean(params.get("ignore_incompatible")))
                .mergeShards(
                        !params.has("merge_shards")
                                || Boolean.parseBoolean(params.get("merge_shards")))
                .withTablePrefix(params.get(TABLE_PREFIX))
                .withTableSuffix(params.get(TABLE_SUFFIX))
                .includingTables(params.get(INCLUDING_TABLES))
                .excludingTables(params.get(EXCLUDING_TABLES))
                .withMode(MultiTablesSinkMode.fromString(params.get("mode")));
        if (params.has(METADATA_COLUMN)) {
            action.withMetadataColumns(Arrays.asList(params.get(METADATA_COLUMN).split(",")));
        }

        if (params.has(TYPE_MAPPING)) {
            String[] options = params.get(TYPE_MAPPING).split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"sqlserver-sync-database\" creates a streaming job "
                        + "with a Flink SqlServer CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole SqlServer database into one Paimon database.\n"
                        + "Only SqlServer tables with primary keys will be considered. "
                        + "Newly created SqlServer tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  sqlserver-sync-database --warehouse <warehouse-path> --database <database-name> "
                        + "[--ignore-incompatible <true/false>] "
                        + "[--merge-shards <true/false>] "
                        + "[--table-prefix <paimon-table-prefix>] "
                        + "[--table-suffix <paimon-table-suffix>] "
                        + "[--including-tables <SqlServer-table-name|name-regular-expr>] "
                        + "[--excluding-tables <SqlServer-table-name|name-regular-expr>] "
                        + "[--mode <sync-mode>] "
                        + "[--metadata-column <metadata-column>] "
                        + "[--type-mapping <option1,option2...>] "
                        + "[--sqlserver-conf <sqlserver-cdc-source-conf> [--sqlserver-conf <sqlserver-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println(
                "--ignore-incompatible is default false, in this case, if SqlServer table name exists in Paimon "
                        + "and their schema is incompatible, an exception will be thrown. "
                        + "You can specify it to true explicitly to ignore the incompatible tables and exception.");
        System.out.println();

        System.out.println(
                "--merge-shards is default true, in this case, if some tables in different databases have the same name, "
                        + "their schemas will be merged and their records will be synchronized into one Paimon table. "
                        + "Otherwise, each table's records will be synchronized to a corresponding Paimon table, "
                        + "and the Paimon table will be named to 'databaseName_tableName' to avoid potential name conflict.");
        System.out.println();

        System.out.println(
                "--table-prefix is the prefix of all Paimon tables to be synchronized. For example, if you want all "
                        + "synchronized tables to have \"ods_\" as prefix, you can specify `--table-prefix ods_`.");
        System.out.println("The usage of --table-suffix is same as `--table-prefix`");
        System.out.println();

        System.out.println(
                "--including-tables is used to specify which source tables are to be synchronized. "
                        + "You must use '|' to separate multiple tables. Regular expression is supported.");
        System.out.println(
                "--excluding-tables is used to specify which source tables are not to be synchronized. "
                        + "The usage is same as --including-tables.");
        System.out.println(
                "--excluding-tables has higher priority than --including-tables if you specified both.");
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
                "--metadata-column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println(
                "--type-mapping is used to specify how to map SqlServer type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("SqlServer CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' and 'database-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the SqlServer database you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/sqlserver-cdc.html#connector-options");
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
                "  sqlserver-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --sqlserver-conf hostname=127.0.0.1 \\\n"
                        + "    --sqlserver-conf username=root \\\n"
                        + "    --sqlserver-conf password=123456 \\\n"
                        + "    --sqlserver-conf database-name=source_db \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
