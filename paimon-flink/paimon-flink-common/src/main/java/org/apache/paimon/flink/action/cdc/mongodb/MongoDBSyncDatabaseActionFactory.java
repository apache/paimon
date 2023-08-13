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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link MongoDBSyncDatabaseAction}. */
public class MongoDBSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mongodb-sync-database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "warehouse");
        checkRequiredArgument(params, "database");
        checkRequiredArgument(params, "mongodb-conf");

        String warehouse = params.get("warehouse");
        String database = params.get("database");
        boolean mergeShards =
                !params.has("merge-shards") || Boolean.parseBoolean(params.get("merge-shards"));
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> mongodbConfigOption = optionalConfigMap(params, "mongodb-conf");
        Map<String, String> catalogConfigOption = optionalConfigMap(params, "catalog-conf");
        Map<String, String> tableConfigOption = optionalConfigMap(params, "table-conf");
        return Optional.of(
                new MongoDBSyncDatabaseAction(
                        mongodbConfigOption,
                        warehouse,
                        database,
                        mergeShards,
                        tablePrefix,
                        tableSuffix,
                        includingTables,
                        excludingTables,
                        catalogConfigOption,
                        tableConfigOption));
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mongodb-sync-database\" creates a streaming job "
                        + "with a Flink MongoDB CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole MongoDB database into one Paimon database.\n"
                        + "Only MongoDB tables with primary keys will be considered. "
                        + "Newly created MongoDB tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mongodb-sync-database --warehouse <warehouse-path> --database <database-name> "
                        + "[--merge-shards <true/false>] "
                        + "[--table-prefix <paimon-table-prefix>] "
                        + "[--table-suffix <paimon-table-suffix>] "
                        + "[--including-tables <mongodb-table-name|name-regular-expr>] "
                        + "[--excluding-tables <mongodb-table-name|name-regular-expr>] "
                        + "[--mongodb-conf <mongodb-cdc-source-conf> [--mongodb-conf <mongodb-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
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

        System.out.println("MongoDB CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hosts', 'username', 'password' and 'database' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the MongoDB database you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html#connector-options");
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
                "  mongodb-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --mongodb-conf hosts=127.0.0.1 \\\n"
                        + "    --mongodb-conf username=root \\\n"
                        + "    --mongodb-conf password=123456 \\\n"
                        + "    --mongodb-conf database=source_db \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
