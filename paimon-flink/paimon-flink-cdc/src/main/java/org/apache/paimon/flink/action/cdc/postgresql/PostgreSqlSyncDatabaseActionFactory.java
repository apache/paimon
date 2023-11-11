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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link PostgreSqlSyncDatabaseAction}. */
public class PostgreSqlSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "postgresql-sync-database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "postgresql-conf");

        PostgreSqlSyncDatabaseAction action = new PostgreSqlSyncDatabaseAction(
                getRequiredValue(params, "warehouse"),
                getRequiredValue(params, "database"),
                optionalConfigMap(params, "catalog-conf"),
                optionalConfigMap(params, "postgresql-conf"));

        action.ignoreIncompatible(Boolean.parseBoolean(params.get("ignore-incompatible")))
                .withTablePrefix(params.get("table-prefix"))
                .withTableSuffix(params.get("table-suffix"))
                .includingTables(params.get("including-tables"))
                .excludingTables(params.get("excluding-tables"))
                .withTableConfig(optionalConfigMap(params, "table-conf"));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"postgresql-sync-database\" creates a streaming job "
                        + "with a Flink PostgreSQL CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole PostgreSQL database into one Paimon database.\n"
                        + "Only PostgreSQL tables with primary keys will be considered. "
                        + "Newly created PostgreSQL tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  postgresql-sync-database --warehouse <warehouse-path> --database <database-name> "
                        + "[--ignore-incompatible <true/false>] "
                        + "[--table-prefix <paimon-table-prefix>] "
                        + "[--table-suffix <paimon-table-suffix>] "
                        + "[--including-tables <postgresql-table-name|name-regular-expr>] "
                        + "[--excluding-tables <postgresql-table-name|name-regular-expr>] "
                        + "[--postgresql-conf <postgresql-cdc-source-conf> [--postgresql-conf <postgresql-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println(
                "--ignore-incompatible is default false, in this case, if PostgreSQL table name exists in Paimon "
                        + "and their schema is incompatible, an exception will be thrown. "
                        + "You can specify it to true explicitly to ignore the incompatible tables and exception.");
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

        System.out.println("PostgreSQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' , 'database-name' and 'schema-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the PostgreSQL databse you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#connector-options");
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
                "  postgresql-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --postgresql-conf hostname=127.0.0.1 \\\n"
                        + "    --postgresql-conf username=postgres \\\n"
                        + "    --postgresql-conf password=123456 \\\n"
                        + "    --postgresql-conf database-name=source_db \\\n"
                        + "    --postgresql-conf schema-name=public \\\n"
                        + "    --postgresql-conf slot.name=my_replication_slot \\\n"
                        + "    --postgresql-conf decoding.plugin.name=pgoutput \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
