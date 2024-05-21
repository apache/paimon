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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionFactoryBase;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.KAFKA_CONF;

/** Factory to create {@link KafkaSyncDatabaseAction}. */
public class KafkaSyncDatabaseActionFactory
        extends SyncDatabaseActionFactoryBase<KafkaSyncDatabaseAction> {

    public static final String IDENTIFIER = "kafka_sync_database";
    private static final String MERGE_SHARDS = "merge_shards";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    protected String cdcConfigIdentifier() {
        return KAFKA_CONF;
    }

    @Override
    public KafkaSyncDatabaseAction createAction() {
        return new KafkaSyncDatabaseAction(warehouse, database, catalogConfig, cdcSourceConfig);
    }

    @Override
    protected void withParams(MultipleParameterToolAdapter params, KafkaSyncDatabaseAction action) {
        super.withParams(params, action);
        action.mergeShards(
                !params.has(MERGE_SHARDS) || Boolean.parseBoolean(params.get(MERGE_SHARDS)));
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"kafka_sync_database\" creates a streaming job "
                        + "with a Flink Kafka source and multiple Paimon table sinks "
                        + "to synchronize multiple tables into one Paimon database.\n"
                        + "Only tables with primary keys will be considered. ");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  kafka_sync_database --warehouse <warehouse_path> --database <database_name> "
                        + "[--table_prefix <paimon_table_prefix>] "
                        + "[--table_suffix <paimon_table_suffix>] "
                        + "[--including_tables <table_name|name_regular_expr>] "
                        + "[--excluding_tables <table_name|name_regular_expr>] "
                        + "[--type_mapping <option1,option2...>] "
                        + "[--kafka_conf <kafka_source_conf> [--kafka_conf <kafka_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
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
                "--type_mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println(
                "--merge_shards is default true, in this case, if some tables in different databases have the same name, "
                        + "their schemas will be merged and their records will be synchronized into one Paimon table. "
                        + "Otherwise, each table's records will be synchronized to a corresponding Paimon table, "
                        + "and the Paimon table will be named to 'databaseName_tableName' to avoid potential name conflict.");
        System.out.println();

        System.out.println("kafka source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'topic', 'properties.bootstrap.servers', 'properties.group.id', 'value.format' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/#connector-options");
        System.out.println();
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
                "  kafka_sync_database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --kafka_conf properties.bootstrap.servers=127.0.0.1:9020 \\\n"
                        + "    --kafka_conf topic=order\\;logistic\\;user \\\n"
                        + "    --kafka_conf properties.group.id=123456 \\\n"
                        + "    --kafka_conf value.format=canal-json \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
