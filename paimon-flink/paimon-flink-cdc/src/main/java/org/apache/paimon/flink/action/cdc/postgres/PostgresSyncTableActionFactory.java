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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.cdc.SyncTableActionFactoryBase;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.POSTGRES_CONF;

/** Factory to create {@link PostgresSyncTableAction}. */
public class PostgresSyncTableActionFactory extends SyncTableActionFactoryBase {

    public static final String IDENTIFIER = "postgres_sync_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String cdcConfigIdentifier() {
        return POSTGRES_CONF;
    }

    @Override
    public PostgresSyncTableAction createAction() {
        return new PostgresSyncTableAction(
                database, table, this.catalogConfig, this.cdcSourceConfig);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"postgres_sync_table\" creates a streaming job "
                        + "with a Flink PostgreSQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  postgres_sync_table --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> "
                        + "--table <table_name> "
                        + "[--partition_keys <partition_keys>] "
                        + "[--primary_keys <primary_keys>] "
                        + "[--type_mapping <option1,option2...>] "
                        + "[--computed_column <'column_name=expr_name(args[, ...])'> [--computed_column ...]] "
                        + "[--metadata_column <metadata_column>] "
                        + "[--postgres_conf <postgres_cdc_source_conf> [--postgres_conf <postgres_cdc_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from PostgreSQL tables if not specified.");
        System.out.println();

        System.out.println(
                "--type_mapping is used to specify how to map PostgreSQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed_column.");
        System.out.println();

        System.out.println(
                "--metadata_column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println("PostgresSQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name', 'schema-name', 'table-name' and 'slot.name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  postgres_sync_table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition_keys pt \\\n"
                        + "    --primary_keys pt,uid \\\n"
                        + "    --postgres_conf hostname=127.0.0.1 \\\n"
                        + "    --postgres_conf username=root \\\n"
                        + "    --postgres_conf password=123456 \\\n"
                        + "    --postgres_conf database-name=source_db \\\n"
                        + "    --postgres_conf schema-name=public \\\n"
                        + "    --postgres_conf table-name='source_table' \\\n"
                        + "    --postgres_conf slot.name=flink \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
