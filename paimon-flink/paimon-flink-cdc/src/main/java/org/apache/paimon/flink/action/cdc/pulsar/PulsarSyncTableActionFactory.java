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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.paimon.flink.action.cdc.SyncTableActionFactoryBase;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PULSAR_CONF;

/** Factory to create {@link PulsarSyncTableAction}. */
public class PulsarSyncTableActionFactory extends SyncTableActionFactoryBase {

    public static final String IDENTIFIER = "pulsar_sync_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String cdcConfigIdentifier() {
        return PULSAR_CONF;
    }

    @Override
    public PulsarSyncTableAction createAction() {
        return new PulsarSyncTableAction(database, table, this.catalogConfig, this.cdcSourceConfig);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"pulsar_sync_table\" creates a streaming job "
                        + "with a Flink Pulsar CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  pulsar_sync_table --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> "
                        + "[--partition_keys <partition_keys>] "
                        + "[--primary_keys <primary_keys>] "
                        + "[--type_mapping <option1,option2...>] "
                        + "[--computed_column <'column_name=expr_name(args[, ...])'> [--computed_column ...]] "
                        + "[--pulsar_conf <pulsar_source_conf> [--pulsar_conf <pulsar_source_conf> ...]] "
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
        System.out.println("Primary keys will be derived from tables if not specified.");
        System.out.println();

        System.out.println(
                "--type_mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed_column.");
        System.out.println();

        System.out.println("pulsar source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'topic', 'value.format', 'pulsar.client.serviceUrl', 'pulsar.admin.adminUrl', 'pulsar.consumer.subscriptionName' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/pulsar/#source-configurable-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  pulsar_sync_table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition_keys pt \\\n"
                        + "    --primary_keys pt,uid \\\n"
                        + "    --pulsar_conf topic=order \\\n"
                        + "    --pulsar_conf value.format=canal-json \\\n"
                        + "    --pulsar_conf pulsar.client.serviceUrl=pulsar://127.0.0.1:6650 \\\n"
                        + "    --pulsar_conf pulsar.admin.adminUrl=http://127.0.0.1:8080 \\\n"
                        + "    --pulsar_conf pulsar.consumer.subscriptionName=paimon-tests \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
