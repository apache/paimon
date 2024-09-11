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

import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.SyncTableActionFactoryBase;

import java.util.ArrayList;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.COMPUTED_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MONGODB_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PARTITION_KEYS;

/** Factory to create {@link MongoDBSyncTableAction}. */
public class MongoDBSyncTableActionFactory extends SyncTableActionFactoryBase {

    public static final String IDENTIFIER = "mongodb_sync_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String cdcConfigIdentifier() {
        return MONGODB_CONF;
    }

    @Override
    public SyncTableActionBase createAction() {
        return new MongoDBSyncTableAction(
                this.tablePath.f0,
                this.tablePath.f1,
                this.tablePath.f2,
                this.catalogConfig,
                this.cdcSourceConfig);
    }

    @Override
    protected void withParams(MultipleParameterToolAdapter params, SyncTableActionBase action) {
        if (params.has(PARTITION_KEYS)) {
            action.withPartitionKeys(params.get(PARTITION_KEYS).split(","));
        }

        if (params.has(COMPUTED_COLUMN)) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter(COMPUTED_COLUMN)));
        }
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mongodb_sync_table\" creates a streaming job "
                        + "with a Flink mongodb CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mongodb_sync_table --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> "
                        + "[--partition_keys <partition_keys>] "
                        + "[--computed_column <'column_name=expr_name(args[, ...])'> [--computed_column ...]] "
                        + "[--mongodb_conf <mongodb_cdc_source_conf> [--mongodb_conf <mongodb_cdc_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed_column.");
        System.out.println();

        System.out.println("mongodb CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hosts', 'username', 'password', 'database' and 'collection' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "The 'mongodb_conf' introduces the 'schema.start.mode' parameter on top of the MongoDB CDC source configuration. 'schema.start.mode' provides two modes: 'dynamic' (default) and 'specified'."
                        + "In 'dynamic' mode, MongoDB schema information is parsed at one level, which forms the basis for schema change evolution."
                        + "In 'specified' mode, synchronization takes place according to specified criteria."
                        + "This can be done by configuring 'field.name' to specify the synchronization fields and 'parser.path' to specify the JSON parsing path for those fields.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mongodb_sync_table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition_keys pt \\\n"
                        + "    --mongodb_conf hosts=127.0.0.1:27017 \\\n"
                        + "    --mongodb_conf username=root \\\n"
                        + "    --mongodb_conf password=123456 \\\n"
                        + "    --mongodb_conf database=source_db \\\n"
                        + "    --mongodb_conf collection='source_table' \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
