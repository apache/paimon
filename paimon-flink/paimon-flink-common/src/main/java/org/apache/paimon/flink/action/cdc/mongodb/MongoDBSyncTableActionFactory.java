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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Optional;

/** Factory to create {@link MongoDBSyncTableAction}. */
public class MongoDBSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mongodb-sync-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        checkRequiredArgument(params, "mongodb-conf");

        MongoDBSyncTableAction.Builder builder =
                new MongoDBSyncTableAction.Builder(
                                tablePath.f0,
                                tablePath.f1,
                                tablePath.f2,
                                optionalConfigMap(params, "catalog-conf"),
                                optionalConfigMap(params, "mongodb-conf"))
                        .withTableConfig(optionalConfigMap(params, "table-conf"));

        if (params.has("partition-keys")) {
            builder.withPartitionKeys(params.get("partition-keys").split(","));
        }

        if (params.has("computed-column")) {
            builder.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter("computed-column")));
        }
        return Optional.ofNullable(builder.buildAction());
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mongodb-sync-table\" creates a streaming job "
                        + "with a Flink mongodb CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mongodb-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] "
                        + "[--mongodb-conf <mongodb-cdc-source-conf> [--mongodb-conf <mongodb-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed-column.");
        System.out.println();

        System.out.println("mongodb CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hosts', 'username', 'password', 'database' and 'collection' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "The 'mongodb-conf' introduces the 'schema.start.mode' parameter on top of the MongoDB CDC source configuration. 'schema.start.mode' provides two modes: 'dynamic' (default) and 'specified'."
                        + "In 'dynamic' mode, MongoDB schema information is parsed at one level, which forms the basis for schema change evolution."
                        + "In 'specified' mode, synchronization takes place according to specified criteria."
                        + "This can be done by configuring 'field.name' to specify the synchronization fields and 'parser.path' to specify the JSON parsing path for those fields.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mongodb-sync-table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition-keys pt \\\n"
                        + "    --mongodb-conf hosts=127.0.0.1:27017 \\\n"
                        + "    --mongodb-conf username=root \\\n"
                        + "    --mongodb-conf password=123456 \\\n"
                        + "    --mongodb-conf database=source_db \\\n"
                        + "    --mongodb-conf collection='source_table' \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
