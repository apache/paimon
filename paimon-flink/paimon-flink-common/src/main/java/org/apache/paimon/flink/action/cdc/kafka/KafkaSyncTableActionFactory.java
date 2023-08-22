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

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Factory to create {@link KafkaSyncTableAction}. */
public class KafkaSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "kafka-sync-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);

        List<String> partitionKeys = Collections.emptyList();
        if (params.has("partition-keys")) {
            partitionKeys =
                    Arrays.stream(params.get("partition-keys").split(","))
                            .collect(Collectors.toList());
        }

        List<String> primaryKeys = Collections.emptyList();
        if (params.has("primary-keys")) {
            primaryKeys =
                    Arrays.stream(params.get("primary-keys").split(","))
                            .collect(Collectors.toList());
        }
        List<String> computedColumnArgs = Collections.emptyList();
        if (params.has("computed-column")) {
            computedColumnArgs = new ArrayList<>(params.getMultiParameter("computed-column"));
        }

        checkRequiredArgument(params, "kafka-conf");

        Map<String, String> kafkaConfig = optionalConfigMap(params, "kafka-conf");
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");
        Map<String, String> paimonConfig = optionalConfigMap(params, "paimon-conf");

        return Optional.of(
                new KafkaSyncTableAction(
                        kafkaConfig,
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        partitionKeys,
                        primaryKeys,
                        computedColumnArgs,
                        catalogConfig,
                        paimonConfig));
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"kafka-sync-table\" creates a streaming job "
                        + "with a Flink Kafka Canal CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  kafka-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--primary-keys <primary-keys>] "
                        + "[--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] "
                        + "[--kafka-conf <kafka-source-conf> [--kafka-conf <kafka-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
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

        System.out.println("Please see doc for usage of --computed-column.");
        System.out.println();

        System.out.println("kafka source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'topic', 'properties.bootstrap.servers', 'properties.group.id'"
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  kafka-sync-table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition-keys pt \\\n"
                        + "    --primary-keys pt,uid \\\n"
                        + "    --kafka-conf properties.bootstrap.servers=127.0.0.1:9020 \\\n"
                        + "    --kafka-conf topic=order \\\n"
                        + "    --kafka-conf properties.group.id=123456 \\\n"
                        + "    --kafka-conf value.format=canal-json \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
