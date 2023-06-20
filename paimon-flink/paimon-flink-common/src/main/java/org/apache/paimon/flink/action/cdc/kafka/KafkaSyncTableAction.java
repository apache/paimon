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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.kafka.canal.CanalJsonEventParser;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.Action.optionalConfigMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one kafka topic into one Paimon table.
 *
 * <p>This topic must be from canal-json format.
 *
 * <p>You should specify Kafka source topic in {@code kafkaConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/kafka/">document
 * of flink-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified Kafka topic. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified Kafka topic.
 *
 * <p>This action supports a limited number of schema changes. Unsupported schema changes will be
 * ignored. Currently supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported. Other type changes will cause exceptions.
 * </ul>
 */
public class KafkaSyncTableAction extends ActionBase {

    private final Configuration kafkaConfig;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;

    private final List<String> computedColumnArgs;

    private final Map<String, String> paimonConfig;

    KafkaSyncTableAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> catalogConfig,
            Map<String, String> paimonConfig) {
        this(
                kafkaConfig,
                warehouse,
                database,
                table,
                partitionKeys,
                primaryKeys,
                Collections.emptyList(),
                catalogConfig,
                paimonConfig);
    }

    public KafkaSyncTableAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<String> computedColumnArgs,
            Map<String, String> catalogConfig,
            Map<String, String> paimonConfig) {
        super(warehouse, catalogConfig);
        this.kafkaConfig = Configuration.fromMap(kafkaConfig);
        this.database = database;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.computedColumnArgs = computedColumnArgs;
        this.paimonConfig = paimonConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(
                kafkaConfig.contains(KafkaConnectorOptions.VALUE_FORMAT),
                KafkaConnectorOptions.VALUE_FORMAT.key() + " cannot be null ");
        KafkaSource<String> source = KafkaActionUtils.buildKafkaSource(kafkaConfig);
        String topic = kafkaConfig.get(KafkaConnectorOptions.TOPIC).get(0);
        KafkaSchema kafkaSchema = KafkaSchema.getKafkaSchema(kafkaConfig, topic);

        catalog.createDatabase(database, true);
        boolean caseSensitive = catalog.caseSensitive();

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        List<ComputedColumn> computedColumns =
                KafkaActionUtils.buildComputedColumns(computedColumnArgs, kafkaSchema.fields());
        Schema fromCanal =
                KafkaActionUtils.buildPaimonSchema(
                        kafkaSchema,
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        paimonConfig,
                        caseSensitive);
        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            KafkaActionUtils.assertSchemaCompatible(table.schema(), fromCanal);
        } catch (Catalog.TableNotExistException e) {
            Schema schema =
                    KafkaActionUtils.buildPaimonSchema(
                            kafkaSchema,
                            partitionKeys,
                            primaryKeys,
                            computedColumns,
                            paimonConfig,
                            caseSensitive);
            catalog.createTable(identifier, schema, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }
        String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        EventParser.Factory<String> parserFactory;
        if ("canal-json".equals(format)) {
            parserFactory = () -> new CanalJsonEventParser(caseSensitive, computedColumns);
        } else {
            throw new UnsupportedOperationException("This format: " + format + " is not support.");
        }

        CdcSinkBuilder<String> sinkBuilder =
                new CdcSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "Kafka Source"))
                        .withParserFactory(parserFactory)
                        .withTable(table);
        String sinkParallelism = paimonConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static Optional<Action> create(String[] args) {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Tuple3<String, String, String> tablePath = Action.getTablePath(params);
        if (tablePath == null) {
            return Optional.empty();
        }

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

        if (!params.has("kafka-conf")) {
            return Optional.empty();
        }

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

    private static void printHelp() {
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
                        + "see https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/kafka/");
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

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("Kafka-Paimon Table Sync: %s.%s", database, table));
    }
}
