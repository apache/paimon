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
import org.apache.paimon.flink.action.cdc.kafka.canal.CanalRecordParser;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    public KafkaSyncTableAction(
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
            catalog.createTable(identifier, fromCanal, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }
        String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory;
        if ("canal-json".equals(format)) {
            parserFactory = RichCdcMultiplexRecordEventParser::new;
        } else {
            throw new UnsupportedOperationException("This format: " + format + " is not support.");
        }

        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new CdcSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(
                                env.fromSource(
                                                source,
                                                WatermarkStrategy.noWatermarks(),
                                                "Kafka Source")
                                        .flatMap(
                                                new CanalRecordParser(
                                                        caseSensitive, computedColumns)))
                        .withParserFactory(parserFactory)
                        .withTable(table)
                        .withIdentifier(identifier)
                        .withCatalogLoader(catalogLoader());
        String sinkParallelism = paimonConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("Kafka-Paimon Table Sync: %s.%s", database, table));
    }
}
