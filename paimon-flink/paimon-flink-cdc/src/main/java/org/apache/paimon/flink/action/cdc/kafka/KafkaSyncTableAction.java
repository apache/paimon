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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.formats.DataFormat;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.assertSchemaCompatible;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.buildPaimonSchema;
import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;

/**
 * An {@link Action} which synchronize one kafka topic into one Paimon table.
 *
 * <p>This topic must be from canal-json format.
 *
 * <p>You should specify Kafka source topic in {@code kafkaConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/">document
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

    private final String database;
    private final String table;
    private final Configuration kafkaConfig;

    private List<String> partitionKeys = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();

    private Map<String, String> tableConfig = new HashMap<>();
    private List<String> computedColumnArgs = new ArrayList<>();
    private TypeMapping typeMapping = TypeMapping.defaultMapping();

    public KafkaSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> kafkaConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.table = table;
        this.kafkaConfig = Configuration.fromMap(kafkaConfig);
    }

    public KafkaSyncTableAction withPartitionKeys(String... partitionKeys) {
        return withPartitionKeys(Arrays.asList(partitionKeys));
    }

    public KafkaSyncTableAction withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public KafkaSyncTableAction withPrimaryKeys(String... primaryKeys) {
        return withPrimaryKeys(Arrays.asList(primaryKeys));
    }

    public KafkaSyncTableAction withPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public KafkaSyncTableAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public KafkaSyncTableAction withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    public KafkaSyncTableAction withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    @Override
    public void build(StreamExecutionEnvironment env) throws Exception {
        KafkaSource<String> source = KafkaActionUtils.buildKafkaSource(kafkaConfig);
        String topic = kafkaConfig.get(KafkaConnectorOptions.TOPIC).get(0);

        catalog.createDatabase(database, true);
        boolean caseSensitive = catalog.caseSensitive();
        Schema kafkaSchema =
                KafkaSchemaUtils.getKafkaSchema(kafkaConfig, topic, typeMapping, caseSensitive);

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        List<ComputedColumn> computedColumns =
                buildComputedColumns(computedColumnArgs, kafkaSchema);
        Schema fromKafka =
                buildPaimonSchema(
                        partitionKeys, primaryKeys, computedColumns, tableConfig, kafkaSchema);

        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            assertSchemaCompatible(table.schema(), fromKafka.fields());
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, fromKafka, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }
        DataFormat format = DataFormat.getDataFormat(kafkaConfig);
        RecordParser recordParser =
                format.createParser(caseSensitive, typeMapping, computedColumns);
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory =
                () -> new RichCdcMultiplexRecordEventParser(caseSensitive);

        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new CdcSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(
                                env.fromSource(
                                                source,
                                                WatermarkStrategy.noWatermarks(),
                                                "Kafka Source")
                                        .flatMap(recordParser))
                        .withParserFactory(parserFactory)
                        .withTable(table)
                        .withIdentifier(identifier)
                        .withCatalogLoader(catalogLoader());
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    @VisibleForTesting
    public Map<String, String> tableConfig() {
        return tableConfig;
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
