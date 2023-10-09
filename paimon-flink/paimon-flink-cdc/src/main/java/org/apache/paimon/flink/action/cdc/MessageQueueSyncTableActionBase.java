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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.assertSchemaCompatible;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.buildPaimonSchema;
import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;

/**
 * Base {@link Action} for synchronizing one message queue topic into one Paimon table.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified topics. If the Paimon table already exists, its
 * schema will be compared against the schema of all specified topics.
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
public abstract class MessageQueueSyncTableActionBase extends ActionBase {

    protected final String database;
    protected final String table;
    protected final Configuration mqConfig;
    private FileStoreTable fileStoreTable;

    protected List<String> partitionKeys = new ArrayList<>();
    protected List<String> primaryKeys = new ArrayList<>();

    protected Map<String, String> tableConfig = new HashMap<>();
    protected List<String> computedColumnArgs = new ArrayList<>();
    protected TypeMapping typeMapping = TypeMapping.defaultMapping();

    public MessageQueueSyncTableActionBase(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mqConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.table = table;
        this.mqConfig = Configuration.fromMap(mqConfig);
    }

    public MessageQueueSyncTableActionBase withPartitionKeys(String... partitionKeys) {
        return withPartitionKeys(Arrays.asList(partitionKeys));
    }

    public MessageQueueSyncTableActionBase withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public MessageQueueSyncTableActionBase withPrimaryKeys(String... primaryKeys) {
        return withPrimaryKeys(Arrays.asList(primaryKeys));
    }

    public MessageQueueSyncTableActionBase withPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public MessageQueueSyncTableActionBase withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public MessageQueueSyncTableActionBase withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    public MessageQueueSyncTableActionBase withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    protected abstract Source<String, ?, ?> buildSource();

    protected abstract Schema buildSchema();

    protected abstract DataFormat getDataFormat();

    protected abstract String sourceName();

    @Override
    public void build(StreamExecutionEnvironment env) throws Exception {
        Source<String, ?, ?> source = buildSource();

        catalog.createDatabase(database, true);
        boolean caseSensitive = catalog.caseSensitive();
        // TODO: add case validate

        Schema mqSchema = buildSchema();

        Identifier identifier = new Identifier(database, table);
        List<ComputedColumn> computedColumns = buildComputedColumns(computedColumnArgs, mqSchema);
        Schema fromMq =
                buildPaimonSchema(
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        tableConfig,
                        mqSchema,
                        new CdcMetadataConverter[] {});

        try {
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            fileStoreTable = fileStoreTable.copy(tableConfig);
            assertSchemaCompatible(fileStoreTable.schema(), fromMq.fields());
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, fromMq, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        }
        DataFormat format = getDataFormat();
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
                                                sourceName())
                                        .flatMap(recordParser))
                        .withParserFactory(parserFactory)
                        .withTable(fileStoreTable)
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

    @VisibleForTesting
    public FileStoreTable fileStoreTable() {
        return fileStoreTable;
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    protected abstract String jobName();

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(jobName());
    }
}
