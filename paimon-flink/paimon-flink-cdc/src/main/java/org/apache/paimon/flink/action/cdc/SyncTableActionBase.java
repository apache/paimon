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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.assertSchemaCompatible;
import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Base {@link Action} for synchronizing into one Paimon table. */
public abstract class SyncTableActionBase extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(SyncTableActionBase.class);

    protected final String database;
    protected final String table;
    protected final Configuration cdcSourceConfig;
    protected FileStoreTable fileStoreTable;

    protected List<String> partitionKeys = new ArrayList<>();
    protected List<String> primaryKeys = new ArrayList<>();

    protected Map<String, String> tableConfig = new HashMap<>();
    protected List<String> computedColumnArgs = new ArrayList<>();
    protected TypeMapping typeMapping = TypeMapping.defaultMapping();

    protected List<ComputedColumn> computedColumns = new ArrayList<>();
    protected CdcMetadataConverter[] metadataConverters = new CdcMetadataConverter[] {};

    public SyncTableActionBase(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.table = table;
        this.cdcSourceConfig = Configuration.fromMap(cdcSourceConfig);
    }

    public SyncTableActionBase withPartitionKeys(String... partitionKeys) {
        return withPartitionKeys(Arrays.asList(partitionKeys));
    }

    public SyncTableActionBase withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public SyncTableActionBase withPrimaryKeys(String... primaryKeys) {
        return withPrimaryKeys(Arrays.asList(primaryKeys));
    }

    public SyncTableActionBase withPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public SyncTableActionBase withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public SyncTableActionBase withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    public SyncTableActionBase withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    public SyncTableActionBase withMetadataColumns(List<String> metadataColumns) {
        this.metadataConverters =
                metadataColumns.stream()
                        .map(this::metadataConverter)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .toArray(CdcMetadataConverter[]::new);
        return this;
    }

    protected Optional<CdcMetadataConverter<?>> metadataConverter(String column) {
        return Optional.empty();
    }

    protected void checkCdcSourceArgument() {}

    protected abstract Schema retrieveSchema() throws Exception;

    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
                partitionKeys,
                primaryKeys,
                computedColumns,
                tableConfig,
                retrievedSchema,
                metadataConverters,
                true);
    }

    protected abstract Source<String, ?, ?> buildSource() throws Exception;

    protected abstract String sourceName();

    protected abstract FlatMapFunction<String, RichCdcMultiplexRecord> recordParse();

    @Override
    public void build() throws Exception {
        checkCdcSourceArgument();
        catalog.createDatabase(database, true);

        boolean caseSensitive = catalog.caseSensitive();

        validateCaseInsensitive(caseSensitive);

        Identifier identifier = new Identifier(database, table);
        // Check if table exists before trying to get or create it
        if (catalog.tableExists(identifier)) {
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier).copy(tableConfig);
            try {
                Schema retrievedSchema = retrieveSchema();
                computedColumns =
                        buildComputedColumns(computedColumnArgs, retrievedSchema.fields());
                Schema paimonSchema = buildPaimonSchema(retrievedSchema);
                assertSchemaCompatible(fileStoreTable.schema(), paimonSchema.fields());
            } catch (SchemaRetrievalException e) {
                LOG.info(
                        "Failed to retrieve schema from record data but there exists specified Paimon table. "
                                + "Schema compatibility check will be skipped. If you have specified computed columns, "
                                + "here will use the existed Paimon table schema to build them. Please make sure "
                                + "the Paimon table has defined all the argument columns used for computed columns.");
                computedColumns =
                        buildComputedColumns(computedColumnArgs, fileStoreTable.schema().fields());
                // check partition keys and primary keys in case that user specified them
                checkConstraints();
            }
        } else {
            Schema retrievedSchema = retrieveSchema();
            computedColumns = buildComputedColumns(computedColumnArgs, retrievedSchema.fields());
            Schema paimonSchema = buildPaimonSchema(retrievedSchema);
            catalog.createTable(identifier, paimonSchema, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier).copy(tableConfig);
        }

        checkComputedColumns(computedColumns);

        DataStream<RichCdcMultiplexRecord> input =
                env.fromSource(buildSource(), WatermarkStrategy.noWatermarks(), sourceName())
                        .flatMap(recordParse())
                        .name("Parse");
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory =
                () -> new RichCdcMultiplexRecordEventParser(caseSensitive);

        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new CdcSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(input)
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

    protected void validateCaseInsensitive(boolean caseSensitive) {
        checkArgument(
                database.equals(database.toLowerCase()),
                String.format(
                        "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                        database));
        checkArgument(
                table.equals(table.toLowerCase()),
                String.format(
                        "Table name [%s] cannot contain upper case in case-insensitive catalog.",
                        table));
        for (String part : partitionKeys) {
            checkArgument(
                    part.equals(part.toLowerCase()),
                    String.format(
                            "Partition keys [%s] cannot contain upper case in case-insensitive catalog.",
                            partitionKeys));
        }
        for (String pk : primaryKeys) {
            checkArgument(
                    pk.equals(pk.toLowerCase()),
                    String.format(
                            "Primary keys [%s] cannot contain upper case in case-insensitive catalog.",
                            primaryKeys));
        }
    }

    protected void checkComputedColumns(List<ComputedColumn> computedColumns) {
        if (!computedColumns.isEmpty()) {
            List<String> computedFields =
                    computedColumns.stream()
                            .map(ComputedColumn::columnName)
                            .collect(Collectors.toList());
            List<String> fieldNames = fileStoreTable.schema().fieldNames();
            checkArgument(
                    new HashSet<>(fieldNames).containsAll(computedFields),
                    " Exists Table should contain all computed columns %s, but are %s.",
                    computedFields,
                    fieldNames);
        }
    }

    protected void checkConstraints() {
        if (!partitionKeys.isEmpty()) {
            List<String> actualPartitionKeys = fileStoreTable.partitionKeys();
            checkState(
                    actualPartitionKeys.size() == partitionKeys.size()
                            && actualPartitionKeys.containsAll(partitionKeys),
                    "Specified partition keys [%s] are not equal to the existed table partition keys [%s]. "
                            + "You should remove the --partition-keys argument or re-create the table if the partition keys are wrong.",
                    String.join(",", partitionKeys),
                    String.join(",", actualPartitionKeys));
        }

        if (!primaryKeys.isEmpty()) {
            List<String> actualPrimaryKeys = fileStoreTable.primaryKeys();
            checkState(
                    actualPrimaryKeys.size() == primaryKeys.size()
                            && actualPrimaryKeys.containsAll(primaryKeys),
                    "Specified primary keys [%s] are not equal to the existed table primary keys [%s]. "
                            + "You should remove the --primary-keys argument or re-create the table if the primary keys are wrong.",
                    String.join(",", primaryKeys),
                    String.join(",", actualPrimaryKeys));
        }
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
        build();
        execute(jobName());
    }

    /** Custom exception to indicate issues with schema retrieval. */
    public static class SchemaRetrievalException extends Exception {
        public SchemaRetrievalException(String message) {
            super(message);
        }
    }
}
