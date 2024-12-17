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
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.assertSchemaCompatible;
import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Base {@link Action} for synchronizing into one Paimon table. */
public abstract class SyncTableActionBase extends SynchronizationActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(SyncTableActionBase.class);

    protected final String table;

    protected FileStoreTable fileStoreTable;
    protected List<String> partitionKeys = new ArrayList<>();
    protected List<String> primaryKeys = new ArrayList<>();
    protected List<String> computedColumnArgs = new ArrayList<>();
    protected List<ComputedColumn> computedColumns = new ArrayList<>();

    public SyncTableActionBase(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig,
            SyncJobHandler.SourceType sourceType) {
        super(
                warehouse,
                database,
                catalogConfig,
                cdcSourceConfig,
                new SyncJobHandler(sourceType, cdcSourceConfig, database, table));
        this.table = table;
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

    public SyncTableActionBase withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    protected abstract Schema retrieveSchema() throws Exception;

    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
                table,
                partitionKeys,
                primaryKeys,
                computedColumns,
                tableConfig,
                retrievedSchema,
                metadataConverters,
                caseSensitive,
                true,
                true);
    }

    @Override
    protected void validateCaseSensitivity() {
        CatalogUtils.validateCaseInsensitive(caseSensitive, "Database", database);
        CatalogUtils.validateCaseInsensitive(caseSensitive, "Table", table);
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        Identifier identifier = new Identifier(database, table);
        // Check if table exists before trying to get or create it
        try {
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            fileStoreTable = alterTableOptions(identifier, fileStoreTable);
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
                // schema evolution will add the computed columns
                computedColumns =
                        buildComputedColumns(
                                computedColumnArgs,
                                fileStoreTable.schema().fields(),
                                caseSensitive);
                // check partition keys and primary keys in case that user specified them
                checkConstraints();
            }
        } catch (Catalog.TableNotExistException e) {
            Schema retrievedSchema = retrieveSchema();
            computedColumns = buildComputedColumns(computedColumnArgs, retrievedSchema.fields());
            Schema paimonSchema = buildPaimonSchema(retrievedSchema);
            catalog.createTable(identifier, paimonSchema, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        }
    }

    @Override
    protected FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse() {
        return syncJobHandler.provideRecordParser(computedColumns, typeMapping, metadataConverters);
    }

    @Override
    protected EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory() {
        boolean caseSensitive = this.caseSensitive;
        return () -> new RichCdcMultiplexRecordEventParser(caseSensitive);
    }

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {
        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new CdcSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(input)
                        .withParserFactory(parserFactory)
                        .withTable(fileStoreTable)
                        .withIdentifier(new Identifier(database, table))
                        .withCatalogLoader(catalogLoader());
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void checkConstraints() {
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
    public FileStoreTable fileStoreTable() {
        return fileStoreTable;
    }

    /** Custom exception to indicate issues with schema retrieval. */
    public static class SchemaRetrievalException extends Exception {
        public SchemaRetrievalException(String message) {
            super(message);
        }
    }
}
