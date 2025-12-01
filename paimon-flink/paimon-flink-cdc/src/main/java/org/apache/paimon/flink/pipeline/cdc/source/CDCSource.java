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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.pipeline.cdc.source.enumerator.CDCCheckpoint;
import org.apache.paimon.flink.pipeline.cdc.source.enumerator.CDCSourceEnumerator;
import org.apache.paimon.flink.pipeline.cdc.source.reader.CDCSourceReader;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.getSchemaDifference;
import static org.apache.paimon.disk.IOManagerImpl.splitPaths;
import static org.apache.paimon.flink.pipeline.cdc.util.CDCUtils.createCatalog;
import static org.apache.paimon.flink.pipeline.cdc.util.PaimonToFlinkCDCTypeConverter.convertPaimonSchemaToFlinkCDCSchema;

/** The {@link Source} that integrates with Flink CDC framework. */
public class CDCSource implements Source<Event, TableAwareFileStoreSourceSplit, CDCCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final CatalogContext catalogContext;
    private final Configuration cdcConfig;
    private final org.apache.flink.configuration.Configuration flinkConfig;

    public CDCSource(
            CatalogContext catalogContext,
            Configuration cdcConfig,
            org.apache.flink.configuration.Configuration flinkConfig) {
        this.catalogContext = catalogContext;
        this.cdcConfig = cdcConfig;
        this.flinkConfig = flinkConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> createEnumerator(
            SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context) {
        return restoreEnumerator(context, null);
    }

    @Override
    public SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context,
            @Nullable CDCCheckpoint checkpoint) {
        return new CDCSourceEnumerator(
                context,
                flinkConfig,
                catalogContext
                        .options()
                        .get(org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                        .toMillis(),
                catalogContext,
                cdcConfig,
                checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> getSplitSerializer() {
        return new TableAwareFileStoreSourceSplit.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<CDCCheckpoint> getEnumeratorCheckpointSerializer() {
        return new CDCCheckpoint.Serializer();
    }

    @Override
    public SourceReader<Event, TableAwareFileStoreSourceSplit> createReader(
            SourceReaderContext context) {
        IOManager ioManager =
                IOManager.create(splitPaths(context.getConfiguration().get(CoreOptions.TMP_DIRS)));
        SourceReaderMetricGroup metricGroup = context.metricGroup();
        FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(metricGroup);

        Catalog catalog = createCatalog(catalogContext, flinkConfig);
        TableManager manager = new TableManager(catalog, ioManager, metricGroup);
        return new CDCSourceReader(context, sourceReaderMetrics, ioManager, manager);
    }

    /** A manager for information related to the tables. */
    public static class TableManager {
        private final Map<Identifier, FileStoreTable> tableMap = new HashMap<>();
        private final Map<Tuple2<Identifier, Long>, TableSchema> tableSchemaMap = new HashMap<>();
        private final Map<Tuple2<Identifier, Long>, TableRead> tableReadMap = new HashMap<>();
        private final Catalog catalog;
        private final IOManager ioManager;
        private final SourceReaderMetricGroup metricGroup;

        protected TableManager(
                Catalog catalog, IOManager ioManager, SourceReaderMetricGroup metricGroup) {
            this.catalog = catalog;
            this.ioManager = ioManager;
            this.metricGroup = metricGroup;
        }

        public @Nullable TableSchema getTableSchema(
                Identifier identifier, @Nullable Long schemaId) {
            if (schemaId == null) {
                return null;
            }

            Tuple2<Identifier, Long> cacheKey = Tuple2.of(identifier, schemaId);
            if (tableSchemaMap.containsKey(cacheKey)) {
                return tableSchemaMap.get(cacheKey);
            }

            FileStoreTable table = getTable(identifier);
            TableSchema tableSchema = table.schemaManager().schema(schemaId);
            tableSchemaMap.put(cacheKey, tableSchema);
            return tableSchema;
        }

        public TableRead getTableRead(Identifier identifier, TableSchema schema) {
            Tuple2<Identifier, Long> cacheKey = Tuple2.of(identifier, schema.id());
            if (tableReadMap.containsKey(cacheKey)) {
                return tableReadMap.get(cacheKey);
            }

            FileStoreTable table = getTable(identifier).copy(schema);
            TableRead tableRead =
                    table.newReadBuilder()
                            .newRead()
                            .withIOManager(ioManager)
                            .withMetricRegistry(new FlinkMetricRegistry(metricGroup));
            tableReadMap.put(cacheKey, tableRead);
            return tableRead;
        }

        public List<SchemaChangeEvent> generateSchemaChangeEventList(
                Identifier identifier, @Nullable Long lastSchemaId, long schemaId) {
            if (lastSchemaId != null && lastSchemaId.equals(schemaId)) {
                return Collections.emptyList();
            }

            return getSchemaDifference(
                    TableId.tableId(identifier.getDatabaseName(), identifier.getTableName()),
                    convertPaimonSchemaToFlinkCDCSchema(getTableSchema(identifier, lastSchemaId)),
                    convertPaimonSchemaToFlinkCDCSchema(getTableSchema(identifier, schemaId)));
        }

        private FileStoreTable getTable(Identifier identifier) {
            if (tableMap.containsKey(identifier)) {
                return tableMap.get(identifier);
            }

            Table table;
            try {
                table = catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }

            // As the table should have just been checked by the enumerator, it is supposed to exist
            // and be a FileStoreTable.
            tableMap.put(identifier, (FileStoreTable) table);
            return (FileStoreTable) table;
        }
    }
}
