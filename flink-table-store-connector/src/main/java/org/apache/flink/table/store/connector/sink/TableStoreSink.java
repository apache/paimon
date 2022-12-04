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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.CoreOptions.ChangelogProducer;
import org.apache.flink.table.store.CoreOptions.LogChangelogMode;
import org.apache.flink.table.store.connector.FlinkConnectorOptions;
import org.apache.flink.table.store.connector.TableStoreDataStreamSinkProvider;
import org.apache.flink.table.store.file.catalog.CatalogLock;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.AppendOnlyFileStoreTable;
import org.apache.flink.table.store.table.ChangelogValueCountFileStoreTable;
import org.apache.flink.table.store.table.ChangelogWithKeyFileStoreTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.store.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.flink.table.store.CoreOptions.LOG_CHANGELOG_MODE;

/** Table sink to create {@link StoreSink}. */
public class TableStoreSink implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final DynamicTableFactory.Context context;
    @Nullable private final LogStoreTableFactory logStoreTableFactory;

    private Map<String, String> staticPartitions = new HashMap<>();
    private boolean overwrite = false;
    @Nullable private CatalogLock.Factory lockFactory;

    public TableStoreSink(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (table instanceof AppendOnlyFileStoreTable) {
            // Don't check this, for example, only inserts are available from the database, but the
            // plan phase contains all changelogs
            return requestedMode;
        } else if (table instanceof ChangelogValueCountFileStoreTable) {
            // no primary key, sink all changelogs
            return requestedMode;
        } else if (table instanceof ChangelogWithKeyFileStoreTable) {
            Configuration options = Configuration.fromMap(table.schema().options());
            if (options.get(CHANGELOG_PRODUCER) == ChangelogProducer.INPUT) {
                return requestedMode;
            }

            if (options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL) {
                return requestedMode;
            }

            // with primary key, default sink upsert
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown FileStoreTable subclass " + table.getClass().getName());
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (overwrite && !context.isBounded()) {
            throw new UnsupportedOperationException(
                    "Table store doesn't support streaming INSERT OVERWRITE.");
        }

        LogSinkProvider logSinkProvider = null;
        if (logStoreTableFactory != null) {
            logSinkProvider = logStoreTableFactory.createSinkProvider(this.context, context);
        }

        Configuration conf = Configuration.fromMap(table.schema().options());
        // Do not sink to log store when overwrite mode
        final LogSinkFunction logSinkFunction =
                overwrite || conf.get(FlinkConnectorOptions.COMPACTION_MANUAL_TRIGGERED)
                        ? null
                        : (logSinkProvider == null ? null : logSinkProvider.createSink());
        return new TableStoreDataStreamSinkProvider(
                (dataStream) ->
                        new FlinkSinkBuilder(table)
                                .withInput(
                                        new DataStream<>(
                                                dataStream.getExecutionEnvironment(),
                                                dataStream.getTransformation()))
                                .withLockFactory(
                                        Lock.factory(lockFactory, tableIdentifier.toObjectPath()))
                                .withLogSinkFunction(logSinkFunction)
                                .withOverwritePartition(overwrite ? staticPartitions : null)
                                .withParallelism(conf.get(FlinkConnectorOptions.SINK_PARALLELISM))
                                .build());
    }

    @Override
    public DynamicTableSink copy() {
        TableStoreSink copied =
                new TableStoreSink(tableIdentifier, table, context, logStoreTableFactory);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        copied.lockFactory = lockFactory;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "TableStoreSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.schema()
                .partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setLockFactory(@Nullable CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
    }
}
