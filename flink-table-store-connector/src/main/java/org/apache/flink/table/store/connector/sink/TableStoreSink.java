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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RequireCatalogLock;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.store.connector.TableStore;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Table sink to create {@link StoreSink}. */
public class TableStoreSink
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning, RequireCatalogLock {

    private final TableStore tableStore;
    private final LogOptions.LogChangelogMode logChangelogMode;
    @Nullable private final LogSinkProvider logSinkProvider;

    private Map<String, String> staticPartitions = new HashMap<>();
    private boolean overwrite;
    @Nullable private CatalogLock.Factory lockFactory;

    public TableStoreSink(
            TableStore tableStore,
            LogOptions.LogChangelogMode logChangelogMode,
            @Nullable LogSinkProvider logSinkProvider) {
        this.tableStore = tableStore;
        this.logChangelogMode = logChangelogMode;
        this.logSinkProvider = logSinkProvider;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!tableStore.valueCountMode()
                && logChangelogMode == LogOptions.LogChangelogMode.UPSERT) {
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return (DataStreamSinkProvider)
                (providerContext, dataStream) ->
                        tableStore
                                .sinkBuilder()
                                .withInput(
                                        new DataStream<>(
                                                dataStream.getExecutionEnvironment(),
                                                dataStream.getTransformation()))
                                .withLockFactory(lockFactory)
                                .withLogSinkProvider(logSinkProvider)
                                .withOverwritePartition(staticPartitions)
                                .build();
    }

    @Override
    public DynamicTableSink copy() {
        TableStoreSink copied = new TableStoreSink(tableStore, logChangelogMode, logSinkProvider);
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
        tableStore
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

    @Override
    public void setLockFactory(@Nullable CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
    }
}
