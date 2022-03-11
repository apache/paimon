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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RequireCatalogLock;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.StoreTableContext;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSinkTranslator;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.table.store.connector.utils.TableStoreUtils.createLogStoreContext;
import static org.apache.flink.table.store.connector.utils.TableStoreUtils.createLogStoreTableFactory;

/** Table sink to create {@link StoreSink}. */
public class StoreTableSink
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning, RequireCatalogLock {

    private final StoreTableContext storeTableContext;

    private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
    private boolean overwrite;
    @Nullable private CatalogLock.Factory lockFactory;

    public StoreTableSink(StoreTableContext storeTableContext) {
        this.storeTableContext = storeTableContext;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return storeTableContext.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return (DataStreamSinkProvider)
                (providerContext, dataStream) -> {
                    Transformation<RowData> transformation = dataStream.getTransformation();
                    return GlobalCommittingSinkTranslator.translate(
                            new DataStream<>(dataStream.getExecutionEnvironment(), transformation),
                            createStoreSink());
                };
    }

    @Override
    public DynamicTableSink copy() {
        StoreTableSink copied = new StoreTableSink(this.storeTableContext);
        copied.staticPartitions = new LinkedHashMap<>(this.staticPartitions);
        copied.overwrite = this.overwrite;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "StoreTableSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        this.storeTableContext
                .getPartitionKeys()
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

    // ~ Tools ------------------------------------------------------------------

    private StoreSink<?, ?> createStoreSink() {
        return new StoreSink<>(
                storeTableContext.tableIdentifier(),
                storeTableContext.fileStore(),
                storeTableContext.partitionIndex(),
                storeTableContext.primaryKeyIndex(),
                storeTableContext.numBucket(),
                lockFactory,
                overwrite ? staticPartitions : null,
                !storeTableContext.batchMode() && storeTableContext.enableChangeTracking()
                        ? createLogSinkProvider()
                        : null);
    }

    private LogSinkProvider createLogSinkProvider() {
        return createLogStoreTableFactory()
                .createSinkProvider(
                        createLogStoreContext(storeTableContext.getContext()),
                        new LogStoreTableFactory.SinkContext() {
                            @Override
                            public boolean isBounded() {
                                return false;
                            }

                            @Override
                            public <T> TypeInformation<T> createTypeInformation(
                                    DataType consumedDataType) {
                                return createTypeInformation(
                                        TypeConversions.fromDataToLogicalType(consumedDataType));
                            }

                            @Override
                            public <T> TypeInformation<T> createTypeInformation(
                                    LogicalType consumedLogicalType) {
                                return InternalTypeInfo.of(consumedLogicalType);
                            }

                            @Override
                            public DataStructureConverter createDataStructureConverter(
                                    DataType consumedDataType) {
                                return new SinkRuntimeProviderContext(isBounded())
                                        .createDataStructureConverter(consumedDataType);
                            }
                        });
    }
}
