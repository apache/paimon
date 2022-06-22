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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.TableStoreFactoryOptions;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.AppendOnlyFileStoreTable;
import org.apache.flink.table.store.table.ChangelogValueCountFileStoreTable;
import org.apache.flink.table.store.table.ChangelogWithKeyFileStoreTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Table source to create {@link FileStoreSource} under batch mode or change-tracking is disabled.
 * For streaming mode with change-tracking enabled and FULL scan mode, it will create a {@link
 * org.apache.flink.connector.base.source.hybrid.HybridSource} of {@link FileStoreSource} and kafka
 * log source created by {@link LogSourceProvider}.
 */
public class TableStoreSource
        implements ScanTableSource, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final boolean streaming;
    private final DynamicTableFactory.Context logStoreContext;
    @Nullable private final LogStoreTableFactory logStoreTableFactory;

    @Nullable private Predicate predicate;
    @Nullable private int[][] projectFields;

    public TableStoreSource(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            boolean streaming,
            DynamicTableFactory.Context logStoreContext,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.streaming = streaming;
        this.logStoreContext = logStoreContext;
        this.logStoreTableFactory = logStoreTableFactory;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        if (table instanceof AppendOnlyFileStoreTable) {
            return ChangelogMode.insertOnly();
        } else if (table instanceof ChangelogValueCountFileStoreTable) {
            return ChangelogMode.all();
        } else if (table instanceof ChangelogWithKeyFileStoreTable) {
            // optimization: transaction consistency and all changelog mode avoid the generation of
            // normalized nodes. See TableStoreSink.getChangelogMode validation.
            Configuration logOptions =
                    new DelegatingConfiguration(
                            Configuration.fromMap(table.schema().options()), LogOptions.LOG_PREFIX);
            return logOptions.get(LogOptions.CONSISTENCY) == LogOptions.LogConsistency.TRANSACTIONAL
                            && logOptions.get(LogOptions.CHANGELOG_MODE)
                                    == LogOptions.LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown FileStoreTable subclass " + table.getClass().getName());
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        LogSourceProvider logSourceProvider = null;
        if (logStoreTableFactory != null) {
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(
                            logStoreContext,
                            new LogStoreTableFactory.SourceContext() {
                                @Override
                                public <T> TypeInformation<T> createTypeInformation(
                                        DataType producedDataType) {
                                    return scanContext.createTypeInformation(producedDataType);
                                }

                                @Override
                                public <T> TypeInformation<T> createTypeInformation(
                                        LogicalType producedLogicalType) {
                                    return scanContext.createTypeInformation(producedLogicalType);
                                }

                                @Override
                                public DataStructureConverter createDataStructureConverter(
                                        DataType producedDataType) {
                                    return scanContext.createDataStructureConverter(
                                            producedDataType);
                                }
                            },
                            projectFields);
        }

        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(tableIdentifier, table)
                        .withContinuousMode(streaming)
                        .withLogSourceProvider(logSourceProvider)
                        .withProjection(projectFields)
                        .withPredicate(predicate)
                        .withParallelism(
                                Configuration.fromMap(table.schema().options())
                                        .get(TableStoreFactoryOptions.SCAN_PARALLELISM));

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment env) {
                return sourceBuilder.withEnv(env).build();
            }

            @Override
            public boolean isBounded() {
                return !streaming;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        TableStoreSource copied =
                new TableStoreSource(
                        tableIdentifier, table, streaming, logStoreContext, logStoreTableFactory);
        copied.predicate = predicate;
        copied.projectFields = projectFields;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "TableStoreSource";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<Predicate> converted = new ArrayList<>();
        RowType rowType = table.schema().logicalRowType();
        for (ResolvedExpression filter : filters) {
            PredicateConverter.convert(rowType, filter).ifPresent(converted::add);
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        return Result.of(filters, filters);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectFields = projectedFields;
    }
}
