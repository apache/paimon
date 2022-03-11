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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.StoreTableContext;
import org.apache.flink.table.store.file.predicate.And;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.utils.TableStoreUtils.createLogStoreContext;
import static org.apache.flink.table.store.connector.utils.TableStoreUtils.createLogStoreTableFactory;

/**
 * Table source to create {@link FileStoreSource} under batch mode or change-tracking is disabled.
 * For streaming mode with change-tracking enabled and FULL scan mode, it will create a {@link
 * org.apache.flink.connector.base.source.hybrid.HybridSource} of {@link FileStoreSource} and kafka
 * log source created by {@link LogSourceProvider}.
 */
public class StoreTableSource implements ScanTableSource, SupportsFilterPushDown {

    private final StoreTableContext storeTableContext;

    private List<ResolvedExpression> partitionFilters = new ArrayList<>();
    private List<ResolvedExpression> fieldFilters = new ArrayList<>();

    public StoreTableSource(StoreTableContext storeTableContext) {
        this.storeTableContext = storeTableContext;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return storeTableContext.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (fileModeOnly()) {
            return SourceProvider.of(createFileSource());
        }
        if (logModeOnly()) {
            return SourceProvider.of(createLogSourceProvider().createSource(null));
        } else {
            HybridSource.HybridSourceBuilder<RowData, StaticFileStoreSplitEnumerator> builder =
                    new HybridSource.HybridSourceBuilder<>();
            builder.addSource(createFileSource());
            builder.addSource(
                    (HybridSource.SourceFactory<
                                    RowData, Source<RowData, ?, ?>, StaticFileStoreSplitEnumerator>)
                            sourceSwitchContext -> {
                                return createLogSourceProvider()
                                        .createSource(null); // TODO: get log offset
                            },
                    Boundedness.CONTINUOUS_UNBOUNDED);
            return SourceProvider.of(builder.build());
        }
    }

    @Override
    public DynamicTableSource copy() {
        StoreTableSource copied = new StoreTableSource(storeTableContext);
        copied.partitionFilters = new ArrayList<>(partitionFilters);
        copied.fieldFilters = new ArrayList<>(fieldFilters);
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "StoreTableSource";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        if (storeTableContext.getPartitionKeys().size() > 0) {
            classifyFilters(filters);
        } else {
            fieldFilters = filters;
        }
        return Result.of(
                Stream.concat(partitionFilters.stream(), fieldFilters.stream())
                        .collect(Collectors.toList()),
                Collections.emptyList());
    }

    // ~ Tools ------------------------------------------------------------------

    private boolean fileModeOnly() {
        return storeTableContext.batchMode() || !storeTableContext.enableChangeTracking();
    }

    private boolean logModeOnly() {
        return !storeTableContext.batchMode()
                && storeTableContext.enableChangeTracking()
                && startupMode() != LogOptions.LogStartupMode.FULL;
    }

    private LogOptions.LogStartupMode startupMode() {
        return LogOptions.LogStartupMode.fromValue(
                storeTableContext
                        .getContext()
                        .getCatalogTable()
                        .getOptions()
                        .getOrDefault(
                                LogOptions.SCAN.key(), LogOptions.SCAN.defaultValue().toString()));
    }

    private FileStoreSource createFileSource() {
        return new FileStoreSource(
                storeTableContext.fileStore(),
                storeTableContext.primaryKeyIndex().length == 0,
                null,
                partitionFilters.stream()
                        .map(filter -> filter.accept(PredicateConverter.CONVERTER))
                        .reduce(And::new)
                        .orElse(null));
    }

    private LogSourceProvider createLogSourceProvider() {
        return createLogStoreTableFactory()
                .createSourceProvider(
                        createLogStoreContext(storeTableContext.getContext()),
                        new LogStoreTableFactory.SourceContext() {
                            @Override
                            public <T> TypeInformation<T> createTypeInformation(
                                    DataType producedDataType) {
                                return createTypeInformation(
                                        TypeConversions.fromDataToLogicalType(producedDataType));
                            }

                            @Override
                            public <T> TypeInformation<T> createTypeInformation(
                                    LogicalType producedLogicalType) {
                                return InternalTypeInfo.of(producedLogicalType);
                            }

                            @Override
                            public DynamicTableSource.DataStructureConverter
                                    createDataStructureConverter(DataType producedDataType) {
                                return ScanRuntimeProviderContext.INSTANCE
                                        .createDataStructureConverter(producedDataType);
                            }
                        });
    }

    private void classifyFilters(List<ResolvedExpression> filters) {
        List<String> fieldNames = storeTableContext.getRowType().getFieldNames();
        List<String> partitionNames = storeTableContext.getPartitionType().getFieldNames();
        PartitionIndexVisitor visitor =
                new PartitionIndexVisitor(
                        fieldNames.stream().mapToInt(partitionNames::indexOf).toArray());
        filters.forEach(
                filter -> {
                    try {
                        partitionFilters.add(filter.accept(visitor));
                    } catch (FoundFieldReference e) {
                        fieldFilters.add(filter);
                    }
                });
    }

    private static class PartitionIndexVisitor implements ExpressionVisitor<ResolvedExpression> {

        private final int[] mapping;

        PartitionIndexVisitor(int[] mapping) {
            this.mapping = mapping;
        }

        @Override
        public ResolvedExpression visit(CallExpression call) {
            return CallExpression.anonymous(
                    call.getFunctionDefinition(),
                    call.getResolvedChildren().stream()
                            .map(e -> e.accept(this))
                            .collect(Collectors.toList()),
                    call.getOutputDataType());
        }

        @Override
        public ResolvedExpression visit(ValueLiteralExpression valueLiteral) {
            return valueLiteral;
        }

        @Override
        public ResolvedExpression visit(FieldReferenceExpression fieldReference) {
            int adjustIndex = mapping[fieldReference.getFieldIndex()];
            if (adjustIndex == -1) {
                // not a partition field
                throw new FoundFieldReference();
            }
            return new FieldReferenceExpression(
                    fieldReference.getName(),
                    fieldReference.getOutputDataType(),
                    fieldReference.getInputIndex(),
                    adjustIndex);
        }

        @Override
        public ResolvedExpression visit(TypeLiteralExpression typeLiteral) {
            return typeLiteral;
        }

        @Override
        public ResolvedExpression visit(Expression other) {
            return (ResolvedExpression) other;
        }
    }

    private static class FoundFieldReference extends RuntimeException {}
}
