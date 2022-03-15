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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.store.connector.TableStore;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table source to create {@link FileStoreSource} under batch mode or change-tracking is disabled.
 * For streaming mode with change-tracking enabled and FULL scan mode, it will create a {@link
 * org.apache.flink.connector.base.source.hybrid.HybridSource} of {@link FileStoreSource} and kafka
 * log source created by {@link LogSourceProvider}.
 */
public class TableStoreSource
        implements ScanTableSource, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final TableStore tableStore;
    private final boolean streaming;
    @Nullable private final LogSourceProvider logSourceProvider;

    @Nullable private List<ResolvedExpression> partitionFilters;
    @Nullable private List<ResolvedExpression> fieldFilters;
    @Nullable private int[][] projectFields;

    public TableStoreSource(
            TableStore tableStore,
            boolean streaming,
            @Nullable LogSourceProvider logSourceProvider) {
        this.tableStore = tableStore;
        this.streaming = streaming;
        this.logSourceProvider = logSourceProvider;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return streaming
                ? tableStore.valueCountMode()
                        ? ChangelogMode.all()
                        // TODO: optimize upsert
                        : ChangelogMode.upsert()
                : ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        TableStore.SourceBuilder builder =
                tableStore
                        .sourceBuilder()
                        .withContinuousMode(streaming)
                        .withHybridMode(streaming && logSourceProvider != null)
                        .withLogSourceProvider(logSourceProvider)
                        .withProjection(projectFields)
                        .withPartitionPredicate(PredicateConverter.convert(partitionFilters))
                        .withFieldPredicate(PredicateConverter.convert(fieldFilters));
        return SourceProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        TableStoreSource copied = new TableStoreSource(tableStore, streaming, logSourceProvider);
        copied.partitionFilters = partitionFilters;
        copied.fieldFilters = fieldFilters;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "TableStoreSource";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        if (tableStore.partitioned()) {
            classifyFilters(filters);
        } else {
            fieldFilters = filters;
        }
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectFields = projectedFields;
    }

    private void classifyFilters(List<ResolvedExpression> filters) {
        List<String> fieldNames = tableStore.fieldNames();
        List<String> partitionKeys = tableStore.partitionKeys();
        PartitionIndexVisitor visitor =
                new PartitionIndexVisitor(
                        fieldNames.stream().mapToInt(partitionKeys::indexOf).toArray());
        filters.forEach(
                filter -> {
                    try {
                        if (partitionFilters == null) {
                            partitionFilters = new ArrayList<>();
                        }
                        partitionFilters.add(filter.accept(visitor));
                    } catch (FoundFieldReference e) {
                        if (fieldFilters == null) {
                            fieldFilters = new ArrayList<>();
                        }
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
