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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PartitionExpressionVisitor;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/** A Flink {@link ScanTableSource} for paimon. */
public abstract class FlinkTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableSource.class);

    protected final Table table;

    @Nullable protected Predicate predicate;
    @Nullable protected int[][] projectFields;
    @Nullable protected Long limit;

    public FlinkTableSource(Table table) {
        this(table, null, null, null);
    }

    public FlinkTableSource(
            Table table,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit) {
        this.table = table;
        this.predicate = predicate;
        this.projectFields = projectFields;
        this.limit = limit;
    }

    /** @return The unconsumed filters. */
    public List<ResolvedExpression> pushFilters(List<ResolvedExpression> filters) {
        List<String> partitionKeys = table.partitionKeys();
        Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> partitionedFilters;

        if (partitionKeys.isEmpty() || isStreaming()) {
            partitionedFilters = Tuple2.of(new ArrayList<>(), filters);
        } else {
            partitionedFilters = partition(filters, table.partitionKeys());
        }
        List<Predicate> converted = new ArrayList<>();
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());
        // The source must ensure the consumed filters are fully evaluated, otherwise the result
        // of query will be wrong.
        List<ResolvedExpression> unConsumedFilters = new ArrayList<>();
        List<ResolvedExpression> consumedFilters = new ArrayList<>();

        if (partitionedFilters.f0.isEmpty()) {
            for (ResolvedExpression filter : filters) {
                PredicateConverter.convert(rowType, filter).ifPresent(converted::add);
            }
            predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
            return filters;
        } else {
            for (ResolvedExpression filter : partitionedFilters.f0) {
                Optional<Predicate> predicateOptional = PredicateConverter.convert(rowType, filter);
                if (predicateOptional.isPresent()) {
                    converted.add(predicateOptional.get());
                    consumedFilters.add(filter);
                } else {
                    unConsumedFilters.add(filter);
                }
            }

            for (ResolvedExpression filter : partitionedFilters.f1) {
                PredicateConverter.convert(rowType, filter).ifPresent(converted::add);
                unConsumedFilters.add(filter);
            }

            predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
            LOG.info("Consumed filters: {} of {}", consumedFilters, filters);

            return unConsumedFilters;
        }
    }

    /** split filters to partition and non-partition part. */
    private Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> partition(
            List<ResolvedExpression> filters, List<String> partitionKeys) {
        if (partitionKeys.isEmpty()) {
            return Tuple2.of(new ArrayList<>(), filters);
        } else {
            List<ResolvedExpression> partitionFiltersCanBeConsumed = new ArrayList<>();
            List<ResolvedExpression> remainingFilters = new ArrayList<>();
            for (ResolvedExpression filter : filters) {
                PartitionExpressionVisitor visitor =
                        new PartitionExpressionVisitor(new HashSet<>(partitionKeys));
                try {
                    filter.accept(visitor);
                    if (!visitor.getVisitedInputRef().isEmpty()) {
                        partitionFiltersCanBeConsumed.add(filter);
                    } else {
                        remainingFilters.add(filter);
                    }
                } catch (PredicateConverter.UnsupportedExpression e) {
                    remainingFilters.add(filter);
                }
            }
            return Tuple2.of(partitionFiltersCanBeConsumed, remainingFilters);
        }
    }

    public void pushProjection(int[][] projectedFields) {
        this.projectFields = projectedFields;
    }

    public void pushLimit(long limit) {
        this.limit = limit;
    }

    public abstract ChangelogMode getChangelogMode();

    public abstract ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext);

    public abstract void pushWatermark(WatermarkStrategy<RowData> watermarkStrategy);

    public abstract LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context);

    public abstract TableStats reportStatistics();

    public abstract FlinkTableSource copy();

    public abstract String asSummaryString();

    public abstract List<String> listAcceptedFilterFields();

    public abstract void applyDynamicFiltering(List<String> candidateFilterFields);

    public abstract boolean isStreaming();
}
