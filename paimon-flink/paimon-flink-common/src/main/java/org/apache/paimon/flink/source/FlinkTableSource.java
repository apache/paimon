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

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;

/** A Flink {@link ScanTableSource} for paimon. */
public abstract class FlinkTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableSource.class);

    protected static final String FLINK_INFER_SCAN_PARALLELISM =
            String.format(
                    "%s%s", PAIMON_PREFIX, FlinkConnectorOptions.INFER_SCAN_PARALLELISM.key());

    protected final Table table;

    @Nullable protected Predicate predicate;
    @Nullable protected int[][] projectFields;
    @Nullable protected Long limit;
    protected SplitStatistics splitStatistics;

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
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());

        // The source must ensure the consumed filters are fully evaluated, otherwise the result
        // of query will be wrong.
        List<ResolvedExpression> unConsumedFilters = new ArrayList<>();
        List<ResolvedExpression> consumedFilters = new ArrayList<>();
        List<Predicate> converted = new ArrayList<>();
        PredicateVisitor<Boolean> visitor = new PartitionPredicateVisitor(partitionKeys);

        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOptional = PredicateConverter.convert(rowType, filter);

            if (!predicateOptional.isPresent()) {
                unConsumedFilters.add(filter);
            } else {
                Predicate p = predicateOptional.get();
                if (isStreaming() || !p.visit(visitor)) {
                    unConsumedFilters.add(filter);
                } else {
                    consumedFilters.add(filter);
                }
                converted.add(p);
            }
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        LOG.info("Consumed filters: {} of {}", consumedFilters, filters);

        return unConsumedFilters;
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

    protected void scanSplitsForInference() {
        if (splitStatistics == null) {
            List<Split> splits =
                    table.newReadBuilder().withFilter(predicate).newScan().plan().splits();
            splitStatistics = new SplitStatistics(splits);
        }
    }

    /** Split statistics for inferring row count and parallelism size. */
    protected static class SplitStatistics {

        private final int splitNumber;
        private final long totalRowCount;

        protected SplitStatistics(List<Split> splits) {
            this.splitNumber = splits.size();
            this.totalRowCount = splits.stream().mapToLong(Split::rowCount).sum();
        }

        public int splitNumber() {
            return splitNumber;
        }

        public long totalRowCount() {
            return totalRowCount;
        }
    }

    public Table getTable() {
        return table;
    }
}
