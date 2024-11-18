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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;

/** A Flink {@link ScanTableSource} for paimon. */
public abstract class FlinkTableSource
        implements ScanTableSource,
                SupportsFilterPushDown,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

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

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<String> partitionKeys = table.partitionKeys();
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());

        // The source must ensure the consumed filters are fully evaluated, otherwise the result
        // of query will be wrong.
        List<ResolvedExpression> unConsumedFilters = new ArrayList<>();
        List<ResolvedExpression> consumedFilters = new ArrayList<>();
        List<Predicate> converted = new ArrayList<>();
        PredicateVisitor<Boolean> onlyPartFieldsVisitor =
                new PartitionPredicateVisitor(partitionKeys);

        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOptional = PredicateConverter.convert(rowType, filter);

            if (!predicateOptional.isPresent()) {
                unConsumedFilters.add(filter);
            } else {
                Predicate p = predicateOptional.get();
                if (isStreaming() || !p.visit(onlyPartFieldsVisitor)) {
                    unConsumedFilters.add(filter);
                } else {
                    consumedFilters.add(filter);
                }
                converted.add(p);
            }
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        LOG.info("Consumed filters: {} of {}", consumedFilters, filters);

        return Result.of(filters, unConsumedFilters);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectFields = projectedFields;
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    public abstract boolean isStreaming();

    @Nullable
    protected Integer inferSourceParallelism(StreamExecutionEnvironment env) {
        Options options = Options.fromMap(this.table.options());
        Configuration envConfig = (Configuration) env.getConfiguration();
        if (envConfig.containsKey(FLINK_INFER_SCAN_PARALLELISM)) {
            options.set(
                    FlinkConnectorOptions.INFER_SCAN_PARALLELISM,
                    Boolean.parseBoolean(envConfig.toMap().get(FLINK_INFER_SCAN_PARALLELISM)));
        }
        Integer parallelism = options.get(FlinkConnectorOptions.SCAN_PARALLELISM);
        if (parallelism == null && options.get(FlinkConnectorOptions.INFER_SCAN_PARALLELISM)) {
            if (isStreaming()) {
                parallelism = Math.max(1, options.get(CoreOptions.BUCKET));
            } else {
                scanSplitsForInference();
                parallelism = splitStatistics.splitNumber();
                if (null != limit && limit > 0) {
                    int limitCount =
                            limit >= Integer.MAX_VALUE ? Integer.MAX_VALUE : limit.intValue();
                    parallelism = Math.min(parallelism, limitCount);
                }

                parallelism = Math.max(1, parallelism);
                parallelism =
                        Math.min(
                                parallelism,
                                options.get(FlinkConnectorOptions.INFER_SCAN_MAX_PARALLELISM));
            }
        }
        return parallelism;
    }

    protected void scanSplitsForInference() {
        if (splitStatistics == null) {
            if (table instanceof DataTable) {
                List<PartitionEntry> partitionEntries =
                        table.newReadBuilder()
                                .withFilter(predicate)
                                .dropStats()
                                .newScan()
                                .listPartitionEntries();
                long totalSize = 0;
                long rowCount = 0;
                for (PartitionEntry entry : partitionEntries) {
                    totalSize += entry.fileSizeInBytes();
                    rowCount += entry.recordCount();
                }
                long splitTargetSize = ((DataTable) table).coreOptions().splitTargetSize();
                splitStatistics =
                        new SplitStatistics((int) (totalSize / splitTargetSize + 1), rowCount);
            } else {
                List<Split> splits =
                        table.newReadBuilder()
                                .withFilter(predicate)
                                .dropStats()
                                .newScan()
                                .plan()
                                .splits();
                splitStatistics =
                        new SplitStatistics(
                                splits.size(), splits.stream().mapToLong(Split::rowCount).sum());
            }
        }
    }

    /** Split statistics for inferring row count and parallelism size. */
    protected static class SplitStatistics {

        private final int splitNumber;
        private final long totalRowCount;

        protected SplitStatistics(int splitNumber, long totalRowCount) {
            this.splitNumber = splitNumber;
            this.totalRowCount = totalRowCount;
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
