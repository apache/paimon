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
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;

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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** A Flink {@link ScanTableSource} for paimon. */
public abstract class FlinkTableSource {

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

    public void pushFilters(List<ResolvedExpression> filters) {
        List<Predicate> converted = new ArrayList<>();
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());
        for (ResolvedExpression filter : filters) {
            PredicateConverter.convert(rowType, filter).ifPresent(converted::add);
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
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
}
