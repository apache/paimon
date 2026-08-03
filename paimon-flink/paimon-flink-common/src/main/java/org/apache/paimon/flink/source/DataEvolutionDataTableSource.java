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

import org.apache.paimon.flink.source.aggregate.PushedAggregateResult;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link DataTableSource} exposing row-level modification metadata for Data Evolution tables. */
public class DataEvolutionDataTableSource extends DataTableSource
        implements SupportsReadingMetadata, SupportsRowLevelModificationScan {

    public DataEvolutionDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context) {
        super(tableIdentifier, table, unbounded, context);
    }

    private DataEvolutionDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields,
            @Nullable PushedAggregateResult pushedAggregateResult) {
        super(
                tableIdentifier,
                table,
                unbounded,
                context,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                pushedAggregateResult);
    }

    @Override
    protected DataTableSource newSource() {
        return new DataEvolutionDataTableSource(
                tableIdentifier,
                table,
                unbounded,
                context,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                pushedAggregateResult);
    }
}
