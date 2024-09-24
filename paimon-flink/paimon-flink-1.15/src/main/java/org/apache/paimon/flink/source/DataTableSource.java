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

import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** A {@link BaseDataTableSource} for Flink 1.15. */
public class DataTableSource extends BaseDataTableSource {

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                null,
                null,
                null,
                null,
                false);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            boolean isBatchCountStar) {
        super(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                isBatchCountStar);
    }

    @Override
    public DataTableSource copy() {
        return new DataTableSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                isBatchCountStar);
    }

    @Override
    protected List<String> dynamicPartitionFilteringFields() {
        return Collections.emptyList();
    }
}
