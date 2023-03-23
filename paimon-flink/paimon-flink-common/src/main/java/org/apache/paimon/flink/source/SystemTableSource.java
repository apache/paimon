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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/** A {@link FlinkTableSource} for system table. */
public class SystemTableSource extends FlinkTableSource {

    private final Table table;
    private final boolean isStreamingMode;
    private final int splitBatchSize;

    public SystemTableSource(Table table, boolean isStreamingMode, int splitBatchSize) {
        super(table);
        this.table = table;
        this.isStreamingMode = isStreamingMode;
        this.splitBatchSize = splitBatchSize;
    }

    public SystemTableSource(
            Table table,
            boolean isStreamingMode,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            int splitBatchSize) {
        super(table, predicate, projectFields, limit);
        this.table = table;
        this.isStreamingMode = isStreamingMode;
        this.splitBatchSize = splitBatchSize;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        Source<RowData, ?, ?> source;
        if (table instanceof DataTable) {
            DataTable dataTable = (DataTable) table;
            source =
                    isStreamingMode
                            ? new ContinuousFileStoreSource(
                                    dataTable, projectFields, predicate, limit)
                            : new StaticFileStoreSource(dataTable, projectFields, predicate, limit);
        } else {
            source =
                    new SimpleSystemSource(
                            table.newReadBuilder()
                                    .withFilter(predicate)
                                    .withProjection(projectFields),
                            limit,
                            splitBatchSize);
        }
        return SourceProvider.of(source);
    }

    @Override
    public DynamicTableSource copy() {
        return new SystemTableSource(
                table, isStreamingMode, predicate, projectFields, limit, splitBatchSize);
    }

    @Override
    public String asSummaryString() {
        return "Paimon-SystemTable-Source";
    }
}
