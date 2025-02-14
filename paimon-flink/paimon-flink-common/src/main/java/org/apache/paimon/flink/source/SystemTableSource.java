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
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/** A {@link FlinkTableSource} for system table. */
public class SystemTableSource extends FlinkTableSource {

    private final boolean unbounded;
    private final int splitBatchSize;
    private final FlinkConnectorOptions.SplitAssignMode splitAssignMode;
    private final ObjectIdentifier tableIdentifier;

    public SystemTableSource(Table table, boolean unbounded, ObjectIdentifier tableIdentifier) {
        super(table);
        this.unbounded = unbounded;
        Options options = Options.fromMap(table.options());
        this.splitBatchSize = options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE);
        this.splitAssignMode = options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE);
        this.tableIdentifier = tableIdentifier;
    }

    public SystemTableSource(
            Table table,
            boolean unbounded,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            int splitBatchSize,
            FlinkConnectorOptions.SplitAssignMode splitAssignMode,
            ObjectIdentifier tableIdentifier) {
        super(table, predicate, projectFields, limit);
        this.unbounded = unbounded;
        this.splitBatchSize = splitBatchSize;
        this.splitAssignMode = splitAssignMode;
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        Source<RowData, ?, ?> source;

        NestedProjectedRowData rowData = null;
        org.apache.paimon.types.RowType readType = null;
        if (projectFields != null) {
            Projection projection = Projection.of(projectFields);
            rowData = projection.getOuterProjectRow(table.rowType());
            readType = projection.project(table.rowType());
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        if (readType != null) {
            readBuilder.withReadType(readType);
        }
        readBuilder.withFilter(predicate);

        if (unbounded && table instanceof DataTable) {
            source =
                    new ContinuousFileStoreSource(
                            readBuilder, table.options(), limit, BucketMode.HASH_FIXED, rowData);
        } else {
            source =
                    new StaticFileStoreSource(
                            readBuilder, limit, splitBatchSize, splitAssignMode, null, rowData);
        }
        return new PaimonDataStreamScanProvider(
                source.getBoundedness() == Boundedness.BOUNDED,
                env -> {
                    Integer parallelism = inferSourceParallelism(env);
                    DataStreamSource<RowData> dataStreamSource =
                            env.fromSource(
                                    source,
                                    WatermarkStrategy.noWatermarks(),
                                    tableIdentifier.asSummaryString());
                    if (parallelism != null) {
                        dataStreamSource.setParallelism(parallelism);
                    }
                    return dataStreamSource;
                });
    }

    @Override
    public SystemTableSource copy() {
        return new SystemTableSource(
                table,
                unbounded,
                predicate,
                projectFields,
                limit,
                splitBatchSize,
                splitAssignMode,
                tableIdentifier);
    }

    @Override
    public String asSummaryString() {
        return "Paimon-SystemTable-Source";
    }

    @Override
    public boolean isUnbounded() {
        return unbounded;
    }
}
