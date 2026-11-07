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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * A runnable fake lake-stream store {@link ScanTableSource}. It emits the rows that were written to
 * {@link FakeStoreData} for its table key, standing in for the store's union read of un-tiered
 * data.
 */
public class FakeStoreSource implements ScanTableSource {

    private final String tableKey;
    private final RowType rowType;

    public FakeStoreSource(String tableKey, RowType rowType) {
        this.tableKey = tableKey;
        this.rowType = rowType;
    }

    /** The store table key this source reads, {@code database.table}. */
    public String tableKey() {
        return tableKey;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment executor) {
                return executor.fromData(
                        FakeStoreData.rows(tableKey), InternalTypeInfo.of(rowType));
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new FakeStoreSource(tableKey, rowType);
    }

    @Override
    public String asSummaryString() {
        return "FakeStoreSource";
    }
}
