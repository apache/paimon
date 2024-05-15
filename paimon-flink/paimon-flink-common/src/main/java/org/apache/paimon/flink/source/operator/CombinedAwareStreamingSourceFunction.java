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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.compact.MultiAwareBucketTableScan;
import org.apache.paimon.flink.compact.MultiTableScanBase;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.table.data.RowData;

import java.util.regex.Pattern;

import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.FINISHED;
import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.IS_EMPTY;

/** It is responsible for monitoring compactor source of multi bucket table in stream mode. */
public class CombinedAwareStreamingSourceFunction
        extends CombinedCompactorSourceFunction<Tuple2<Split, String>> {

    private final long monitorInterval;
    private transient MultiTableScanBase<Tuple2<Split, String>> tableScan;

    public CombinedAwareStreamingSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        super(catalogLoader, includingPattern, excludingPattern, databasePattern, true);
        this.monitorInterval = monitorInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        tableScan =
                new MultiAwareBucketTableScan(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        isStreaming,
                        isRunning);
    }

    @SuppressWarnings("BusyWait")
    @Override
    void scanTable() throws Exception {
        while (isRunning.get()) {
            MultiTableScanBase.ScanResult scanResult = tableScan.scanTable(ctx);
            if (scanResult == FINISHED) {
                return;
            }
            if (scanResult == IS_EMPTY) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    public static DataStream<RowData> buildSource(
            StreamExecutionEnvironment env,
            String name,
            TypeInformation<RowData> typeInfo,
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {

        CombinedAwareStreamingSourceFunction function =
                new CombinedAwareStreamingSourceFunction(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        StreamSource<Tuple2<Split, String>, ?> sourceOperator = new StreamSource<>(function);
        boolean isParallel = false;
        TupleTypeInfo<Tuple2<Split, String>> tupleTypeInfo =
                new TupleTypeInfo<>(
                        new JavaTypeInfo<>(Split.class), BasicTypeInfo.STRING_TYPE_INFO);
        return new DataStreamSource<>(
                        env,
                        tupleTypeInfo,
                        sourceOperator,
                        isParallel,
                        name,
                        Boundedness.CONTINUOUS_UNBOUNDED)
                .forceNonParallel()
                .partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        split -> ((DataSplit) split.f0).bucket())
                .transform(name, typeInfo, new MultiTablesReadOperator(catalogLoader, true));
    }
}
