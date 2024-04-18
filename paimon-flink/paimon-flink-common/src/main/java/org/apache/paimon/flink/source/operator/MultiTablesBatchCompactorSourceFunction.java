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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.InnerStreamTableScanImpl;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** It is responsible for monitoring compactor source in batch mode. */
public class MultiTablesBatchCompactorSourceFunction extends MultiTablesCompactorSourceFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTablesBatchCompactorSourceFunction.class);

    public MultiTablesBatchCompactorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        super(
                catalogLoader,
                includingPattern,
                excludingPattern,
                databasePattern,
                false,
                monitorInterval);
    }

    @Override
    public void run(SourceContext<Tuple2<Split, String>> ctx) throws Exception {
        this.ctx = ctx;
        if (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                try {
                    // batch mode do not need check for new tables
                    List<Tuple2<Split, String>> splits = new ArrayList<>();
                    for (Map.Entry<Identifier, StreamTableScan> entry : scansMap.entrySet()) {
                        Identifier identifier = entry.getKey();
                        StreamTableScan scan = entry.getValue();
                        int maxLevel = ((InnerStreamTableScanImpl) scan).options().numLevels() - 1;
                        splits.addAll(
                                scan.plan().splits().stream()
                                        .filter(
                                                split -> {
                                                    DataSplit dataSplit = (DataSplit) split;
                                                    if (dataSplit.dataFiles().isEmpty()) {
                                                        return false;
                                                    }
                                                    return dataSplit.dataFiles().stream()
                                                            .map(DataFileMeta::level)
                                                            .anyMatch(level -> level != maxLevel);
                                                })
                                        .map(split -> new Tuple2<>(split, identifier.getFullName()))
                                        .collect(Collectors.toList()));
                    }

                    isEmpty = splits.isEmpty();
                    splits.forEach(ctx::collect);

                } catch (EndOfScanException esf) {
                    LOG.info("Catching EndOfStreamException, the stream is finished.");
                    return;
                }
            }
            if (isEmpty) {
                throw new Exception(
                        "No splits were collected. Please ensure there are tables detected after pattern matching");
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
        MultiTablesBatchCompactorSourceFunction function =
                new MultiTablesBatchCompactorSourceFunction(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        StreamSource<Tuple2<Split, String>, ?> sourceOperator = new StreamSource<>(function);
        TupleTypeInfo<Tuple2<Split, String>> tupleTypeInfo =
                new TupleTypeInfo<>(
                        new JavaTypeInfo<>(Split.class), BasicTypeInfo.STRING_TYPE_INFO);
        return new DataStreamSource<>(
                        env, tupleTypeInfo, sourceOperator, false, name, Boundedness.BOUNDED)
                .forceNonParallel()
                .partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        split -> ((DataSplit) split.f0).bucket())
                .transform(name, typeInfo, new MultiTablesReadOperator(catalogLoader, false));
    }
}
