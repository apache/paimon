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

package org.apache.paimon.flink.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeShuffle;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortPlan;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.DataEvolutionRowIdRewriteSink;
import org.apache.paimon.flink.sorter.OrderSorter;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Bounded range exchange and sorted shard writers for a Data Evolution rewrite. */
public class DataEvolutionRangeSort {

    public static void build(
            FileStoreTable table,
            DataStream<DataEvolutionRangeSortPlan> plans,
            List<String> fields,
            Snapshot snapshot) {
        StreamExecutionEnvironment env = plans.getExecutionEnvironment();
        CoreOptions options = table.coreOptions();
        int parallelism =
                options.toConfiguration()
                        .getOptional(FlinkConnectorOptions.UNAWARE_BUCKET_COMPACTION_PARALLELISM)
                        .orElse(Math.max(1, env.getParallelism()));
        String commitUser = CoreOptions.createCommitUser(options.toConfiguration());
        DataEvolutionRangeShuffle exchange = new DataEvolutionRangeShuffle(table, fields);
        DataStream<IndexedSplit> splits =
                plans.flatMap(
                                (plan, out) -> plan.readSplits().forEach(out::collect),
                                new JavaTypeInfo<>(IndexedSplit.class))
                        .setParallelism(1);
        DataStream<RowData> rows =
                splits.rebalance()
                        .flatMap(
                                (split, out) -> {
                                    try (CloseableIterator<InternalRow> input =
                                            exchange.read(split).toCloseableIterator()) {
                                        while (input.hasNext()) {
                                            out.collect(new FlinkRowData(input.next()));
                                        }
                                    }
                                },
                                InternalTypeInfo.of(
                                        LogicalTypeConversion.toLogicalType(
                                                exchange.exchangeType())))
                        .setParallelism(parallelism);

        TableSortInfo info =
                new TableSortInfo.Builder()
                        .setSortColumns(exchange.sortColumns())
                        .setSinkParallelism(parallelism)
                        .setRangeNumber(parallelism)
                        .setLocalSampleSize(parallelism * options.getLocalSampleMagnification())
                        .setGlobalSampleSize(parallelism * 1000)
                        .build();
        DataStream<RowData> sorted =
                new OrderSorter(env, rows, options, exchange.exchangeType(), info).sort();
        TupleTypeInfo<Tuple2<Integer, CommitMessage>> metadataType =
                new TupleTypeInfo<>(Types.INT, new JavaTypeInfo<>(CommitMessage.class));
        DataStream<Tuple2<Integer, CommitMessage>> outputs =
                sorted.forward()
                        .transform(
                                "Write Data Evolution sort ranges",
                                metadataType,
                                new WriteOperator(exchange, commitUser))
                        .setParallelism(parallelism);
        DataStream<Tuple2<Integer, CommitMessage>> inputs =
                plans.map(plan -> Tuple2.of(-1, plan.inputFiles()), metadataType).setParallelism(1);
        DataStream<Committable> replacements =
                inputs.union(outputs)
                        .transform(
                                "Assemble Data Evolution sort ranges",
                                new CommittableTypeInfo(),
                                new AssembleOperator())
                        .forceNonParallel();
        DataEvolutionRowIdRewriteSink.commitSorted(table, snapshot, replacements, commitUser);
    }

    private static class WriteOperator extends TableStreamOperator<Tuple2<Integer, CommitMessage>>
            implements OneInputStreamOperator<RowData, Tuple2<Integer, CommitMessage>>,
                    BoundedOneInput {
        private final DataEvolutionRangeShuffle exchange;
        private final String commitUser;
        private transient DataEvolutionRangeShuffle.ShardWriter writer;

        WriteOperator(DataEvolutionRangeShuffle exchange, String commitUser) {
            this.exchange = exchange;
            this.commitUser = commitUser;
        }

        @Override
        public void open() throws Exception {
            super.open();
            writer = exchange.createWriter(commitUser);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws Exception {
            RowData row = element.getValue();
            writer.write(row.getBinary(0), row.getBinary(exchange.payloadIndex()));
        }

        @Override
        public void endInput() throws Exception {
            int range = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
            for (CommitMessage message : writer.finish()) {
                output.collect(new StreamRecord<>(Tuple2.of(range, message)));
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (writer != null) {
                    writer.close();
                }
            } finally {
                super.close();
            }
        }
    }

    private static class AssembleOperator extends TableStreamOperator<Committable>
            implements OneInputStreamOperator<Tuple2<Integer, CommitMessage>, Committable>,
                    BoundedOneInput {
        private final List<CommitMessage> inputs = new ArrayList<>();
        private final List<Tuple2<Integer, CommitMessage>> runs = new ArrayList<>();

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, CommitMessage>> element) {
            Tuple2<Integer, CommitMessage> entry = element.getValue();
            if (entry.f0 == -1) {
                inputs.add(entry.f1);
            } else {
                runs.add(Tuple2.of(entry.f0, entry.f1));
            }
        }

        @Override
        public void endInput() {
            runs.sort(Comparator.comparingInt(entry -> entry.f0));
            List<CommitMessage> outputs = new ArrayList<>();
            runs.forEach(run -> outputs.add(run.f1));
            for (CommitMessage replacement :
                    DataEvolutionRangeShuffle.replacements(inputs, outputs)) {
                output.collect(new StreamRecord<>(new Committable(Long.MAX_VALUE, replacement)));
            }
        }
    }
}
