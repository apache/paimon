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

package org.apache.paimon.flink.btree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder.groupSplitsByRange;
import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder.splitByContiguousRowRange;

/** The {@link BTreeIndexTopoBuilder} for BTree index in Flink. */
public class BTreeIndexTopoBuilder {

    public static boolean buildIndex(
            StreamExecutionEnvironment env,
            Supplier<BTreeGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            List<String> indexColumns,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        List<DataStream<Committable>> allStreams = new ArrayList<>();
        for (String indexColumn : indexColumns) {
            BTreeGlobalIndexBuilder indexBuilder =
                    indexBuilderSupplier.get().withIndexField(indexColumn);
            if (partitionPredicate != null) {
                indexBuilder = indexBuilder.withPartitionPredicate(partitionPredicate);
            }

            Optional<Pair<RowRangeIndex, List<DataSplit>>> indexRangeAndSplits =
                    indexBuilder.scan();
            if (!indexRangeAndSplits.isPresent()) {
                return false;
            }

            Pair<RowRangeIndex, List<DataSplit>> scanResult = indexRangeAndSplits.get();
            List<DataSplit> splits = splitByContiguousRowRange(scanResult.getRight());
            if (splits.isEmpty()) {
                return false;
            }
            Map<BinaryRow, Map<Range, List<Split>>> partitionRangeSplits =
                    groupSplitsByRange(scanResult.getLeft(), splits);
            if (partitionRangeSplits.isEmpty()) {
                return false;
            }

            // 2. Select necessary columns (index field + ROW_ID)
            List<String> selectedColumns = new ArrayList<>();
            selectedColumns.add(indexColumn);

            RowType readType =
                    SpecialFields.rowTypeWithRowId(table.rowType().project(selectedColumns));
            int indexFieldPos = readType.getFieldIndex(indexColumn);
            int rowIdPos = readType.getFieldIndex(SpecialFields.ROW_ID.name());
            DataType indexFieldType = readType.getTypeAt(indexFieldPos);

            // 3. Calculate maximum parallelism bound
            long recordsPerRange = userOptions.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE);
            int maxParallelism =
                    userOptions.get(BTreeIndexOptions.BTREE_INDEX_BUILD_MAX_PARALLELISM);

            // 4. Build one topology per contiguous row range
            CoreOptions coreOptions = table.coreOptions();
            ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
            List<String> sortColumns = new ArrayList<>();
            sortColumns.add(indexColumn);
            int partitionFieldSize = table.partitionKeys().size();
            BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionFieldSize);
            for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                    partitionRangeSplits.entrySet()) {
                BinaryRow partition = partitionEntry.getKey();
                for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                    Range range = entry.getKey();
                    List<Split> rangeSplits = entry.getValue();
                    if (rangeSplits.isEmpty()) {
                        continue;
                    }

                    DataStream<Committable> commitMessages =
                            executeForPartitionRange(
                                    env,
                                    range,
                                    rangeSplits,
                                    readBuilder,
                                    indexBuilder,
                                    partitionFieldSize,
                                    binaryRowSerializer.serializeToBytes(partition),
                                    indexFieldPos,
                                    rowIdPos,
                                    indexFieldType,
                                    sortColumns,
                                    coreOptions,
                                    readType,
                                    recordsPerRange,
                                    maxParallelism);

                    allStreams.add(commitMessages);
                }
            }
        }
        if (!allStreams.isEmpty()) {
            @SuppressWarnings("unchecked")
            DataStream<Committable>[] rest =
                    allStreams.subList(1, allStreams.size()).toArray(new DataStream[0]);
            commit(table, allStreams.get(0).union(rest));
        }

        return true;
    }

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        if (buildIndex(
                env,
                () -> new BTreeGlobalIndexBuilder(table),
                table,
                Collections.singletonList(indexColumn),
                partitionPredicate,
                userOptions)) {
            env.execute("Create btree global index for table: " + table.name());
        }
    }

    protected static DataStream<Committable> executeForPartitionRange(
            StreamExecutionEnvironment env,
            Range range,
            List<Split> rangeSplits,
            ReadBuilder readBuilder,
            BTreeGlobalIndexBuilder indexBuilder,
            int partitionFieldSize,
            byte[] partition,
            int indexFieldPos,
            int rowIdPos,
            DataType indexFieldType,
            List<String> sortColumns,
            CoreOptions coreOptions,
            RowType readType,
            long recordsPerRange,
            int maxParallelism) {
        int parallelism = Math.max((int) (range.count() / recordsPerRange), 1);
        parallelism = Math.min(parallelism, maxParallelism);

        DataStream<Split> sourceStream =
                StreamExecutionEnvironmentUtils.fromData(
                                env,
                                new JavaTypeInfo<>(Split.class),
                                rangeSplits.toArray(new Split[0]))
                        .name("Global Index Source " + " range=" + range)
                        .setParallelism(1);

        DataStream<RowData> rowDataStream =
                sourceStream
                        .transform(
                                "Read Data " + range,
                                InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(readType)),
                                new ReadDataOperator(readBuilder))
                        .setParallelism(parallelism);

        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(sortColumns)
                        .setSortStrategy(CoreOptions.OrderType.ORDER)
                        .setSinkParallelism(parallelism)
                        .setLocalSampleSize(parallelism * coreOptions.getLocalSampleMagnification())
                        .setGlobalSampleSize(parallelism * 1000)
                        .setRangeNumber(parallelism * 10)
                        .build();

        TableSorter sorter =
                TableSorter.getSorter(env, rowDataStream, coreOptions, readType, sortInfo);
        DataStream<RowData> sortedStream = sorter.sort();

        return sortedStream
                .transform(
                        "write-btree-index " + range,
                        new CommittableTypeInfo(),
                        new WriteIndexOperator(
                                range,
                                partitionFieldSize,
                                partition,
                                indexBuilder,
                                indexFieldPos,
                                rowIdPos,
                                indexFieldType))
                .setParallelism(parallelism);
    }

    private static void commit(FileStoreTable table, DataStream<Committable> written) {
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        "BTreeIndexCommitter-" + UUID.randomUUID(),
                        context ->
                                new StoreCommitter(
                                        table, table.newCommit(context.commitUser()), context),
                        new NoopCommittableStateManager());

        written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    /** Operator to read data from splits. */
    private static class ReadDataOperator
            extends org.apache.flink.table.runtime.operators.TableStreamOperator<RowData>
            implements org.apache.flink.streaming.api.operators.OneInputStreamOperator<
                    Split, RowData> {

        private static final long serialVersionUID = 1L;

        private final ReadBuilder readBuilder;

        private transient TableRead tableRead;

        public ReadDataOperator(ReadBuilder readBuilder) {
            this.readBuilder = readBuilder;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.tableRead = readBuilder.newRead();
        }

        @Override
        public void processElement(StreamRecord<Split> element) throws Exception {
            Split split = element.getValue();
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                reader.forEachRemaining(
                        row -> output.collect(new StreamRecord<>(new FlinkRowData(row))));
            }
        }
    }

    private static class WriteIndexOperator extends BoundedOneInputOperator<RowData, Committable> {

        private final Range rowRange;
        private final byte[] partition;
        private final int partitionFieldSize;
        private final BTreeGlobalIndexBuilder builder;
        private final int indexFieldPos;
        private final int rowIdPos;
        private final DataType indexFieldType;

        private transient long counter;
        private transient GlobalIndexParallelWriter currentWriter;
        private transient List<CommitMessage> commitMessages;
        private transient InternalRow.FieldGetter indexFieldGetter;
        private transient BinaryRowSerializer binaryRowSerializer;

        public WriteIndexOperator(
                Range rowRange,
                int partitionFieldSize,
                byte[] partition,
                BTreeGlobalIndexBuilder builder,
                int indexFieldPos,
                int rowIdPos,
                DataType indexFieldType) {
            this.rowRange = rowRange;
            this.partitionFieldSize = partitionFieldSize;
            this.partition = partition;
            this.builder = builder;
            this.indexFieldPos = indexFieldPos;
            this.rowIdPos = rowIdPos;
            this.indexFieldType = indexFieldType;
        }

        @Override
        public void open() throws Exception {
            super.open();
            commitMessages = new ArrayList<>();
            indexFieldGetter = InternalRow.createFieldGetter(indexFieldType, indexFieldPos);
            this.binaryRowSerializer = new BinaryRowSerializer(partitionFieldSize);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws IOException {
            InternalRow row = new FlinkRowWrapper(element.getValue());
            if (currentWriter != null && counter >= builder.recordsPerRange()) {
                commitMessages.add(
                        builder.flushIndex(
                                rowRange,
                                currentWriter.finish(),
                                binaryRowSerializer.deserializeFromBytes(partition)));
                currentWriter = null;
                counter = 0;
            }

            counter++;

            if (currentWriter == null) {
                currentWriter = builder.createWriter();
            }

            long localRowId = row.getLong(rowIdPos) - rowRange.from;
            currentWriter.write(indexFieldGetter.getFieldOrNull(row), localRowId);
        }

        @Override
        public void endInput() throws IOException {
            if (counter > 0) {
                commitMessages.add(
                        builder.flushIndex(
                                rowRange,
                                currentWriter.finish(),
                                binaryRowSerializer.deserializeFromBytes(partition)));
            }
            for (CommitMessage message : commitMessages) {
                output.collect(
                        new StreamRecord<>(
                                new Committable(BatchWriteBuilder.COMMIT_IDENTIFIER, message)));
            }
            commitMessages.clear();
        }
    }
}
