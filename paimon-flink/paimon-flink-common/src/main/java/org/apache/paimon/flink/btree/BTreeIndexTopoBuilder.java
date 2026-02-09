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
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.RowIdIndexFieldsExtractor;
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
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder.calcRowRange;

/** The {@link BTreeIndexTopoBuilder} for BTree index in Flink. */
public class BTreeIndexTopoBuilder {

    public static void buildIndex(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        // 1. Create BTree index builder and scan splits
        BTreeGlobalIndexBuilder indexBuilder =
                new BTreeGlobalIndexBuilder(table)
                        .withIndexType("btree")
                        .withIndexField(indexColumn);
        if (partitionPredicate != null) {
            indexBuilder = indexBuilder.withPartitionPredicate(partitionPredicate);
        }

        List<DataSplit> splits = indexBuilder.scan();
        Range range = calcRowRange(splits);
        if (splits.isEmpty() || range == null) {
            return;
        }

        // 2. Select necessary columns (partition keys + index field + ROW_ID)
        List<String> selectedColumns = new ArrayList<>(table.partitionKeys());
        selectedColumns.add(indexColumn);

        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType().project(selectedColumns));

        // 3. Calculate parallelism and sort
        long recordsPerRange = userOptions.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE);
        int parallelism = Math.max((int) (range.count() / recordsPerRange), 1);
        int maxParallelism = userOptions.get(BTreeIndexOptions.BTREE_INDEX_BUILD_MAX_PARALLELISM);
        parallelism = Math.min(parallelism, maxParallelism);

        // 4. Create source from splits and select columns
        DataStream<DataSplit> sourceStream =
                env.fromData(new JavaTypeInfo<>(DataSplit.class), splits.toArray(new DataSplit[0]))
                        .name("Global Index Source")
                        .setParallelism(1);

        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        DataStream<RowData> rowDataStream =
                sourceStream
                        .transform(
                                "Read Data",
                                InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(readType)),
                                new ReadDataOperator(readBuilder))
                        .setParallelism(parallelism);

        // 5. Sort data using TableSorter style
        // Configure sort info similar to SortCompactAction
        CoreOptions coreOptions = table.coreOptions();
        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(selectedColumns)
                        .setSortStrategy(CoreOptions.OrderType.ORDER)
                        .setSinkParallelism(parallelism)
                        .setLocalSampleSize(parallelism * coreOptions.getLocalSampleMagnification())
                        .setGlobalSampleSize(parallelism * 1000)
                        .setRangeNumber(parallelism * 10)
                        .build();

        // Use TableSorter for sorting
        TableSorter sorter =
                TableSorter.getSorter(env, rowDataStream, coreOptions, readType, sortInfo);
        DataStream<RowData> sortedStream = sorter.sort();

        // 6. Build index for each partition
        DataStream<Committable> commitMessages =
                sortedStream
                        .transform(
                                "write-btree-index",
                                new CommittableTypeInfo(),
                                new WriteIndexOperator(range, indexBuilder))
                        .setParallelism(parallelism);

        // 7. Commit all commit messages
        commit(table, commitMessages);

        env.execute("Create btree global index for table: " + table.name());
    }

    private static void commit(FileStoreTable table, DataStream<Committable> written) {
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        "BTreeIndexCommitter",
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
                    DataSplit, RowData> {

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
        public void processElement(StreamRecord<DataSplit> element) throws Exception {
            DataSplit split = element.getValue();
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                reader.forEachRemaining(
                        row -> output.collect(new StreamRecord<>(new FlinkRowData(row))));
            }
        }
    }

    private static class WriteIndexOperator extends BoundedOneInputOperator<RowData, Committable> {

        private final Range rowRange;
        private final BTreeGlobalIndexBuilder builder;

        private transient long counter;
        private transient BinaryRow currentPart;
        private transient GlobalIndexParallelWriter currentWriter;
        private transient List<CommitMessage> commitMessages;

        public WriteIndexOperator(Range rowRange, BTreeGlobalIndexBuilder builder) {
            this.rowRange = rowRange;
            this.builder = builder;
        }

        @Override
        public void open() throws Exception {
            super.open();
            commitMessages = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws IOException {
            InternalRow row = new FlinkRowWrapper(element.getValue());

            RowIdIndexFieldsExtractor extractor = builder.extractor();
            BinaryRow partRow = extractor.extractPartition(row);

            // the input is sorted by <partition, indexedField>
            if (currentWriter != null) {
                if (!Objects.equals(partRow, currentPart) || counter >= builder.recordsPerRange()) {
                    commitMessages.add(
                            builder.flushIndex(rowRange, currentWriter.finish(), currentPart));
                    currentWriter = null;
                    counter = 0;
                }
            }

            // write <value, rowId> pair to index file
            currentPart = partRow;
            counter++;

            if (currentWriter == null) {
                currentWriter = builder.createWriter();
            }

            // convert the original rowId to local rowId
            long localRowId = extractor.extractRowId(row) - rowRange.from;
            currentWriter.write(extractor.extractIndexField(row), localRowId);
        }

        @Override
        public void endInput() throws IOException {
            if (counter > 0) {
                commitMessages.add(
                        builder.flushIndex(rowRange, currentWriter.finish(), currentPart));
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
