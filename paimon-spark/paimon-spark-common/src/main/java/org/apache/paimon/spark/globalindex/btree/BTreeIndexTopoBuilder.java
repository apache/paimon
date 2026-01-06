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

package org.apache.paimon.spark.globalindex.btree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.SparkRow;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderFactoryUtils;
import org.apache.paimon.spark.globalindex.GlobalIndexTopoBuilder;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** The {@link GlobalIndexTopoBuilder} for BTree index. */
public class BTreeIndexTopoBuilder implements GlobalIndexTopoBuilder {

    @Override
    public List<CommitMessage> buildIndex(
            SparkSession spark,
            DataSourceV2Relation relation,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options)
            throws IOException {

        // 1. read the whole dataset of target partitions
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }

        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();
        Range fullRange = calcRowRange(dataSplits);
        if (dataSplits.isEmpty() || fullRange == null) {
            return Collections.emptyList();
        }

        // we need to read all partition columns for shuffle
        List<String> selectedColumns = new ArrayList<>();
        selectedColumns.addAll(table.partitionKeys());
        selectedColumns.addAll(readType.getFieldNames());

        Dataset<Row> source =
                PaimonUtils.createDataset(
                        spark,
                        ScanPlanHelper$.MODULE$.createNewScanPlan(
                                dataSplits.toArray(new DataSplit[0]), relation));

        Dataset<Row> selected =
                source.select(selectedColumns.stream().map(functions::col).toArray(Column[]::new));

        // 2. shuffle and sort by partitions and index keys
        Column[] sortFields =
                selectedColumns.stream()
                        .filter(name -> !SpecialFields.ROW_ID.name().equals(name))
                        .map(functions::col)
                        .toArray(Column[]::new);

        long recordsPerRange = options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE);
        // this should be superfast since append only table can utilize count-start pushdown well.
        long rowCount = source.count();
        int partitionNum = Math.max((int) (rowCount / recordsPerRange), 1);
        int maxParallelism = options.get(BTreeIndexOptions.BTREE_INDEX_BUILD_MAX_PARALLELISM);
        partitionNum = Math.min(partitionNum, maxParallelism);

        // For efficiency, we do not repartition within each paimon partition. Instead, we directly
        // divide ranges by <partitions, index field>, and each subtask is expected to process
        // records from multiple partitions. The drawback is that if a Paimon partition spans
        // multiple Spark partitions, the first and last output files may contain relatively few
        // records.
        Dataset<Row> partitioned =
                selected.repartitionByRange(partitionNum, sortFields)
                        .sortWithinPartitions(sortFields);

        // 3. write index for each partition & range
        final GlobalIndexBuilderContext context =
                new GlobalIndexBuilderContext(
                        table, null, readType, indexField, indexType, 0, options, fullRange);
        final RowType rowType =
                SpecialFields.rowTypeWithRowId(table.rowType()).project(selectedColumns);
        JavaRDD<byte[]> written =
                partitioned
                        .javaRDD()
                        .map(row -> (InternalRow) (new SparkRow(rowType, row)))
                        .mapPartitions(
                                (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                        iter -> {
                                            CommitMessageSerializer commitMessageSerializer =
                                                    new CommitMessageSerializer();

                                            GlobalIndexBuilder globalIndexBuilder =
                                                    GlobalIndexBuilderFactoryUtils
                                                            .createIndexBuilder(context);

                                            List<CommitMessage> commitMessages =
                                                    globalIndexBuilder.build(
                                                            CloseableIterator.adapterForIterator(
                                                                    iter));
                                            List<byte[]> messageBytes = new ArrayList<>();

                                            for (CommitMessage commitMessage : commitMessages) {
                                                messageBytes.add(
                                                        commitMessageSerializer.serialize(
                                                                commitMessage));
                                            }

                                            return messageBytes.iterator();
                                        });

        // 4. collect all commit messages and return
        List<byte[]> commitBytes = written.collect();
        List<CommitMessage> result = new ArrayList<>();
        CommitMessageSerializer commitMessageSerializer = new CommitMessageSerializer();
        for (byte[] commitByte : commitBytes) {
            result.add(
                    commitMessageSerializer.deserialize(
                            commitMessageSerializer.getVersion(), commitByte));
        }

        return result;
    }

    private Range calcRowRange(List<DataSplit> dataSplits) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (DataSplit dataSplit : dataSplits) {
            for (DataFileMeta file : dataSplit.dataFiles()) {
                if (file.firstRowId() != null) {
                    start = Math.min(start, file.firstRowId());
                    end = Math.max(end, file.firstRowId() + file.rowCount());
                }
            }
        }
        return start == Long.MAX_VALUE ? null : new Range(start, end);
    }
}
