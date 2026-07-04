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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Default topology builder. */
public class DefaultGlobalIndexTopoBuilder implements GlobalIndexTopologyBuilder {

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
        return buildIndex(
                spark,
                relation,
                partitionPredicate,
                table,
                indexType,
                readType,
                indexField,
                Collections.emptyList(),
                options);
    }

    @Override
    public List<CommitMessage> buildIndex(
            SparkSession spark,
            DataSourceV2Relation relation,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            List<DataField> extraFields,
            Options options)
            throws IOException {
        Options tableOptions = table.coreOptions().toConfiguration();
        long rowsPerShard =
                tableOptions
                        .getOptional(GLOBAL_INDEX_ROW_COUNT_PER_SHARD)
                        .orElse(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.defaultValue());
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }
        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionPredicate)
                        .plan()
                        .files();
        List<DataField> indexFields = new ArrayList<>();
        indexFields.add(indexField);
        indexFields.addAll(extraFields);
        List<String> indexColumns =
                indexFields.stream().map(DataField::name).collect(Collectors.toList());
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        long boundaryRowId =
                GlobalIndexBuilderUtils.findMinNonIndexableRowId(
                        schemaManager, entries, indexColumns);
        entries = GlobalIndexBuilderUtils.filterEntriesBefore(entries, boundaryRowId);
        List<Range> rowRangesToBuild =
                GlobalIndexBuilderUtils.unindexedRowRanges(
                        table, snapshot, indexType, indexFields, partitionPredicate);
        if (rowRangesToBuild.isEmpty()) {
            return Collections.emptyList();
        }
        // generate splits for each partition && shard
        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        table, entries, rowsPerShard, rowRangesToBuild);

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        List<Pair<byte[], byte[]>> taskList = new ArrayList<>();
        for (IndexedSplit indexedSplit : splits) {
            checkArgument(
                    indexedSplit.rowRanges().size() == 1,
                    "Each IndexedSplit should contain exactly one row range.");
            DefaultGlobalIndexBuilder builder =
                    new DefaultGlobalIndexBuilder(
                            table,
                            indexedSplit.dataSplit().partition(),
                            readType,
                            indexField,
                            extraFields,
                            indexType,
                            indexedSplit.rowRanges().get(0),
                            options);
            byte[] builderBytes = InstantiationUtil.serializeObject(builder);
            byte[] splitBytes = InstantiationUtil.serializeObject(indexedSplit);
            taskList.add(Pair.of(builderBytes, splitBytes));
        }

        if (taskList.isEmpty()) {
            return Collections.emptyList();
        }

        List<byte[]> commitMessageBytes =
                javaSparkContext
                        .parallelize(taskList, taskList.size())
                        .map(DefaultGlobalIndexTopoBuilder::buildIndex)
                        .collect();
        return CommitMessageSerializer.deserializeAll(commitMessageBytes);
    }

    private static byte[] buildIndex(Pair<byte[], byte[]> builderAndSplits) throws Exception {
        ClassLoader classLoader = DefaultGlobalIndexBuilder.class.getClassLoader();
        DefaultGlobalIndexBuilder indexBuilder =
                InstantiationUtil.deserializeObject(builderAndSplits.getLeft(), classLoader);
        byte[] dataSplitBytes = builderAndSplits.getRight();
        IndexedSplit split = InstantiationUtil.deserializeObject(dataSplitBytes, classLoader);
        ReadBuilder builder = indexBuilder.table().newReadBuilder();
        builder.withReadType(indexBuilder.readType());

        try (RecordReader<InternalRow> recordReader = builder.newRead().createReader(split);
                CloseableIterator<InternalRow> data = recordReader.toCloseableIterator()) {
            CommitMessage commitMessage = indexBuilder.build(data);
            return new CommitMessageSerializer().serialize(commitMessage);
        }
    }

    /**
     * Groups files into shards by partition. This method delegates to the generic global index
     * build planner and keeps the previous test surface stable.
     *
     * @param entriesByPartition manifest entries grouped by partition
     * @param rowsPerShard number of rows per shard
     * @param pathFactory path factory for creating bucket paths
     * @return map of partition to shard splits
     */
    public static Map<BinaryRow, List<IndexedSplit>> groupFilesIntoShardsByPartition(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, Path> pathFactory) {
        return groupFilesIntoShardsByPartition(entriesByPartition, rowsPerShard, pathFactory, null);
    }

    public static Map<BinaryRow, List<IndexedSplit>> groupFilesIntoShardsByPartition(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, Path> pathFactory,
            @Nullable List<Range> rowRangesToBuild) {
        List<ManifestEntry> entries =
                entriesByPartition.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return GlobalIndexBuilderUtils.createShardIndexedSplits(
                        entries,
                        rowsPerShard,
                        (partition, bucket) -> pathFactory.apply(partition, bucket).toString(),
                        rowRangesToBuild)
                .stream()
                .collect(Collectors.groupingBy(split -> split.dataSplit().partition()));
    }
}
