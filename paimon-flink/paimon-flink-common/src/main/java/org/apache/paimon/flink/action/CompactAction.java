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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.postpone.PostponeBucketCompactSplitSource;
import org.apache.paimon.flink.postpone.RewritePostponeBucketCommittableOperator;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.sink.FixedBucketSink;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.RowDataChannelComputer;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;

/** Table compact action for Flink. */
public class CompactAction extends TableActionBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactAction.class);

    private List<Map<String, String>> partitions;

    private String whereSql;

    @Nullable private Duration partitionIdleTime = null;

    private Boolean fullCompaction;

    public CompactAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                            table.getClass().getName()));
        }
        HashMap<String, String> dynamicOptions = new HashMap<>(tableConf);
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        table = table.copy(dynamicOptions);
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public CompactAction withPartitions(List<Map<String, String>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public CompactAction withWhereSql(String whereSql) {
        this.whereSql = whereSql;
        return this;
    }

    public CompactAction withPartitionIdleTime(@Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
        return this;
    }

    public CompactAction withFullCompaction(Boolean fullCompaction) {
        this.fullCompaction = fullCompaction;
        return this;
    }

    @Override
    public void build() throws Exception {
        buildImpl();
    }

    private boolean buildImpl() throws Exception {
        ReadableConfig conf = env.getConfiguration();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        if (fileStoreTable.coreOptions().bucket() == BucketMode.POSTPONE_BUCKET) {
            return buildForPostponeBucketCompaction(env, fileStoreTable, isStreaming);
        } else if (fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            buildForUnawareBucketCompaction(env, fileStoreTable, isStreaming);
            return true;
        } else {
            buildForTraditionalCompaction(env, fileStoreTable, isStreaming);
            return true;
        }
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        if (fullCompaction == null) {
            fullCompaction = !isStreaming;
        } else {
            Preconditions.checkArgument(
                    !(fullCompaction && isStreaming),
                    "The full compact strategy is only supported in batch mode. Please add -Dexecution.runtime-mode=BATCH.");
        }
        if (isStreaming) {
            // for completely asynchronous compaction
            HashMap<String, String> dynamicOptions =
                    new HashMap<String, String>() {
                        {
                            put(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key(), "2147483647");
                            put(CoreOptions.SORT_SPILL_THRESHOLD.key(), "10");
                            put(CoreOptions.LOOKUP_WAIT.key(), "false");
                        }
                    };
            table = table.copy(dynamicOptions);
        }
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(identifier.getFullName(), table);
        CompactorSinkBuilder sinkBuilder = new CompactorSinkBuilder(table, fullCompaction);

        sourceBuilder.withPartitionPredicate(getPredicate());
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(isStreaming)
                        .withPartitionIdleTime(partitionIdleTime)
                        .build();
        sinkBuilder.withInput(source).build();
    }

    private void buildForUnawareBucketCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
                new UnawareBucketCompactionTopoBuilder(env, identifier.getFullName(), table);

        unawareBucketCompactionTopoBuilder.withPartitionPredicate(getPredicate());
        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.withPartitionIdleTime(partitionIdleTime);
        unawareBucketCompactionTopoBuilder.build();
    }

    protected Predicate getPredicate() throws Exception {
        Preconditions.checkArgument(
                partitions == null || whereSql == null,
                "partitions and where cannot be used together.");
        Predicate predicate = null;
        if (partitions != null) {
            predicate =
                    PredicateBuilder.or(
                            partitions.stream()
                                    .map(
                                            p ->
                                                    createPartitionPredicate(
                                                            p,
                                                            table.rowType(),
                                                            ((FileStoreTable) table)
                                                                    .coreOptions()
                                                                    .partitionDefaultName()))
                                    .toArray(Predicate[]::new));
        } else if (whereSql != null) {
            SimpleSqlPredicateConvertor simpleSqlPredicateConvertor =
                    new SimpleSqlPredicateConvertor(table.rowType());
            predicate = simpleSqlPredicateConvertor.convertSqlToPredicate(whereSql);
        }

        // Check whether predicate contain non partition key.
        if (predicate != null) {
            LOGGER.info("the partition predicate of compaction is {}", predicate);
            PartitionPredicateVisitor partitionPredicateVisitor =
                    new PartitionPredicateVisitor(table.partitionKeys());
            Preconditions.checkArgument(
                    predicate.visit(partitionPredicateVisitor),
                    "Only partition key can be specialized in compaction action.");
        }

        return predicate;
    }

    private boolean buildForPostponeBucketCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming) {
        Preconditions.checkArgument(
                !isStreaming, "Postpone bucket compaction currently only supports batch mode");
        Preconditions.checkArgument(
                partitions == null,
                "Postpone bucket compaction currently does not support specifying partitions");
        Preconditions.checkArgument(
                whereSql == null,
                "Postpone bucket compaction currently does not support predicates");

        // change bucket to a positive value, so we can scan files from the bucket = -2 directory
        Map<String, String> bucketOptions = new HashMap<>(table.options());
        bucketOptions.put(CoreOptions.BUCKET.key(), "1");
        FileStoreTable fileStoreTable = table.copy(table.schema().copy(bucketOptions));

        List<BinaryRow> partitions =
                fileStoreTable
                        .newScan()
                        .withBucketFilter(new PostponeBucketFilter())
                        .listPartitions();
        if (partitions.isEmpty()) {
            return false;
        }

        Options options = new Options(fileStoreTable.options());
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        fileStoreTable.coreOptions().partitionDefaultName(),
                        fileStoreTable.rowType(),
                        fileStoreTable.partitionKeys().toArray(new String[0]),
                        fileStoreTable.coreOptions().legacyPartitionName());
        for (BinaryRow partition : partitions) {
            int bucketNum = options.get(FlinkConnectorOptions.POSTPONE_DEFAULT_BUCKET_NUM);

            Iterator<ManifestEntry> it =
                    fileStoreTable
                            .newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucketFilter(new NormalBucketFilter())
                            .readFileIterator();
            if (it.hasNext()) {
                bucketNum = it.next().totalBuckets();
            }

            bucketOptions = new HashMap<>(table.options());
            bucketOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
            FileStoreTable realTable = table.copy(table.schema().copy(bucketOptions));

            LinkedHashMap<String, String> partitionSpec =
                    partitionComputer.generatePartValues(partition);
            Pair<DataStream<RowData>, DataStream<Committable>> sourcePair =
                    PostponeBucketCompactSplitSource.buildSource(
                            env,
                            realTable.fullName() + partitionSpec,
                            realTable.rowType(),
                            realTable
                                    .newReadBuilder()
                                    .withPartitionFilter(partitionSpec)
                                    .withBucketFilter(new PostponeBucketFilter()),
                            options.get(FlinkConnectorOptions.SCAN_PARALLELISM));

            DataStream<InternalRow> partitioned =
                    FlinkStreamPartitioner.partition(
                            FlinkSinkBuilder.mapToInternalRow(
                                    sourcePair.getLeft(), realTable.rowType()),
                            new RowDataChannelComputer(realTable.schema(), false),
                            null);
            FixedBucketSink sink = new FixedBucketSink(realTable, null, null);
            String commitUser =
                    CoreOptions.createCommitUser(realTable.coreOptions().toConfiguration());
            DataStream<Committable> written =
                    sink.doWrite(partitioned, commitUser, partitioned.getParallelism())
                            .forward()
                            .transform(
                                    "Rewrite compact committable",
                                    new CommittableTypeInfo(),
                                    new RewritePostponeBucketCommittableOperator(realTable));
            sink.doCommit(written.union(sourcePair.getRight()), commitUser);
        }

        return true;
    }

    private static class PostponeBucketFilter implements Filter<Integer>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(Integer bucket) {
            return bucket == BucketMode.POSTPONE_BUCKET;
        }
    }

    private static class NormalBucketFilter implements Filter<Integer>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(Integer bucket) {
            return bucket >= 0;
        }
    }

    @Override
    public void run() throws Exception {
        if (buildImpl()) {
            execute("Compact job : " + table.fullName());
        }
    }
}
