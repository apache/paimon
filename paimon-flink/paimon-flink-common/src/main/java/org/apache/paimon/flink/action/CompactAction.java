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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.compact.AppendTableCompact;
import org.apache.paimon.flink.compact.DataEvolutionTableCompact;
import org.apache.paimon.flink.compact.IncrementalClusterCompact;
import org.apache.paimon.flink.postpone.PostponeBucketCompactOperator;
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
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PostponeUtils;
import org.apache.paimon.table.PostponeUtils.CompactBucket;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Table compact action for Flink. */
public class CompactAction extends TableActionBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactAction.class);

    protected List<Map<String, String>> partitions;

    protected String whereSql;

    @Nullable protected Duration partitionIdleTime = null;

    @Nullable protected Boolean fullCompaction;

    public CompactAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig);
        this.forceStartFlinkJob = true;
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
        if (!buildImpl()) {
            // empty input: no-op topology so procedure execution succeeds without commit
            env.fromSequence(0, 0).name("Nothing to Sort Compact").sinkTo(new DiscardingSink<>());
        }
    }

    protected boolean buildImpl() throws Exception {
        ReadableConfig conf = env.getConfiguration();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        PartitionPredicate partitionPredicate = getPartitionPredicate();
        if (fileStoreTable.coreOptions().bucket() == BucketMode.POSTPONE_BUCKET) {
            buildForPostponeBucketCompaction(env, fileStoreTable, isStreaming);
        } else if (fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            if (fileStoreTable.coreOptions().dataEvolutionEnabled()) {
                buildForDataEvolutionTableCompact(env, fileStoreTable, isStreaming);
            } else if (fileStoreTable.coreOptions().clusteringIncrementalEnabled()) {
                new IncrementalClusterCompact(
                                env, fileStoreTable, partitionPredicate, fullCompaction)
                        .build();
            } else {
                buildForAppendTableCompact(env, fileStoreTable, isStreaming);
            }
        } else {
            buildForBucketedTableCompact(env, fileStoreTable, isStreaming);
        }
        return true;
    }

    protected void buildForBucketedTableCompact(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        if (fullCompaction == null) {
            if (table.coreOptions().bucketClusterEnabled()) {
                fullCompaction = false;
            } else {
                fullCompaction = !isStreaming;
            }
        } else {
            checkArgument(
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

        sourceBuilder.withPartitionPredicate(getPartitionPredicate());
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(isStreaming)
                        .withPartitionIdleTime(partitionIdleTime)
                        .build();
        sinkBuilder.withInput(source).build();
    }

    protected void buildForAppendTableCompact(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        AppendTableCompact builder = new AppendTableCompact(env, identifier.getFullName(), table);
        builder.withPartitionPredicate(getPartitionPredicate());
        builder.withContinuousMode(isStreaming);
        builder.withPartitionIdleTime(partitionIdleTime);
        builder.build();
    }

    protected void buildForDataEvolutionTableCompact(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        checkArgument(!isStreaming, "Data evolution table compact only supports batch mode yet.");
        DataEvolutionTableCompact builder =
                new DataEvolutionTableCompact(env, identifier.getFullName(), table);
        builder.withPartitionPredicate(getPartitionPredicate());
        builder.build();
    }

    protected PartitionPredicate getPartitionPredicate() throws Exception {
        checkArgument(
                partitions == null || whereSql == null,
                "partitions and where cannot be used together.");
        Predicate predicate = null;
        RowType partitionType = table.rowType().project(table.partitionKeys());
        String partitionDefaultName = ((FileStoreTable) table).coreOptions().partitionDefaultName();
        if (partitions != null) {
            boolean fullMode =
                    partitions.stream()
                            .allMatch(part -> part.size() == partitionType.getFieldCount());
            if (fullMode) {
                List<BinaryRow> binaryPartitions =
                        createBinaryPartitions(partitions, partitionType, partitionDefaultName);
                return PartitionPredicate.fromMultiple(partitionType, binaryPartitions);
            } else {
                // partitions may be partial partition fields, so here must to use predicate way.
                predicate =
                        partitions.stream()
                                .map(
                                        partition ->
                                                createPartitionPredicate(
                                                        partition,
                                                        table.rowType(),
                                                        partitionDefaultName))
                                .reduce(PredicateBuilder::or)
                                .orElseThrow(
                                        () ->
                                                new RuntimeException(
                                                        "Failed to get partition filter."));
            }
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
            checkArgument(
                    predicate.visit(partitionPredicateVisitor),
                    "Only partition key can be specialized in compaction action.");
            predicate =
                    predicate
                            .visit(
                                    PredicateProjectionConverter.fromProjection(
                                            table.rowType().projectIndexes(table.partitionKeys())))
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Failed to convert partition predicate."));
        }

        return PartitionPredicate.fromPredicate(partitionType, predicate);
    }

    protected boolean buildForPostponeBucketCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming) {
        checkArgument(
                !isStreaming, "Postpone bucket compaction currently only supports batch mode");
        checkArgument(
                partitions == null,
                "Postpone bucket compaction currently does not support specifying partitions");
        checkArgument(
                whereSql == null,
                "Postpone bucket compaction currently does not support predicates");

        Options options = new Options(table.options());
        int defaultBucketNum = options.get(CoreOptions.POSTPONE_DEFAULT_BUCKET_NUM);
        Optional<Long> targetRowNumPerBucket =
                options.getOptional(CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET);
        Optional<Snapshot> optionalSnapshot = table.latestSnapshot();
        if (!optionalSnapshot.isPresent()) {
            return buildNothingToCompact(env);
        }
        long snapshotId = optionalSnapshot.get().id();
        Map<BinaryRow, Integer> knownNumBuckets =
                PostponeUtils.getKnownNumBuckets(table, snapshotId);
        Map<BinaryRow, Long> postponeRowCounts =
                targetRowNumPerBucket.isPresent()
                        ? PostponeUtils.getPostponeRowCounts(table, snapshotId)
                        : Collections.emptyMap();

        List<BinaryRow> postponePartitions =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .partitions();

        Map<BinaryRow, List<CompactBucket>> compactBucketsByPartition = new LinkedHashMap<>();
        for (CompactBucket bucket : PostponeUtils.getLevel0Buckets(table, snapshotId)) {
            compactBucketsByPartition
                    .computeIfAbsent(bucket.partition(), ignored -> new ArrayList<>())
                    .add(bucket);
        }
        Set<BinaryRow> affectedPartitions = new LinkedHashSet<>(postponePartitions);
        affectedPartitions.addAll(compactBucketsByPartition.keySet());
        if (affectedPartitions.isEmpty()) {
            return buildNothingToCompact(env);
        }

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        table.coreOptions().partitionDefaultName(),
                        table.store().partitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        table.coreOptions().legacyPartitionName());
        String commitUser = CoreOptions.createCommitUser(options);
        List<DataStream<Committable>> dataStreams = new ArrayList<>();
        for (BinaryRow partition : affectedPartitions) {
            int bucketNum =
                    PostponeUtils.determineBucketNum(
                            partition,
                            knownNumBuckets,
                            targetRowNumPerBucket,
                            postponeRowCounts,
                            defaultBucketNum);
            FileStoreTable realTable =
                    PostponeUtils.tableForPostponeCompact(table, bucketNum, snapshotId);

            LinkedHashMap<String, String> partitionSpec =
                    partitionComputer.generatePartValues(partition);
            Pair<DataStream<RowData>, DataStream<Committable>> sourcePair =
                    PostponeBucketCompactSplitSource.buildSource(
                            env,
                            realTable,
                            partitionSpec,
                            snapshotId,
                            options.get(FlinkConnectorOptions.SCAN_PARALLELISM));

            DataStream<InternalRow> partitioned =
                    FlinkStreamPartitioner.partition(
                            FlinkSinkBuilder.mapToInternalRow(
                                    sourcePair.getLeft(),
                                    realTable.rowType(),
                                    table.catalogEnvironment().catalogContext()),
                            new RowDataChannelComputer(realTable.schema()),
                            null);
            List<CompactBucket> compactBuckets =
                    compactBucketsByPartition.getOrDefault(partition, Collections.emptyList());
            DataStream<CompactBucket> partitionedCompactBuckets =
                    FlinkStreamPartitioner.partition(
                            env.fromCollection(
                                            compactBuckets, new JavaTypeInfo<>(CompactBucket.class))
                                    .name(
                                            String.format(
                                                    "Level-0 compact buckets: %s - %s",
                                                    table.fullName(), partitionSpec))
                                    .forceNonParallel(),
                            new CompactBucketChannelComputer(),
                            partitioned.getParallelism());
            DataStream<Committable> written =
                    partitioned
                            .connect(partitionedCompactBuckets)
                            .transform(
                                    String.format(
                                            "Write and compact postpone buckets: %s - %s",
                                            table.fullName(), partitionSpec),
                                    new CommittableTypeInfo(),
                                    new PostponeBucketCompactOperator(
                                            realTable, commitUser, snapshotId))
                            .forward()
                            .transform(
                                    "Rewrite compact committable",
                                    new CommittableTypeInfo(),
                                    new RewritePostponeBucketCommittableOperator(realTable));
            dataStreams.add(written);
            dataStreams.add(sourcePair.getRight());
        }

        FileStoreTable fileStoreTable =
                PostponeUtils.tableForPostponeCompact(table, defaultBucketNum, snapshotId);
        FixedBucketSink sink = new FixedBucketSink(fileStoreTable, null);
        DataStream<Committable> dataStream = dataStreams.get(0);
        for (int i = 1; i < dataStreams.size(); i++) {
            dataStream = dataStream.union(dataStreams.get(i));
        }
        sink.doCommit(dataStream, commitUser);
        return true;
    }

    private boolean buildNothingToCompact(StreamExecutionEnvironment env) {
        if (this.forceStartFlinkJob) {
            env.fromSequence(0, 0).name("Nothing to Compact Source").sinkTo(new DiscardingSink<>());
            return true;
        }
        return false;
    }

    private static class CompactBucketChannelComputer implements ChannelComputer<CompactBucket> {

        private static final long serialVersionUID = 1L;

        private transient int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(CompactBucket bucket) {
            return ChannelComputer.select(bucket.partition(), bucket.bucket(), numChannels);
        }
    }

    @Override
    public void run() throws Exception {
        if (buildImpl()) {
            execute("Compact job : " + table.fullName());
        }
    }
}
