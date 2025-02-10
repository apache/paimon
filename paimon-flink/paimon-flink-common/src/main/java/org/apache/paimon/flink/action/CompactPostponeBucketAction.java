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
import org.apache.paimon.flink.postpone.PostponeBucketCompactSplitSource;
import org.apache.paimon.flink.postpone.RewritePostponeBucketCommittableOperator;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.FixedBucketSink;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.RowDataChannelComputer;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Action to compact postpone bucket tables, which distributes records in {@code bucket = -2}
 * directory into real bucket directories.
 */
public class CompactPostponeBucketAction extends TableActionBase {

    public static final int DEFAULT_BUCKET_NUM_NEW_PARTITION = 4;

    private int defaultBucketNum = DEFAULT_BUCKET_NUM_NEW_PARTITION;
    @Nullable private Integer parallelism = null;

    public CompactPostponeBucketAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public CompactPostponeBucketAction withDefaultBucketNum(int defaultBucketNum) {
        this.defaultBucketNum = defaultBucketNum;
        return this;
    }

    public CompactPostponeBucketAction withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public void build() throws Exception {
        buildImpl();
    }

    public List<String> buildImpl() {
        Preconditions.checkArgument(
                String.valueOf(BucketMode.POSTPONE_BUCKET)
                        .equals(table.options().get(CoreOptions.BUCKET.key())),
                "Compact postpone bucket action can only be used for bucket = -2 tables");

        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(flinkConf);

        // change bucket to a positive value, so we can scan files from the bucket = -2 directory
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.BUCKET.key(), "1");
        FileStoreTable fileStoreTable = (FileStoreTable) table.copy(dynamicOptions);

        List<BinaryRow> partitions =
                fileStoreTable
                        .newScan()
                        .withBucketFilter(new PostponeBucketFilter())
                        .listPartitions();
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        fileStoreTable.coreOptions().partitionDefaultName(),
                        fileStoreTable.rowType(),
                        fileStoreTable.partitionKeys().toArray(new String[0]),
                        fileStoreTable.coreOptions().legacyPartitionName());
        for (BinaryRow partition : partitions) {
            int bucketNum = defaultBucketNum;

            Iterator<ManifestEntry> it =
                    fileStoreTable
                            .newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucketFilter(new NormalBucketFilter())
                            .readFileIterator();
            if (it.hasNext()) {
                bucketNum = it.next().totalBuckets();
            }

            dynamicOptions = new HashMap<>();
            dynamicOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
            FileStoreTable realTable = (FileStoreTable) table.copy(dynamicOptions);

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
                                    .withBucketFilter(new PostponeBucketFilter()));

            DataStream<InternalRow> partitioned =
                    FlinkStreamPartitioner.partition(
                            FlinkSinkBuilder.mapToInternalRow(
                                    sourcePair.getLeft(), realTable.rowType()),
                            new RowDataChannelComputer(realTable.schema(), false),
                            parallelism);
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

        List<String> ret = new ArrayList<>();
        for (BinaryRow partition : partitions) {
            ret.add(
                    InternalRowPartitionComputer.partToSimpleString(
                            fileStoreTable.schema().logicalPartitionType(),
                            partition,
                            ",",
                            Integer.MAX_VALUE));
        }
        return ret;
    }

    @Override
    public void run() throws Exception {
        if (!buildImpl().isEmpty()) {
            env.execute("Compact Postpone Bucket : " + table.name());
        }
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
}
