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
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.RescaleFileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/** Action to rescale one partition of a table. */
public class RescaleAction extends TableActionBase {

    private @Nullable Integer bucketNum;
    private Map<String, String> partition = new HashMap<>();
    private @Nullable Integer scanParallelism;
    private @Nullable Integer sinkParallelism;

    public RescaleAction(String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public RescaleAction withBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
        return this;
    }

    public RescaleAction withPartition(Map<String, String> partition) {
        this.partition = partition;
        return this;
    }

    public RescaleAction withScanParallelism(int scanParallelism) {
        this.scanParallelism = scanParallelism;
        return this;
    }

    public RescaleAction withSinkParallelism(int sinkParallelism) {
        this.sinkParallelism = sinkParallelism;
        return this;
    }

    @Override
    public void build() throws Exception {
        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(flinkConf);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Optional<Snapshot> optionalSnapshot = fileStoreTable.latestSnapshot();
        if (!optionalSnapshot.isPresent()) {
            throw new IllegalArgumentException(
                    "Table " + table.fullName() + " has no snapshot. No need to rescale.");
        }
        Snapshot snapshot = optionalSnapshot.get();

        // If someone commits while the rescale job is running, this commit will be lost.
        // So we use strict mode to make sure nothing is lost.
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(
                CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(),
                String.valueOf(snapshot.id()));
        fileStoreTable = fileStoreTable.copy(dynamicOptions);

        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromMap(
                        fileStoreTable.schema().logicalPartitionType(),
                        partition,
                        fileStoreTable.coreOptions().partitionDefaultName());

        DataStream<RowData> source =
                new FlinkSourceBuilder(fileStoreTable)
                        .env(env)
                        .sourceBounded(true)
                        .sourceParallelism(
                                scanParallelism == null
                                        ? currentBucketNum(snapshot)
                                        : scanParallelism)
                        .partitionPredicate(partitionPredicate)
                        .build();
        Map<String, String> bucketOptions = new HashMap<>(fileStoreTable.options());
        if (bucketNum == null) {
            Preconditions.checkArgument(
                    fileStoreTable.coreOptions().bucket() != BucketMode.POSTPONE_BUCKET,
                    "When rescaling postpone bucket tables, you must provide the resulting bucket number.");
        } else {
            bucketOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        }

        RescaleFileStoreTable rescaledTable =
                new RescaleFileStoreTable(
                        fileStoreTable.copy(fileStoreTable.schema().copy(bucketOptions)));

        new FlinkSinkBuilder(rescaledTable)
                .overwrite(partition)
                .parallelism(sinkParallelism == null ? bucketNum : sinkParallelism)
                .forRowData(source)
                .build();
    }

    @Override
    public void run() throws Exception {
        build();
        env.execute("Rescale : " + table.fullName());
    }

    private int currentBucketNum(Snapshot snapshot) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Iterator<ManifestEntry> it =
                fileStoreTable
                        .newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partition)
                        .onlyReadRealBuckets()
                        .readFileIterator();
        Preconditions.checkArgument(
                it.hasNext(),
                "The specified partition does not have any data files. No need to rescale.");
        return it.next().totalBuckets();
    }
}
