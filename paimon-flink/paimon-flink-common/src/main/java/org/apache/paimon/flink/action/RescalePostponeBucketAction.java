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
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.Map;

/** Action to rescale one partition of postpone bucket tables. */
public class RescalePostponeBucketAction extends TableActionBase {

    private final int bucketNum;
    private Map<String, String> partition = new HashMap<>();

    public RescalePostponeBucketAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            int bucketNum) {
        super(databaseName, tableName, catalogConfig);
        this.bucketNum = bucketNum;
    }

    public RescalePostponeBucketAction withPartition(Map<String, String> partition) {
        this.partition = partition;
        return this;
    }

    @Override
    public void build() throws Exception {
        Preconditions.checkArgument(
                String.valueOf(BucketMode.POSTPONE_BUCKET)
                        .equals(table.options().get(CoreOptions.BUCKET.key())),
                "Compact postpone bucket action can only be used for bucket = -2 tables");

        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(flinkConf);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        RowType partitionType = fileStoreTable.schema().logicalPartitionType();
        Predicate partitionPredicate =
                PartitionPredicate.createPartitionPredicate(
                        partitionType,
                        InternalRowPartitionComputer.convertSpecToInternal(
                                partition,
                                partitionType,
                                fileStoreTable.coreOptions().partitionDefaultName()));
        DataStream<RowData> source =
                new FlinkSourceBuilder(fileStoreTable)
                        .env(env)
                        .sourceBounded(true)
                        .predicate(partitionPredicate)
                        .build();

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        FileStoreTable rescaledTable = fileStoreTable.copy(dynamicOptions);
        new FlinkSinkBuilder(rescaledTable).overwrite(partition).forRowData(source).build();
    }

    @Override
    public void run() throws Exception {
        build();
        env.execute("Rescale Postpone Bucket : " + table.name());
    }
}
