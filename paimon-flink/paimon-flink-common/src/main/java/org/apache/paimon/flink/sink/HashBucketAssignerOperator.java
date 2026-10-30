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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Assign bucket for the input record, output record with bucket. */
public class HashBucketAssignerOperator<T> extends AbstractStreamOperator<Tuple2<T, Integer>>
        implements OneInputStreamOperator<T, Tuple2<T, Integer>> {

    private static final long serialVersionUID = 1L;

    private final String initialCommitUser;

    private final FileStoreTable table;
    private final Integer numAssigners;
    private final SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction;
    private final boolean overwrite;

    private transient BucketAssigner assigner;
    private transient PartitionKeyExtractor<T> extractor;
    private transient boolean reuseDeletedSlots;

    public HashBucketAssignerOperator(
            String commitUser,
            Table table,
            Integer numAssigners,
            SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction,
            boolean overwrite) {
        this.initialCommitUser = commitUser;
        this.table = (FileStoreTable) table;
        this.numAssigners = numAssigners;
        this.extractorFunction = extractorFunction;
        this.overwrite = overwrite;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Each job can only have one username and this name must be consistent across restarts.
        // We cannot use job id as commit username here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        String commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        int numberTasks = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        int taskId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        long targetRowNum = table.coreOptions().dynamicBucketTargetRowNum();
        Integer maxBucketsNum = table.coreOptions().dynamicBucketMaxBuckets();
        this.assigner =
                overwrite
                        ? new SimpleHashBucketAssigner(
                                numberTasks, taskId, targetRowNum, maxBucketsNum)
                        : new HashBucketAssigner(
                                table.snapshotManager(),
                                commitUser,
                                table.store().newIndexFileHandler(),
                                numberTasks,
                                MathUtils.min(numAssigners, numberTasks),
                                taskId,
                                targetRowNum,
                                maxBucketsNum);
        this.extractor = extractorFunction.apply(table.schema());
        this.reuseDeletedSlots = table.coreOptions().dynamicBucketReuseDeletedSlots();
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        T value = streamRecord.getValue();
        BinaryRow partition = extractor.partition(value);
        int hash = extractor.trimmedPrimaryKey(value).hashCode();
        int bucket = assigner.assign(partition, hash);
        if (reuseDeletedSlots && extractor.isDelete(value)) {
            // Assign first: the delete record must still be routed to the bucket of its key. Then
            // free the slot of the key, so that the bucket can take a new key. A delete of an
            // unknown key takes a slot in assign and gives it back here, with no net effect.
            assigner.delete(partition, hash);
        }
        output.collect(new StreamRecord<>(new Tuple2<>(value, bucket)));
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        assigner.prepareCommit(checkpointId);
    }
}
