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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.TagBatchCreation;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * Commit {@link Committable} for snapshot using the {@link CommitterOperator}. When the task is
 * completed, the corresponding tag is generated.
 */
public class BatchWriteGeneratorTagOperator<CommitT, GlobalCommitT>
        implements OneInputStreamOperator<CommitT, CommitT>, BoundedOneInput {

    private static final String BATCH_WRITE_TAG_PREFIX = "batch-write-";

    private static final long serialVersionUID = 1L;

    private final CommitterOperator<CommitT, GlobalCommitT> commitOperator;

    protected final FileStoreTable table;

    protected final TagBatchCreation tagBatchCreation;

    public BatchWriteGeneratorTagOperator(
            CommitterOperator<CommitT, GlobalCommitT> commitOperator, FileStoreTable table) {
        this.table = table;
        this.commitOperator = commitOperator;
        this.tagBatchCreation = new TagBatchCreation(table);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        commitOperator.initializeState(streamTaskStateManager);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        return commitOperator.snapshotState(
                checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        commitOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        commitOperator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void open() throws Exception {
        commitOperator.open();
    }

    @Override
    public void processElement(StreamRecord<CommitT> element) throws Exception {
        commitOperator.processElement(element);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        commitOperator.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        commitOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        commitOperator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void finish() throws Exception {
        tagBatchCreation.createTag();
        commitOperator.finish();
    }

    @Override
    public void close() throws Exception {
        commitOperator.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        commitOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        commitOperator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        commitOperator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return commitOperator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return commitOperator.getOperatorID();
    }

    @Override
    public void setCurrentKey(Object key) {
        commitOperator.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return commitOperator.getCurrentKey();
    }

    @Override
    public void setKeyContextElement(StreamRecord<CommitT> record) throws Exception {
        commitOperator.setKeyContextElement(record);
    }

    @Override
    public void endInput() throws Exception {
        commitOperator.endInput();
    }
}
