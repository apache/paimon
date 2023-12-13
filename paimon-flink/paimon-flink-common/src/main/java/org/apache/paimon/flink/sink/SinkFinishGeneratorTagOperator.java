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

import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Commit {@link Committable} for snapshot using the {@link CommitterOperator}. When the task is
 * completed, the corresponding tag is generated.
 */
public class SinkFinishGeneratorTagOperator<CommitT, GlobalCommitT>
        implements OneInputStreamOperator<CommitT, CommitT>,
                SetupableStreamOperator,
                BoundedOneInput {

    private static final String SINK_FINISH_TAG_PREFIX = "sinkFinish-";

    private static final long serialVersionUID = 1L;

    private final CommitterOperator<CommitT, GlobalCommitT> commitOperator;

    protected final FileStoreTable table;

    public SinkFinishGeneratorTagOperator(
            CommitterOperator<CommitT, GlobalCommitT> commitOperator, FileStoreTable table) {
        this.table = table;
        this.commitOperator = commitOperator;
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

    private void createTag() {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null) {
            return;
        }
        TagManager tagManager = table.tagManager();
        TagDeletion tagDeletion = table.store().newTagDeletion();
        Instant instant = Instant.ofEpochMilli(snapshot.timeMillis());
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String tagName =
                SINK_FINISH_TAG_PREFIX
                        + localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        try {
            // If the tag already exists, delete the tag
            if (tagManager.tagExists(tagName)) {
                tagManager.deleteTag(tagName, tagDeletion, snapshotManager);
            }
            // Create a new tag
            tagManager.createTag(snapshot, tagName);
        } catch (Exception e) {
            if (tagManager.tagExists(tagName)) {
                tagManager.deleteTag(tagName, tagDeletion, snapshotManager);
            }
        }
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
        createTag();
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

    @Override
    public void setup(StreamTask containingTask, StreamConfig config, Output output) {
        commitOperator.setup(containingTask, config, output);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return commitOperator.getChainingStrategy();
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        commitOperator.setChainingStrategy(strategy);
    }
}
