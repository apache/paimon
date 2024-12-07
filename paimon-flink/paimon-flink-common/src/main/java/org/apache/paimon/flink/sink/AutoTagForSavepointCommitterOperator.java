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
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.SerializableSupplier;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Commit {@link Committable} for each snapshot using the {@link CommitterOperator}. At the same
 * time, tags are automatically created for each flink savepoint.
 */
public class AutoTagForSavepointCommitterOperator<CommitT, GlobalCommitT>
        implements OneInputStreamOperator<CommitT, CommitT>, BoundedOneInput {
    public static final String SAVEPOINT_TAG_PREFIX = "savepoint-";

    private static final long serialVersionUID = 1L;

    private final CommitterOperator<CommitT, GlobalCommitT> commitOperator;

    private final SerializableSupplier<SnapshotManager> snapshotManagerFactory;

    private final SerializableSupplier<TagManager> tagManagerFactory;

    private final SerializableSupplier<TagDeletion> tagDeletionFactory;

    private final SerializableSupplier<List<TagCallback>> callbacksSupplier;

    private final NavigableSet<Long> identifiersForTags;

    private final Duration tagTimeRetained;

    private transient SnapshotManager snapshotManager;

    private transient TagManager tagManager;

    private transient TagDeletion tagDeletion;

    private transient List<TagCallback> callbacks;

    private transient ListState<Long> identifiersForTagsState;

    public AutoTagForSavepointCommitterOperator(
            CommitterOperator<CommitT, GlobalCommitT> commitOperator,
            SerializableSupplier<SnapshotManager> snapshotManagerFactory,
            SerializableSupplier<TagManager> tagManagerFactory,
            SerializableSupplier<TagDeletion> tagDeletionFactory,
            SerializableSupplier<List<TagCallback>> callbacksSupplier,
            Duration tagTimeRetained) {
        this.commitOperator = commitOperator;
        this.tagManagerFactory = tagManagerFactory;
        this.snapshotManagerFactory = snapshotManagerFactory;
        this.tagDeletionFactory = tagDeletionFactory;
        this.callbacksSupplier = callbacksSupplier;
        this.identifiersForTags = new TreeSet<>();
        this.tagTimeRetained = tagTimeRetained;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        try {
            commitOperator.initializeState(streamTaskStateManager);
        } finally {
            snapshotManager = snapshotManagerFactory.get();
            tagManager = tagManagerFactory.get();
            tagDeletion = tagDeletionFactory.get();
            callbacks = callbacksSupplier.get();

            identifiersForTagsState =
                    commitOperator
                            .getOperatorStateBackend()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "streaming_committer_for_tags_states",
                                            LongSerializer.INSTANCE));
            List<Long> restored = new ArrayList<>();
            identifiersForTagsState.get().forEach(restored::add);
            identifiersForTagsState.clear();
            createTagForIdentifiers(restored);
        }
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        if (checkpointOptions.getCheckpointType().isSavepoint()) {
            identifiersForTags.add(checkpointId);
        }
        identifiersForTagsState.update(new ArrayList<>(identifiersForTags));
        return commitOperator.snapshotState(
                checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        commitOperator.notifyCheckpointComplete(checkpointId);
        Set<Long> headSet = identifiersForTags.headSet(checkpointId, true);
        if (!headSet.isEmpty()) {
            createTagForIdentifiers(new ArrayList<>(headSet));
            headSet.clear();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        commitOperator.notifyCheckpointAborted(checkpointId);
        identifiersForTags.remove(checkpointId);
        String tagName = SAVEPOINT_TAG_PREFIX + checkpointId;
        if (tagManager.tagExists(tagName)) {
            tagManager.deleteTag(tagName, tagDeletion, snapshotManager, callbacks);
        }
    }

    private void createTagForIdentifiers(List<Long> identifiers) {
        List<Snapshot> snapshotForTags =
                snapshotManager.findSnapshotsForIdentifiers(
                        commitOperator.getCommitUser(), identifiers);
        for (Snapshot snapshot : snapshotForTags) {
            String tagName = SAVEPOINT_TAG_PREFIX + snapshot.commitIdentifier();
            if (!tagManager.tagExists(tagName)) {
                tagManager.createTag(snapshot, tagName, tagTimeRetained, callbacks);
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
