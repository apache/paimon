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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommitterOperator;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A committer for MergeInto which wraps a normal committer and provide the ability to check updated
 * columns.
 *
 * <p>This operator is only for batch mode.
 */
public class MergeIntoCommitterOperator
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(MergeIntoCommitterOperator.class);

    private final FileStoreTable table;
    private final Set<String> updatedColumns;
    private final CommitterOperator<Committable, ManifestCommittable> committerOperator;

    private transient Set<BinaryRow> affectedPartitions;

    public MergeIntoCommitterOperator(
            CommitterOperator<Committable, ManifestCommittable> committerOperator,
            FileStoreTable table,
            Set<String> updatedColumns) {
        this.table = table;
        this.updatedColumns = updatedColumns;
        this.committerOperator = committerOperator;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        committerOperator.initializeState(streamTaskStateManager);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        return committerOperator.snapshotState(
                checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        committerOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        committerOperator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void open() throws Exception {
        committerOperator.open();
        affectedPartitions = new HashSet<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        affectedPartitions.add(element.getValue().commitMessage().partition());
        committerOperator.processElement(element);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        committerOperator.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        committerOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        committerOperator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void finish() throws Exception {
        committerOperator.finish();
    }

    @Override
    public void close() throws Exception {
        committerOperator.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        committerOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        committerOperator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        committerOperator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return committerOperator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return committerOperator.getOperatorID();
    }

    @Override
    public void setCurrentKey(Object key) {
        committerOperator.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return committerOperator.getCurrentKey();
    }

    @Override
    public void setKeyContextElement(StreamRecord<Committable> record) throws Exception {
        committerOperator.setKeyContextElement(record);
    }

    @Override
    public void endInput() throws Exception {
        checkUpdatedColumns();
        committerOperator.endInput();
    }

    private void checkUpdatedColumns() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        RowType rowType = table.rowType();
        Preconditions.checkState(latestSnapshot.isPresent());

        List<IndexManifestEntry> affectedEntries =
                table.store()
                        .newIndexFileHandler()
                        .scan(
                                latestSnapshot.get(),
                                entry -> {
                                    GlobalIndexMeta globalIndexMeta =
                                            entry.indexFile().globalIndexMeta();
                                    if (globalIndexMeta != null) {
                                        String fieldName =
                                                rowType.getField(globalIndexMeta.indexFieldId())
                                                        .name();
                                        return updatedColumns.contains(fieldName)
                                                && affectedPartitions.contains(entry.partition());
                                    }
                                    return false;
                                });

        if (!affectedEntries.isEmpty()) {
            CoreOptions.GlobalIndexColumnUpdateAction updateAction =
                    table.coreOptions().globalIndexColumnUpdateAction();
            switch (updateAction) {
                case THROW_ERROR:
                    Set<String> conflictedColumns =
                            affectedEntries.stream()
                                    .map(file -> file.indexFile().globalIndexMeta().indexFieldId())
                                    .map(id -> rowType.getField(id).name())
                                    .collect(Collectors.toSet());

                    throw new RuntimeException(
                            String.format(
                                    "MergeInto: update columns contain globally indexed columns, not supported now.\n"
                                            + "Updated columns: %s\nConflicted columns: %s\n",
                                    updatedColumns, conflictedColumns));
                case DROP_PARTITION_INDEX:
                    Map<BinaryRow, List<IndexFileMeta>> entriesByParts =
                            affectedEntries.stream()
                                    .collect(
                                            Collectors.groupingBy(
                                                    IndexManifestEntry::partition,
                                                    Collectors.mapping(
                                                            IndexManifestEntry::indexFile,
                                                            Collectors.toList())));

                    for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry :
                            entriesByParts.entrySet()) {
                        LOG.debug(
                                "Dropping index files {} due to indexed fields update.",
                                entry.getValue());

                        CommitMessage commitMessage =
                                new CommitMessageImpl(
                                        entry.getKey(),
                                        0,
                                        null,
                                        DataIncrement.deleteIndexIncrement(entry.getValue()),
                                        CompactIncrement.emptyIncrement());

                        Committable committable = new Committable(Long.MAX_VALUE, commitMessage);

                        committerOperator.processElement(new StreamRecord<>(committable));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported option: " + updateAction);
            }
        }
    }
}
