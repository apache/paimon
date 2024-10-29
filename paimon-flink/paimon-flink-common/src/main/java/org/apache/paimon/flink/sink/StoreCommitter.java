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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.sink.partition.PartitionListeners;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableCommitImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link Committer} for dynamic store. */
public class StoreCommitter implements Committer<Committable, ManifestCommittable> {

    private final TableCommitImpl commit;
    @Nullable private final CommitterMetrics committerMetrics;
    private final PartitionListeners partitionListeners;

    public StoreCommitter(FileStoreTable table, TableCommit commit, Context context) {
        this.commit = (TableCommitImpl) commit;

        if (context.metricGroup() != null) {
            this.commit.withMetricRegistry(new FlinkMetricRegistry(context.metricGroup()));
            this.committerMetrics = new CommitterMetrics(context.metricGroup().getIOMetricGroup());
        } else {
            this.committerMetrics = null;
        }

        try {
            this.partitionListeners =
                    PartitionListeners.create(
                            context.streamingCheckpointEnabled(),
                            context.isRestored(),
                            context.stateStore(),
                            table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public CommitterMetrics getCommitterMetrics() {
        return committerMetrics;
    }

    @Override
    public boolean forceCreatingSnapshot() {
        return commit.forceCreatingSnapshot();
    }

    @Override
    public ManifestCommittable combine(
            long checkpointId, long watermark, List<Committable> committables) {
        ManifestCommittable manifestCommittable = new ManifestCommittable(checkpointId, watermark);
        return combine(checkpointId, watermark, manifestCommittable, committables);
    }

    @Override
    public ManifestCommittable combine(
            long checkpointId,
            long watermark,
            ManifestCommittable manifestCommittable,
            List<Committable> committables) {
        for (Committable committable : committables) {
            switch (committable.kind()) {
                case FILE:
                    CommitMessage file = (CommitMessage) committable.wrappedCommittable();
                    manifestCommittable.addFileCommittable(file);
                    break;
                case LOG_OFFSET:
                    LogOffsetCommittable offset =
                            (LogOffsetCommittable) committable.wrappedCommittable();
                    manifestCommittable.addLogOffset(offset.bucket(), offset.offset());
                    break;
            }
        }
        return manifestCommittable;
    }

    @Override
    public void commit(List<ManifestCommittable> committables)
            throws IOException, InterruptedException {
        commit.commitMultiple(committables, false);
        calcNumBytesAndRecordsOut(committables);
        partitionListeners.notifyCommittable(committables);
    }

    @Override
    public int filterAndCommit(
            List<ManifestCommittable> globalCommittables, boolean checkAppendFiles) {
        int committed = commit.filterAndCommitMultiple(globalCommittables, checkAppendFiles);
        partitionListeners.notifyCommittable(globalCommittables);
        return committed;
    }

    @Override
    public Map<Long, List<Committable>> groupByCheckpoint(Collection<Committable> committables) {
        try {
            partitionListeners.snapshotState();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Map<Long, List<Committable>> grouped = new HashMap<>();
        for (Committable c : committables) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }
        return grouped;
    }

    @Override
    public void close() throws Exception {
        commit.close();
        partitionListeners.close();
    }

    private void calcNumBytesAndRecordsOut(List<ManifestCommittable> committables) {
        if (committerMetrics == null) {
            return;
        }

        long bytesOut = 0;
        long recordsOut = 0;
        for (ManifestCommittable committable : committables) {
            List<CommitMessage> commitMessages = committable.fileCommittables();
            for (CommitMessage commitMessage : commitMessages) {
                long dataFileSizeInc =
                        calcTotalFileSize(
                                ((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
                long dataFileRowCountInc =
                        calcTotalFileRowCount(
                                ((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
                bytesOut += dataFileSizeInc;
                recordsOut += dataFileRowCountInc;
            }
        }
        committerMetrics.increaseNumBytesOut(bytesOut);
        committerMetrics.increaseNumRecordsOut(recordsOut);
    }

    private static long calcTotalFileSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).reduce(Long::sum).orElse(0);
    }

    private static long calcTotalFileRowCount(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::rowCount).reduce(Long::sum).orElse(0);
    }
}
