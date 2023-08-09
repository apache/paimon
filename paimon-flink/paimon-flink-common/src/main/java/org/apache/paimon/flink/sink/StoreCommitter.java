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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableCommitImpl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link Committer} for dynamic store. */
public class StoreCommitter implements Committer<Committable, ManifestCommittable> {

    private final TableCommitImpl commit;

    public StoreCommitter(TableCommit commit) {
        this.commit = (TableCommitImpl) commit;
    }

    @Override
    public ManifestCommittable combine(
            long checkpointId, long watermark, List<Committable> committables) {
        ManifestCommittable manifestCommittable = new ManifestCommittable(checkpointId, watermark);
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
    public void commit(List<ManifestCommittable> committables, OperatorIOMetricGroup metricGroup)
            throws IOException, InterruptedException {
        commit.commitMultiple(committables);
        Tuple2<Long, Long> numBytesAndRecords = calcDataBytesAndRecordsSend(committables);
        metricGroup.getNumBytesOutCounter().inc(numBytesAndRecords.f0);
        metricGroup.getNumRecordsOutCounter().inc(numBytesAndRecords.f1);
    }

    @Override
    public int filterAndCommit(List<ManifestCommittable> globalCommittables) {
        return commit.filterAndCommitMultiple(globalCommittables);
    }

    @Override
    public Map<Long, List<Committable>> groupByCheckpoint(Collection<Committable> committables) {
        Map<Long, List<Committable>> grouped = new HashMap<>();
        for (Committable c : committables) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }
        return grouped;
    }

    @Override
    public void close() throws Exception {
        commit.close();
    }

    @VisibleForTesting
    static Tuple2<Long, Long> calcDataBytesAndRecordsSend(List<ManifestCommittable> committables) {
        long bytesSend = 0;
        long recordsSend = 0;
        for (ManifestCommittable committable : committables) {
            List<CommitMessage> commitMessages = committable.fileCommittables();
            for (CommitMessage commitMessage : commitMessages) {
                long dataFileSizeInc =
                        calcTotalFileSize(
                                ((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
                long dataFileRowCountInc =
                        calcTotalFileRowCount(
                                ((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
                bytesSend += dataFileSizeInc;
                recordsSend += dataFileRowCountInc;
            }
        }
        return Tuple2.of(bytesSend, recordsSend);
    }

    private static long calcTotalFileSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(f -> f.fileSize()).reduce(Long::sum).orElse(0);
    }

    private static long calcTotalFileRowCount(List<DataFileMeta> files) {
        return files.stream().mapToLong(f -> f.rowCount()).reduce(Long::sum).orElse(0);
    }
}
