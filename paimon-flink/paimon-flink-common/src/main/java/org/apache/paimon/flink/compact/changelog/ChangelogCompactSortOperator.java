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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

/** Operator to sort changelog files from each bucket by their creation time. */
public class ChangelogCompactSortOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {

    private transient Map<BinaryRow, Map<Integer, List<DataFileMeta>>> newFileChangelogFiles;
    private transient Map<BinaryRow, Map<Integer, List<DataFileMeta>>> compactChangelogFiles;
    private transient Map<BinaryRow, Integer> numBuckets;

    @Override
    public void open() {
        newFileChangelogFiles = new LinkedHashMap<>();
        compactChangelogFiles = new LinkedHashMap<>();
        numBuckets = new LinkedHashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> record) throws Exception {
        Committable committable = record.getValue();
        CommitMessageImpl message = (CommitMessageImpl) committable.commitMessage();
        if (message.newFilesIncrement().changelogFiles().isEmpty()
                && message.compactIncrement().changelogFiles().isEmpty()) {
            output.collect(record);
            return;
        }

        numBuckets.put(message.partition(), message.totalBuckets());

        BiConsumer<DataFileMeta, Map<BinaryRow, Map<Integer, List<DataFileMeta>>>> addChangelog =
                (meta, changelogFiles) ->
                        changelogFiles
                                .computeIfAbsent(message.partition(), p -> new TreeMap<>())
                                .computeIfAbsent(message.bucket(), b -> new ArrayList<>())
                                .add(meta);
        for (DataFileMeta meta : message.newFilesIncrement().changelogFiles()) {
            addChangelog.accept(meta, newFileChangelogFiles);
        }
        for (DataFileMeta meta : message.compactIncrement().changelogFiles()) {
            addChangelog.accept(meta, compactChangelogFiles);
        }

        CommitMessageImpl newMessage =
                new CommitMessageImpl(
                        message.partition(),
                        message.bucket(),
                        message.totalBuckets(),
                        new DataIncrement(
                                message.newFilesIncrement().newFiles(),
                                message.newFilesIncrement().deletedFiles(),
                                Collections.emptyList(),
                                message.newFilesIncrement().newIndexFiles(),
                                message.newFilesIncrement().deletedIndexFiles()),
                        new CompactIncrement(
                                message.compactIncrement().compactBefore(),
                                message.compactIncrement().compactAfter(),
                                Collections.emptyList(),
                                message.compactIncrement().newIndexFiles(),
                                message.compactIncrement().deletedIndexFiles()));
        if (!newMessage.isEmpty()) {
            Committable newCommittable = new Committable(committable.checkpointId(), newMessage);
            output.collect(new StreamRecord<>(newCommittable));
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        emitAll(checkpointId);
    }

    @Override
    public void endInput() {
        emitAll(Long.MAX_VALUE);
    }

    private void emitAll(long checkpointId) {
        Map<BinaryRow, Set<Integer>> activeBuckets = new LinkedHashMap<>();
        collectActiveBuckets(newFileChangelogFiles, activeBuckets);
        collectActiveBuckets(compactChangelogFiles, activeBuckets);

        for (Map.Entry<BinaryRow, Set<Integer>> entry : activeBuckets.entrySet()) {
            BinaryRow partition = entry.getKey();
            for (int bucket : entry.getValue()) {
                CommitMessageImpl newMessage =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                numBuckets.get(partition),
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        sortedChangelogs(newFileChangelogFiles, partition, bucket)),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        sortedChangelogs(
                                                compactChangelogFiles, partition, bucket)));
                Committable newCommittable = new Committable(checkpointId, newMessage);
                output.collect(new StreamRecord<>(newCommittable));
            }
        }

        newFileChangelogFiles.clear();
        compactChangelogFiles.clear();
        numBuckets.clear();
    }

    private void collectActiveBuckets(
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> from,
            Map<BinaryRow, Set<Integer>> activeBuckets) {
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry : from.entrySet()) {
            activeBuckets
                    .computeIfAbsent(entry.getKey(), k -> new TreeSet<>())
                    .addAll(entry.getValue().keySet());
        }
    }

    private List<DataFileMeta> sortedChangelogs(
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> from,
            BinaryRow partition,
            int bucket) {
        List<DataFileMeta> result = new ArrayList<>();
        if (from.containsKey(partition) && from.get(partition).containsKey(bucket)) {
            result.addAll(from.get(partition).get(bucket));
        }
        result.sort(Comparator.comparingLong(DataFileMeta::creationTimeEpochMillis));
        return result;
    }
}
