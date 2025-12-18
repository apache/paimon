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

package org.apache.paimon.flink.cluster;

import org.apache.paimon.append.cluster.IncrementalClusterManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Rewrite committable for new files written after clustered. */
public class RewriteIncrementalClusterCommittableOperator
        extends BoundedOneInputOperator<Committable, Committable> {

    protected static final Logger LOG =
            LoggerFactory.getLogger(RewriteIncrementalClusterCommittableOperator.class);
    private static final long serialVersionUID = 2L;

    private final FileStoreTable table;
    private final Map<Pair<BinaryRow, Integer>, Integer> outputLevels;

    private transient Map<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionFiles;

    public RewriteIncrementalClusterCommittableOperator(
            FileStoreTable table, Map<Pair<BinaryRow, Integer>, Integer> outputLevels) {
        this.table = table;
        this.outputLevels = outputLevels;
    }

    @Override
    public void open() throws Exception {
        partitionFiles = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        Committable committable = element.getValue();
        if (committable.kind() != Committable.Kind.FILE) {
            output.collect(element);
        }

        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        BinaryRow partition = message.partition();
        int bucket = message.bucket();

        Map<Integer, List<DataFileMeta>> bucketFiles = partitionFiles.get(partition);
        if (bucketFiles == null) {
            bucketFiles = new HashMap<>();
            partitionFiles.put(partition.copy(), bucketFiles);
        }
        bucketFiles
                .computeIfAbsent(bucket, file -> new ArrayList<>())
                .addAll(message.newFilesIncrement().newFiles());
        partitionFiles.put(partition, bucketFiles);
    }

    @Override
    public void endInput() throws Exception {
        emitAll(Long.MAX_VALUE);
    }

    protected void emitAll(long checkpointId) {
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionEntry :
                partitionFiles.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            Map<Integer, List<DataFileMeta>> bucketFiles = partitionEntry.getValue();

            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : bucketFiles.entrySet()) {
                int bucket = bucketEntry.getKey();
                // upgrade the clustered file to outputLevel
                List<DataFileMeta> clusterAfter =
                        IncrementalClusterManager.upgrade(
                                bucketEntry.getValue(),
                                outputLevels.get(Pair.of(partition, bucket)));
                LOG.info(
                        "Partition {}, bucket {}: upgrade file level to {}",
                        partition,
                        bucket,
                        outputLevels.get(Pair.of(partition, bucket)));
                CompactIncrement compactIncrement =
                        new CompactIncrement(
                                Collections.emptyList(), clusterAfter, Collections.emptyList());
                CommitMessageImpl clusterMessage =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                table.coreOptions().bucket(),
                                DataIncrement.emptyIncrement(),
                                compactIncrement);
                output.collect(
                        new StreamRecord<>(
                                new Committable(
                                        checkpointId, Committable.Kind.FILE, clusterMessage)));
            }
        }

        partitionFiles.clear();
    }
}
