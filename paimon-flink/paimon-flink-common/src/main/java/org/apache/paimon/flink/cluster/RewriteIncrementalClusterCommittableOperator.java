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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Rewrite committable for new files written after clustered. */
public class RewriteIncrementalClusterCommittableOperator
        extends BoundedOneInputOperator<Committable, Committable> {

    protected static final Logger LOG =
            LoggerFactory.getLogger(RewriteIncrementalClusterCommittableOperator.class);
    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final Map<BinaryRow, Integer> outputLevels;

    private transient Map<BinaryRow, List<DataFileMeta>> partitionFiles;

    public RewriteIncrementalClusterCommittableOperator(
            FileStoreTable table, Map<BinaryRow, Integer> outputLevels) {
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
        CommitMessageImpl message = (CommitMessageImpl) committable.commitMessage();
        checkArgument(message.bucket() == 0);
        BinaryRow partition = message.partition();
        partitionFiles
                .computeIfAbsent(partition, file -> new ArrayList<>())
                .addAll(message.newFilesIncrement().newFiles());
    }

    @Override
    public void endInput() throws Exception {
        emitAll(Long.MAX_VALUE);
    }

    protected void emitAll(long checkpointId) {
        for (Map.Entry<BinaryRow, List<DataFileMeta>> partitionEntry : partitionFiles.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            // upgrade the clustered file to outputLevel
            List<DataFileMeta> clusterAfter =
                    IncrementalClusterManager.upgrade(
                            partitionEntry.getValue(), outputLevels.get(partition));
            LOG.info(
                    "Partition {}: upgrade file level to {}",
                    partition,
                    outputLevels.get(partition));
            CompactIncrement compactIncrement =
                    new CompactIncrement(
                            Collections.emptyList(), clusterAfter, Collections.emptyList());
            CommitMessageImpl clusterMessage =
                    new CommitMessageImpl(
                            partition,
                            // bucket 0 is bucket for unaware-bucket table
                            // for compatibility with the old design
                            0,
                            table.coreOptions().bucket(),
                            DataIncrement.emptyIncrement(),
                            compactIncrement);
            output.collect(new StreamRecord<>(new Committable(checkpointId, clusterMessage)));
        }

        partitionFiles.clear();
    }
}
