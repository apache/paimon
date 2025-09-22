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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;

import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class ListenerTestUtils {

    static Committer.Context createMockContext(
            boolean streamingCheckpointEnabled, boolean isRestored) {
        return Committer.createContext(
                UUID.randomUUID().toString(),
                null,
                streamingCheckpointEnabled,
                isRestored,
                new MockOperatorStateStore(),
                1,
                1);
    }

    static void notifyCommits(PartitionMarkDoneListener markDone, boolean isCompact) {
        notifyCommits(markDone, isCompact, true);
    }

    static void notifyCommits(
            PartitionMarkDoneListener markDone,
            boolean isCompact,
            boolean partitionMarkDoneRecoverFromState) {
        ManifestCommittable committable = new ManifestCommittable(Long.MAX_VALUE);
        DataFileMeta file = DataFileTestUtils.newFile();
        CommitMessageImpl compactMessage;
        if (isCompact) {
            compactMessage =
                    new CommitMessageImpl(
                            BinaryRow.singleColumn(0),
                            0,
                            1,
                            new DataIncrement(emptyList(), emptyList(), emptyList()),
                            new CompactIncrement(singletonList(file), emptyList(), emptyList()));
        } else {
            compactMessage =
                    new CommitMessageImpl(
                            BinaryRow.singleColumn(0),
                            0,
                            1,
                            new DataIncrement(singletonList(file), emptyList(), emptyList()),
                            new CompactIncrement(emptyList(), emptyList(), emptyList()));
        }
        committable.addFileCommittable(compactMessage);
        if (partitionMarkDoneRecoverFromState) {
            markDone.notifyCommittable(singletonList(committable));
        }
    }
}
