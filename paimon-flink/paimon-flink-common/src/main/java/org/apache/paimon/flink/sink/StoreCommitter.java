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

import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link Committer} for dynamic store. */
public class StoreCommitter implements Committer {

    private final TableCommitImpl commit;

    public StoreCommitter(TableCommit commit) {
        this.commit = (TableCommitImpl) commit;
    }

    @Override
    public List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> globalCommittables) {
        Set<Long> identifiers =
                commit.filterCommitted(
                        globalCommittables.stream()
                                .map(ManifestCommittable::identifier)
                                .collect(Collectors.toSet()));
        return globalCommittables.stream()
                .filter(m -> identifiers.contains(m.identifier()))
                .collect(Collectors.toList());
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
    public void commit(List<ManifestCommittable> committables)
            throws IOException, InterruptedException {
        commit.commitMultiple(committables);
    }

    @Override
    public void close() throws Exception {
        commit.close();
    }
}
