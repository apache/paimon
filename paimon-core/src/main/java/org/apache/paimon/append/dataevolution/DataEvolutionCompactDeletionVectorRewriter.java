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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;

import java.util.ArrayList;
import java.util.List;

/** Local execution of the same DV rewrite tasks used by distributed compaction. */
public class DataEvolutionCompactDeletionVectorRewriter {

    private final FileStoreTable table;
    private final Snapshot snapshot;

    public DataEvolutionCompactDeletionVectorRewriter(FileStoreTable table, Snapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
    }

    public List<CommitMessage> rewriteDeletionVectors(List<CommitMessage> messages) {
        List<CommitMessage> result = new ArrayList<>();
        try {
            for (DataEvolutionDeletionVectorRewriteTask task :
                    new DataEvolutionDeletionVectorRewritePlanner(table, snapshot).plan(messages)) {
                result.add(task.execute(table));
            }
        } catch (RuntimeException failure) {
            if (!result.isEmpty()) {
                try (TableCommit commit = table.newCommit("data-evolution-dv-rewrite-abort")) {
                    commit.abort(result);
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
        return result;
    }
}
