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

package org.apache.paimon.operation.commit;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

/** Reuses ordinary compaction outputs across verified metadata-only row-ID reassignment. */
public final class ReassignCompactChangesRewriter implements CommitChangesRewriter {

    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final long lastSafeSnapshot;

    public ReassignCompactChangesRewriter(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            long lastSafeSnapshot) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.lastSafeSnapshot = lastSafeSnapshot;
    }

    @Override
    public CommitChangesProvider compactChangesProvider(
            CommitChanges changes, List<CommitMessage> messages, boolean checkRowIdsFromSnapshot) {
        if (!checkRowIdsFromSnapshot) {
            return new ReassignCompactChangesProvider(
                    fileIO, pathFactory, snapshotManager, lastSafeSnapshot, changes);
        }
        return CommitChangesRewriter.super.compactChangesProvider(
                changes, messages, checkRowIdsFromSnapshot);
    }
}
