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

import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/** Prepares commit metadata while reusing the committer's conflict and publication lifecycle. */
public interface CommitChangesRewriter {

    CommitChangesRewriter IDENTITY = new CommitChangesRewriter() {};

    /** Creates one provider whose state is retained across optimistic retries of this compact. */
    default CommitChangesProvider compactChangesProvider(
            CommitChanges changes, List<CommitMessage> messages, boolean checkRowIdsFromSnapshot) {
        return CommitChangesProvider.provider(
                changes.tableFiles, changes.changelogFiles, changes.indexFiles);
    }

    /**
     * Creates isolated state for one attempt, after resolving its latest changes and before row-ID
     * assignment. The changes are read-only; this method must not create files.
     */
    default Attempt createAttempt(CommitKind kind, CommitChanges changes) {
        return Attempt.IDENTITY;
    }

    /** Index-only preparation after conflict checking and final row-ID assignment. */
    @FunctionalInterface
    interface Attempt {

        Attempt IDENTITY = (snapshotId, files, indexes, ioManager, registerNewFile) -> indexes;

        /**
         * Returns index changes for this candidate snapshot. Inputs are read-only. Register each
         * new file before opening it, so preparation and pre-publication callback failures can
         * clean it up. Files are retained once publication is attempted, even if its outcome is
         * uncertain.
         */
        List<IndexManifestEntry> rewriteIndexFiles(
                long newSnapshotId,
                List<ManifestEntry> assignedFiles,
                List<IndexManifestEntry> indexFiles,
                @Nullable IOManager ioManager,
                Consumer<Path> registerNewFile)
                throws IOException;
    }
}
