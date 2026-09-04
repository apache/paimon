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

package org.apache.paimon.operation;

import org.apache.paimon.manifest.ManifestCommittable;

import java.util.Optional;

/** Internal commit lifecycle used by a catalog which atomically publishes multiple tables. */
public interface TransactionalFileStoreCommit {

    /**
     * Prepare the APPEND or OVERWRITE snapshot for a committable without making it visible. COMPACT
     * changes are intentionally excluded.
     */
    Optional<PreparedSnapshotCommit> prepareCommit(
            ManifestCommittable committable, boolean checkAppendFiles);

    /** Finish local state and callbacks after the catalog has published the snapshot. */
    void completeCommit(PreparedSnapshotCommit preparedCommit);

    /**
     * Mark the publish result as unknown, for example after a catalog request times out. Call
     * {@link #completeCommit} after reconciling that it was published.
     */
    void markCommitUnknown(PreparedSnapshotCommit preparedCommit);

    /** Best-effort commit of the excluded COMPACT changes after the transaction. */
    void commitCompaction(ManifestCommittable committable);
}
