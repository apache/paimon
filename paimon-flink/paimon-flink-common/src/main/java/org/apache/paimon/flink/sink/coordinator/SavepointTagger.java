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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.sink.SavepointTagUtils;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Owns savepoint auto-tagging for {@link CommittingWriteOperatorCoordinator}, replicating the
 * semantics of the classic {@link
 * org.apache.paimon.flink.sink.AutoTagForSavepointCommitterOperator} for the coordinator-commit
 * path. It keeps the set of savepoint checkpoint ids still awaiting a snapshot to tag; this set is
 * deliberately not checkpointed but rebuilt from the savepoint ids replayed with each subtask's
 * committables, so the coordinator's persisted state stays minimal.
 */
public class SavepointTagger {

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagDeletion tagDeletion;
    private final List<TagCallback> callbacks;
    private final Duration tagTimeRetained;
    // findSnapshotsForIdentifiers filters by commit user, so the tagger must be bound to the user
    // the coordinator actually commits with (which the coordinator restores from its state).
    private final String commitUser;
    // Checkpoint ids of pending Flink savepoints awaiting a snapshot to tag.
    private final NavigableSet<Long> pendingIdentifiers = new TreeSet<>();

    public SavepointTagger(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks,
            Duration tagTimeRetained,
            String commitUser) {
        this.snapshotManager = checkNotNull(snapshotManager);
        this.tagManager = checkNotNull(tagManager);
        this.tagDeletion = checkNotNull(tagDeletion);
        this.callbacks = checkNotNull(callbacks);
        this.tagTimeRetained = tagTimeRetained;
        this.commitUser = checkNotNull(commitUser);
    }

    public void add(long savepointIdentifier) {
        pendingIdentifiers.add(savepointIdentifier);
    }

    /**
     * Tags every pending savepoint whose snapshot the commit up to {@code checkpointId} has
     * materialized, then drops those pending intents.
     */
    public void tagUpTo(long checkpointId) {
        NavigableSet<Long> headSet = pendingIdentifiers.headSet(checkpointId, true);
        if (!headSet.isEmpty()) {
            createTags(new ArrayList<>(headSet));
            headSet.clear();
        }
    }

    /** Drops an aborted savepoint's pending intent and removes any tag already created for it. */
    public void dropAborted(long checkpointId) {
        pendingIdentifiers.remove(checkpointId);
        SavepointTagUtils.deleteTagIfMatches(
                tagManager, commitUser, checkpointId, tagDeletion, snapshotManager, callbacks);
    }

    private void createTags(Collection<Long> identifiers) {
        List<Snapshot> snapshots =
                snapshotManager.findSnapshotsForIdentifiers(
                        commitUser, new ArrayList<>(identifiers));
        for (Snapshot snapshot : snapshots) {
            String tagName = SavepointTagUtils.tagNameOf(snapshot.commitIdentifier());
            // ignoreIfExists: a later checkpoint's completion may re-tag an already-tagged
            // snapshot.
            tagManager.createTag(snapshot, tagName, tagTimeRetained, callbacks, true);
        }
    }

    /** Builds a {@link SavepointTagger} bound to the coordinator's restored commit user. */
    public interface Factory extends Serializable {
        SavepointTagger create(String commitUser);
    }
}
