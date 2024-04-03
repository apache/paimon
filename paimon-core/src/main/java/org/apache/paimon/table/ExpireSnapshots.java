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

package org.apache.paimon.table;

import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

/** Expire snapshots. */
public interface ExpireSnapshots {

    /** @return How many snapshots have been expired. */
    int expire();

    /** Expire the snapshots and changelogs. */
    class Expire implements ExpireSnapshots {

        private @Nullable ExpireChangelogImpl expireChangelogs;
        private final ExpireSnapshotsImpl expireSnapshots;

        public Expire(
                SnapshotManager snapshotManager,
                SnapshotDeletion snapshotDeletion,
                ChangelogDeletion changelogDeletion,
                TagManager tagManager,
                boolean cleanEmptyDirectories,
                int snapshotRetainMax,
                int snapshotRetainMin,
                long snapshotTimeToRetain,
                int changelogRetainMax,
                int changelogRetainMin,
                long changelogTimeToRetain,
                int maxDeletes) {
            boolean changelogDecoupled =
                    changelogRetainMax > snapshotRetainMax
                            || changelogRetainMin > snapshotRetainMin
                            || changelogTimeToRetain > snapshotTimeToRetain;
            this.expireSnapshots =
                    new ExpireSnapshotsImpl(
                            snapshotManager,
                            snapshotDeletion,
                            tagManager,
                            cleanEmptyDirectories,
                            changelogDecoupled,
                            snapshotRetainMax,
                            snapshotRetainMin,
                            snapshotTimeToRetain,
                            maxDeletes);
            if (changelogDecoupled) {
                this.expireChangelogs =
                        new ExpireChangelogImpl(
                                snapshotManager,
                                tagManager,
                                changelogDeletion,
                                cleanEmptyDirectories,
                                changelogRetainMax,
                                changelogRetainMin,
                                changelogTimeToRetain,
                                maxDeletes);
            }
        }

        public ExpireSnapshotsImpl getExpireSnapshots() {
            return expireSnapshots;
        }

        @Override
        public int expire() {
            int snapshot = expireSnapshots.expire();
            if (expireChangelogs != null) {
                snapshot += expireChangelogs.expire();
            }

            return snapshot;
        }
    }
}
