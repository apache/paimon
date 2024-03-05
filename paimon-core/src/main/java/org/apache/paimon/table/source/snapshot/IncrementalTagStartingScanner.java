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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

/** {@link StartingScanner} for incremental changes by tag. */
public class IncrementalTagStartingScanner extends AbstractStartingScanner {

    private final String start;
    private final String end;

    public IncrementalTagStartingScanner(
            SnapshotManager snapshotManager, String start, String end) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        Snapshot startingSnapshot = tagManager.taggedSnapshot(start);
        if (startingSnapshot != null) {
            this.startingSnapshotId = startingSnapshot.id();
        }
    }

    @Override
    public Result scan(SnapshotReader reader) {
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        Snapshot tag1 = tagManager.taggedSnapshot(branch, start);
        Snapshot tag2 = tagManager.taggedSnapshot(branch, end);

        if (tag2.id() <= tag1.id()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                            end, tag2.id(), start, tag1.id()));
        }

        return StartingScanner.fromPlan(reader.withSnapshot(tag2).readIncrementalDiff(tag1));
    }
}
