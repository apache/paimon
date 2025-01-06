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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.tag.TagTimeExtractor;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** {@link StartingScanner} for incremental changes by tag. */
public class IncrementalTagStartingScanner extends AbstractStartingScanner {

    private final Snapshot start;
    private final Snapshot end;

    public IncrementalTagStartingScanner(
            SnapshotManager snapshotManager, Snapshot start, Snapshot end) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        this.startingSnapshotId = start.id();
    }

    @Override
    public Result scan(SnapshotReader reader) {
        return StartingScanner.fromPlan(reader.withSnapshot(end).readIncrementalDiff(start));
    }

    public static IncrementalTagStartingScanner create(
            SnapshotManager snapshotManager, String startTagName, String endTagName) {
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        Snapshot start = tagManager.getOrThrow(startTagName).trimToSnapshot();
        Snapshot end = tagManager.getOrThrow(endTagName).trimToSnapshot();
        if (end.id() <= start.id()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                            endTagName, end.id(), startTagName, start.id()));
        }
        return new IncrementalTagStartingScanner(snapshotManager, start, end);
    }

    public static AbstractStartingScanner create(
            SnapshotManager snapshotManager, String endTagName, CoreOptions options) {
        TagTimeExtractor extractor = TagTimeExtractor.createForAutoTag(options);
        checkNotNull(
                extractor,
                "Table's tag creation mode doesn't support '%s' scan mode.",
                CoreOptions.INCREMENTAL_TO_AUTO_TAG);
        TagPeriodHandler periodHandler = TagPeriodHandler.create(options);
        checkArgument(
                periodHandler.isAutoTag(endTagName),
                "Specified tag '%s' is not an auto-created tag.",
                endTagName);

        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());

        Optional<Tag> endTag = tagManager.get(endTagName);
        if (!endTag.isPresent()) {
            return new EmptyResultStartingScanner(snapshotManager);
        }

        Snapshot end = endTag.get().trimToSnapshot();

        Snapshot earliestSnapshot = snapshotManager.earliestSnapshot();
        checkState(earliestSnapshot != null, "No tags can be found.");

        LocalDateTime earliestTime =
                extractor
                        .extract(earliestSnapshot.timeMillis(), earliestSnapshot.watermark())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Cannot get valid tag time from the earliest snapshot."));
        LocalDateTime earliestTagTime = periodHandler.normalizeToPreviousTag(earliestTime);

        LocalDateTime endTagTime = periodHandler.tagToTime(endTagName);
        LocalDateTime previousTagTime = periodHandler.previousTagTime(endTagTime);

        Snapshot start = null;
        while (previousTagTime.isAfter(earliestTagTime)
                || previousTagTime.isEqual(earliestTagTime)) {
            String previousTagName = periodHandler.timeToTag(previousTagTime);
            Optional<Tag> previousTag = tagManager.get(previousTagName);
            if (previousTag.isPresent()) {
                start = previousTag.get().trimToSnapshot();
                break;
            } else {
                previousTagTime = periodHandler.previousTagTime(previousTagTime);
            }
        }

        if (start == null) {
            return new EmptyResultStartingScanner(snapshotManager);
        }

        return new IncrementalTagStartingScanner(snapshotManager, start, end);
    }
}
