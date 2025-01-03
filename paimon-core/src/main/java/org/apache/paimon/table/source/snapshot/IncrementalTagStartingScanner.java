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
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.tag.TagTimeExtractor;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkState;

/** {@link StartingScanner} for incremental changes by tag. */
public class IncrementalTagStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalTagStartingScanner.class);

    @Nullable private final Snapshot start;
    private final Snapshot end;

    public IncrementalTagStartingScanner(
            SnapshotManager snapshotManager, String startTag, String endTag, CoreOptions options) {
        super(snapshotManager);
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.start = findStartSnapshot(tagManager, startTag, options);
        this.end = tagManager.getOrThrow(endTag).trimToSnapshot();

        LOG.info(
                "Start tag {} maps to snapshot {} and end tag {} maps to snapshots {}.",
                startTag,
                start == null ? null : start.id(),
                endTag,
                end.id());

        if (start != null && start.id() >= end.id()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                            endTag, end.id(), startTag, start.id()));
        }
    }

    @Nullable
    private Snapshot findStartSnapshot(TagManager tagManager, String start, CoreOptions options) {
        Optional<Tag> tag = tagManager.get(start);
        if (tag.isPresent()) {
            return tag.get().trimToSnapshot();
        }

        // check if the start is an auto-created tag
        TagTimeExtractor extractor = TagTimeExtractor.createForAutoTag(options);
        if (extractor == null) {
            throw new IllegalArgumentException("The start tag " + start + " does not exist.");
        }
        TagPeriodHandler periodHandler = TagPeriodHandler.create(options);
        if (!periodHandler.isAutoTag(start)) {
            throw new IllegalArgumentException("The start tag " + start + " does not exist.");
        }

        CoreOptions.IncrementalAutoTagStartMode mode = options.incrementalAutoTagStartMode();
        if (mode == CoreOptions.IncrementalAutoTagStartMode.STRICT) {
            throw new IllegalArgumentException(
                    String.format(
                            "The start tag %s doesn't exist. If you allow to start from the most earlier auto-created "
                                    + "tag, please set %s to %s or %s.",
                            start,
                            CoreOptions.INCREMENTAL_AUTO_TAG_START_MODE.key(),
                            CoreOptions.IncrementalAutoTagStartMode.EARLIER_STRICT,
                            CoreOptions.IncrementalAutoTagStartMode.EARLIER_OR_EMPTY));
        }

        Snapshot earliestSnapshot = snapshotManager.earliestSnapshot();
        checkState(earliestSnapshot != null, "No tags can be found.");

        LocalDateTime earliestTime =
                extractor
                        .extract(earliestSnapshot.timeMillis(), earliestSnapshot.watermark())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Cannot get valid tag time from earliest snapshot."));
        LocalDateTime earliestTagTime = periodHandler.normalizeToPreviousTag(earliestTime);

        LocalDateTime startTagTime = periodHandler.tagToTime(start);
        LocalDateTime previousTagTime = periodHandler.previousTagTime(startTagTime);

        Snapshot result = null;
        while (previousTagTime.isAfter(earliestTagTime)
                || previousTagTime.isEqual(earliestTagTime)) {
            String previousTagName = periodHandler.timeToTag(previousTagTime);
            Optional<Tag> previousTag = tagManager.get(previousTagName);
            if (previousTag.isPresent()) {
                result = previousTag.get().trimToSnapshot();
                break;
            } else {
                previousTagTime = periodHandler.previousTagTime(previousTagTime);
            }
        }

        if (result == null && mode == CoreOptions.IncrementalAutoTagStartMode.EARLIER_STRICT) {
            throw new IllegalArgumentException(
                    String.format(
                            "The start tag %s and its earlier tag don't exist. If you allow to return empty, please set "
                                    + "%s to %s.",
                            start,
                            CoreOptions.INCREMENTAL_AUTO_TAG_START_MODE.key(),
                            CoreOptions.IncrementalAutoTagStartMode.EARLIER_OR_EMPTY));
        }
        return result;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        if (start == null) {
            return StartingScanner.fromPlan(new PlanImpl(null, null, Collections.emptyList()));
        }
        return StartingScanner.fromPlan(reader.withSnapshot(end).readIncrementalDiff(start));
    }
}
