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

package org.apache.paimon.tag;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagTimeExtractor.ProcessTimeExtractor;
import org.apache.paimon.tag.TagTimeExtractor.WatermarkExtractor;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A manager to create tags automatically. */
public class TagAutoCreation {

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagTimeExtractor timeExtractor;
    private final TagPeriodHandler periodHandler;
    private final Duration delay;
    private final Duration timeRetained;
    private final List<TagCallback> callbacks;
    private final Duration idlenessTimeout;

    private LocalDateTime nextTag;
    private long nextSnapshot;

    private TagAutoCreation(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagTimeExtractor timeExtractor,
            TagPeriodHandler periodHandler,
            Duration delay,
            @Nullable Duration timeRetained,
            Duration idlenessTimeout,
            List<TagCallback> callbacks) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.timeExtractor = timeExtractor;
        this.periodHandler = periodHandler;
        this.delay = delay;
        this.timeRetained = timeRetained;
        this.callbacks = callbacks;
        this.idlenessTimeout = idlenessTimeout;

        this.periodHandler.validateDelay(delay);

        SortedMap<Snapshot, List<String>> tags = tagManager.tags(periodHandler::isAutoTag);

        if (tags.isEmpty()) {
            this.nextSnapshot =
                    firstNonNull(snapshotManager.earliestSnapshotId(), FIRST_SNAPSHOT_ID);
        } else {
            Snapshot lastTag = tags.lastKey();
            this.nextSnapshot = lastTag.id() + 1;

            String tagName = checkAndGetOneAutoTag(tags.get(lastTag));
            LocalDateTime time = periodHandler.tagToTime(tagName);
            this.nextTag = periodHandler.nextTagTime(time);
        }
    }

    public boolean forceCreatingSnapshot() {
        if (timeExtractor instanceof WatermarkExtractor && idlenessTimeout != null) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            if (latestSnapshot == null) {
                return false;
            }

            LocalDateTime snapshotTime =
                    LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(latestSnapshot.watermark()),
                            ZoneId.systemDefault());

            return isAfterOrEqual(LocalDateTime.now().minus(idlenessTimeout), snapshotTime);
        } else if (timeExtractor instanceof ProcessTimeExtractor) {
            return nextTag == null
                    || isAfterOrEqual(
                            LocalDateTime.now().minus(delay), periodHandler.nextTagTime(nextTag));
        }
        return false;
    }

    public void run() {
        while (true) {
            if (snapshotManager.snapshotExists(nextSnapshot)) {
                tryToCreateTags(snapshotManager.snapshot(nextSnapshot));
                nextSnapshot++;
            } else {
                // avoid snapshot has been expired
                Long earliest = snapshotManager.earliestSnapshotId();
                if (earliest != null && earliest > nextSnapshot) {
                    nextSnapshot = earliest;
                } else {
                    break;
                }
            }
        }
    }

    private void tryToCreateTags(Snapshot snapshot) {
        Optional<LocalDateTime> timeOptional =
                timeExtractor.extract(snapshot.timeMillis(), snapshot.watermark());
        if (!timeOptional.isPresent()) {
            return;
        }

        LocalDateTime time = timeOptional.get();
        if (nextTag == null
                || isAfterOrEqual(time.minus(delay), periodHandler.nextTagTime(nextTag))) {
            LocalDateTime thisTag = periodHandler.normalizeToPreviousTag(time);
            String tagName = periodHandler.timeToTag(thisTag);
            tagManager.createTag(snapshot, tagName, timeRetained, callbacks);
            nextTag = periodHandler.nextTagTime(thisTag);
        }
    }

    private boolean isAfterOrEqual(LocalDateTime t1, LocalDateTime t2) {
        return t1.isAfter(t2) || t1.isEqual(t2);
    }

    public static String checkAndGetOneAutoTag(List<String> autoTags) {
        checkState(
                autoTags.size() == 1,
                "There are more than 1 auto-created tags of the same snapshot: %s. This is unexpected.",
                String.join(",", autoTags));

        return autoTags.get(0);
    }

    @Nullable
    public static TagAutoCreation create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            List<TagCallback> callbacks) {
        TagTimeExtractor extractor = TagTimeExtractor.createForAutoTag(options);
        if (extractor == null) {
            return null;
        }
        return new TagAutoCreation(
                snapshotManager,
                tagManager,
                extractor,
                TagPeriodHandler.create(options),
                options.tagCreationDelay(),
                options.tagDefaultTimeRetained(),
                options.snapshotWatermarkIdleTimeout(),
                callbacks);
    }
}
