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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagTimeExtractor.ProcessTimeExtractor;
import org.apache.paimon.tag.TagTimeExtractor.WatermarkExtractor;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A manager to create tags automatically. */
public class TagAutoCreation {

    private static final Logger LOG = LoggerFactory.getLogger(TagAutoCreation.class);

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagTimeExtractor timeExtractor;
    private final TagPeriodHandler periodHandler;
    private final Duration delay;
    @Nullable private final Integer numRetainedMax;
    @Nullable private final Duration defaultTimeRetained;
    private final List<TagCallback> callbacks;
    private final Duration idlenessTimeout;
    private final boolean automaticCompletion;
    private final ZoneId sinkProcessTimeZone;

    // Maintenance publishes both positions together. Validation replays pending snapshots
    // on a private copy, even when maintenance is asynchronous.
    private volatile TagProgress progress;

    private static final class TagProgress {
        private long nextSnapshot;
        @Nullable private LocalDateTime nextTag;

        private TagProgress(long nextSnapshot, @Nullable LocalDateTime nextTag) {
            this.nextSnapshot = nextSnapshot;
            this.nextTag = nextTag;
        }

        private TagProgress copy() {
            return new TagProgress(nextSnapshot, nextTag);
        }
    }

    private TagAutoCreation(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagTimeExtractor timeExtractor,
            TagPeriodHandler periodHandler,
            Duration delay,
            @Nullable Integer numRetainedMax,
            @Nullable Duration defaultTimeRetained,
            Duration idlenessTimeout,
            boolean automaticCompletion,
            List<TagCallback> callbacks,
            ZoneId sinkProcessTimeZone) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.timeExtractor = timeExtractor;
        this.periodHandler = periodHandler;
        this.delay = delay;
        this.numRetainedMax = numRetainedMax;
        this.defaultTimeRetained = defaultTimeRetained;
        this.callbacks = callbacks;
        this.idlenessTimeout = idlenessTimeout;
        this.automaticCompletion = automaticCompletion;
        this.sinkProcessTimeZone = sinkProcessTimeZone;

        this.periodHandler.validateDelay(delay);

        // Auto-created tag times advance with snapshot IDs. Read only the latest tag to restore
        // progress instead of reading every tag file.
        List<String> tagNames = tagManager.tagNames(periodHandler::isAutoTag);
        tagNames.sort(Comparator.comparing(periodHandler::tagToTime, Comparator.reverseOrder()));

        if (tagNames.isEmpty()) {
            progress =
                    new TagProgress(
                            firstNonNull(snapshotManager.earliestSnapshotId(), FIRST_SNAPSHOT_ID),
                            null);
        } else {
            String tagName = tagNames.get(0);
            Tag tag = tagManager.getOrThrow(tagName);
            LocalDateTime time = periodHandler.tagToTime(tagName);
            progress =
                    new TagProgress(tag.trimToSnapshot().id() + 1, periodHandler.nextTagTime(time));
        }
    }

    public boolean forceCreatingSnapshot() {
        if (timeExtractor instanceof WatermarkExtractor && idlenessTimeout != null) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            if (latestSnapshot == null) {
                return false;
            }

            Long watermark = latestSnapshot.watermark();
            if (watermark == null) {
                return false;
            }

            LocalDateTime snapshotTime =
                    LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(watermark), ZoneId.systemDefault());

            return isAfterOrEqual(LocalDateTime.now().minus(idlenessTimeout), snapshotTime);
        } else if (timeExtractor instanceof ProcessTimeExtractor) {
            return forceCreatingSnapshotProcessTime(LocalDateTime.now(sinkProcessTimeZone));
        }
        return false;
    }

    /** Validate a newly supplied watermark before it can advance the snapshot watermark. */
    public static void validateWatermark(
            CoreOptions options,
            long watermark,
            SnapshotManager snapshotManager,
            @Nullable Snapshot latestSnapshot,
            @Nullable TagAutoCreation creation) {
        if (watermark == Long.MIN_VALUE
                || options.tagCreationMode() != CoreOptions.TagCreationMode.WATERMARK) {
            return;
        }
        ZoneId zone = ZoneId.of(options.sinkWatermarkTimeZone());
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(watermark), zone);
        TagPeriodHandler tagPeriodHandler = TagPeriodHandler.create(options);
        Duration delay = options.tagCreationDelay();
        try {
            tagPeriodHandler.timeToTag(tagPeriodHandler.normalizeToPreviousTag(time));
            if (latestSnapshot != null && !delay.isZero()) {
                tagPeriodHandler.timeToTag(
                        tagPeriodHandler.normalizeToPreviousTag(time.minus(delay)));
            }
        } catch (DateTimeException e) {
            DateTimeException cause = e;
            if (latestSnapshot != null) {
                if (creation == null) {
                    if (options.writeOnly()) {
                        // A dedicated creator can remember tags that have already expired.
                        return;
                    }
                    TagManager tagManager =
                            new TagManager(
                                    snapshotManager.fileIO(),
                                    snapshotManager.tablePath(),
                                    snapshotManager.branch());
                    creation =
                            create(options, snapshotManager, tagManager, Collections.emptyList());
                    checkState(creation != null, "Expected a tag creator for watermark mode.");
                }
                try {
                    LocalDateTime nextTag =
                            creation.processSnapshotsUpTo(latestSnapshot.id(), false);
                    LocalDateTime tag = creation.tagTime(time, nextTag);
                    if (tag != null) {
                        tagPeriodHandler.timeToTag(tag);
                    }
                    return;
                } catch (DateTimeException invalidTag) {
                    cause = invalidTag;
                }
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Watermark %s cannot name an automatic tag in time zone %s. "
                                    + "Supply epoch milliseconds that yield a four-digit tag year.",
                            watermark, zone),
                    cause);
        }
    }

    @VisibleForTesting
    boolean forceCreatingSnapshotProcessTime(LocalDateTime now) {
        LocalDateTime nextTag = progress.nextTag;
        return nextTag == null
                || isAfterOrEqual(now.minus(delay), periodHandler.nextTagTime(nextTag));
    }

    public void run() {
        processSnapshotsUpTo(Long.MAX_VALUE, true);
    }

    @Nullable
    private LocalDateTime processSnapshotsUpTo(long lastSnapshotId, boolean createTags) {
        TagProgress cursor = progress.copy();
        while (cursor.nextSnapshot <= lastSnapshotId) {
            if (snapshotManager.snapshotExists(cursor.nextSnapshot)) {
                Snapshot snapshot = snapshotManager.snapshot(cursor.nextSnapshot);
                if (createTags) {
                    try {
                        tryToCreateTags(snapshot, cursor);
                        cursor.nextSnapshot++;
                    } finally {
                        // Tag creation may have succeeded even if retention failed afterwards.
                        progress = cursor.copy();
                    }
                } else {
                    replayTagProgress(snapshot, cursor);
                    cursor.nextSnapshot++;
                }
            } else {
                Long earliest = snapshotManager.earliestSnapshotId();
                if (earliest == null || earliest <= cursor.nextSnapshot) {
                    break;
                }
                cursor.nextSnapshot = earliest;
            }
        }
        return cursor.nextTag;
    }

    private void replayTagProgress(Snapshot snapshot, TagProgress cursor) {
        timeExtractor
                .extract(snapshot.timeMillis(), snapshot.watermark())
                .map(time -> tagTime(time, cursor.nextTag))
                .ifPresent(
                        tag -> {
                            // Creation stops at this snapshot if the tag cannot be named.
                            periodHandler.timeToTag(tag);
                            cursor.nextTag = periodHandler.nextTagTime(tag);
                        });
    }

    private void tryToCreateTags(Snapshot snapshot, TagProgress cursor) {
        Optional<LocalDateTime> timeOptional =
                timeExtractor.extract(snapshot.timeMillis(), snapshot.watermark());
        LOG.info("Starting to create a tag for snapshot {}.", snapshot.id());
        if (!timeOptional.isPresent()) {
            return;
        }

        LocalDateTime time = timeOptional.get();
        LOG.info("The time of snapshot {} is {}.", snapshot.id(), time);
        LOG.info("The next tag time is {}.", cursor.nextTag);
        LocalDateTime thisTag = tagTime(time, cursor.nextTag);
        if (thisTag != null) {
            LOG.info("Create tag for snapshot {} with time {}.", snapshot.id(), thisTag);
            String tagName = periodHandler.timeToTag(thisTag);
            LOG.info("The tag name is {}.", tagName);
            // shouldn't throw exception when tag exists
            tagManager.createTag(snapshot, tagName, defaultTimeRetained, callbacks, true);
            cursor.nextTag = periodHandler.nextTagTime(thisTag);
            if (timeExtractor instanceof ProcessTimeExtractor) {
                // Publish the next tag time before retention so empty checkpoints
                // do not force another snapshot for the same period.
                progress = cursor.copy();
            }
            LOG.info("The next tag time after this is {}.", cursor.nextTag);

            if (numRetainedMax != null) {
                // only handle auto-created tags here
                SortedMap<Snapshot, List<String>> tags = tagManager.tags(periodHandler::isAutoTag);
                if (tags.size() > numRetainedMax) {
                    int toDelete = tags.size() - numRetainedMax;
                    int i = 0;
                    for (List<String> tag : tags.values()) {
                        String toDeleteTag = checkAndGetOneAutoTag(tag);
                        LOG.info(
                                "Delete tag {}, because the number of auto-created tags reached numRetainedMax of {}.",
                                toDeleteTag,
                                numRetainedMax);
                        tagManager.deleteTag(toDeleteTag, callbacks);
                        i++;
                        if (i == toDelete) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Nullable
    private LocalDateTime tagTime(LocalDateTime time, @Nullable LocalDateTime nextTag) {
        if (nextTag == null) {
            return periodHandler.normalizeToPreviousTag(time);
        }
        LocalDateTime delayedTime = time.minus(delay);
        if (delayedTime.isBefore(periodHandler.nextTagTime(nextTag))) {
            return null;
        }
        return automaticCompletion ? nextTag : periodHandler.normalizeToPreviousTag(delayedTime);
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
                options.tagNumRetainedMax(),
                options.tagDefaultTimeRetained(),
                options.snapshotWatermarkIdleTimeout(),
                options.tagAutomaticCompletion(),
                callbacks,
                options.sinkProcessTimeZone());
    }
}
