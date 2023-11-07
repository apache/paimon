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
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;

/** A manager to create tags automatically. */
public class TagAutoCreation {

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagDeletion tagDeletion;
    private final TagTimeExtractor timeExtractor;
    private final TagPeriodHandler periodHandler;
    private final Duration delay;
    private final Integer numRetainedMax;

    private LocalDateTime nextTag;
    private long nextSnapshot;

    private TagAutoCreation(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            TagTimeExtractor timeExtractor,
            TagPeriodHandler periodHandler,
            Duration delay,
            Integer numRetainedMax) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.tagDeletion = tagDeletion;
        this.timeExtractor = timeExtractor;
        this.periodHandler = periodHandler;
        this.delay = delay;
        this.numRetainedMax = numRetainedMax;

        this.periodHandler.validateDelay(delay);

        SortedMap<Snapshot, String> tags = tagManager.tags();

        if (tags.isEmpty()) {
            this.nextSnapshot =
                    firstNonNull(snapshotManager.earliestSnapshotId(), FIRST_SNAPSHOT_ID);
        } else {
            Snapshot lastTag = tags.lastKey();
            this.nextSnapshot = lastTag.id() + 1;

            LocalDateTime time = periodHandler.tagToTime(tags.get(lastTag));
            this.nextTag = periodHandler.nextTagTime(time);
        }
    }

    public boolean forceCreatingSnapshot() {
        return timeExtractor.forceCreatingSnapshot();
    }

    public void run() {
        while (true) {
            if (snapshotManager.snapshotExists(nextSnapshot)) {
                tryToTag(snapshotManager.snapshot(nextSnapshot));
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

    private void tryToTag(Snapshot snapshot) {
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
            tagManager.createTag(snapshot, tagName);
            nextTag = periodHandler.nextTagTime(thisTag);

            if (numRetainedMax != null) {
                SortedMap<Snapshot, String> tags = tagManager.tags();
                if (tags.size() > numRetainedMax) {
                    int toDelete = tags.size() - numRetainedMax;
                    int i = 0;
                    for (String tag : tags.values()) {
                        tagManager.deleteTag(tag, tagDeletion, snapshotManager);
                        i++;
                        if (i == toDelete) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private boolean isAfterOrEqual(LocalDateTime t1, LocalDateTime t2) {
        return t1.isAfter(t2) || t1.isEqual(t2);
    }

    @Nullable
    public static TagAutoCreation create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion) {
        TagTimeExtractor extractor = TagTimeExtractor.createForAutoTag(options);
        if (extractor == null) {
            return null;
        }
        return new TagAutoCreation(
                snapshotManager,
                tagManager,
                tagDeletion,
                extractor,
                TagPeriodHandler.create(options),
                options.tagCreationDelay(),
                options.tagNumRetainedMax());
    }
}
