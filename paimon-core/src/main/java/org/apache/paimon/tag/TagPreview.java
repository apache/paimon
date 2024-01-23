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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;

/** A class of tag preview to find suitable snapshots. */
public class TagPreview {

    private final TagTimeExtractor timeExtractor;
    private final TagPeriodHandler periodHandler;

    private TagPreview(CoreOptions options) {
        this.timeExtractor = TagTimeExtractor.createForTagPreview(options);
        this.periodHandler = TagPeriodHandler.create(options);
    }

    public static TagPreview create(CoreOptions options) {
        if (options.tagToPartitionPreview() != CoreOptions.TagCreationMode.NONE
                && options.tagToPartitionPreview() != CoreOptions.TagCreationMode.BATCH) {
            return new TagPreview(options);
        }
        return null;
    }

    public Optional<String> extractTag(long timeMilli, @Nullable Long watermark) {
        Optional<LocalDateTime> timeOptional = timeExtractor.extract(timeMilli, watermark);
        if (!timeOptional.isPresent()) {
            return Optional.empty();
        }
        LocalDateTime currentTag =
                periodHandler.nextTagTime(periodHandler.normalizeToPreviousTag(timeOptional.get()));
        String tag = periodHandler.timeToTag(currentTag);
        return Optional.of(tag);
    }

    public Map<String, String> timeTravel(FileStoreTable table, String tag) {
        TagManager tagManager = table.tagManager();
        if (tagManager.tagExists(tag)) {
            return singletonMap(SCAN_TAG_NAME.key(), tag);
        }

        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot =
                snapshotManager.traversalSnapshotsFromLatestSafely(
                        s ->
                                extractTag(s.timeMillis(), s.watermark())
                                        .map(t -> t.compareTo(tag) <= 0)
                                        .orElse(false));
        if (snapshot != null) {
            return singletonMap(SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
        }

        Optional<String> findTag =
                tagManager.tags().values().stream()
                        .map(this::toOneAutoTag)
                        .filter(t -> t.compareTo(tag) <= 0)
                        .max(Comparator.naturalOrder());
        if (findTag.isPresent()) {
            return singletonMap(SCAN_TAG_NAME.key(), findTag.get());
        }

        throw new RuntimeException("Cannot find snapshot or tag for tag name: " + tag);
    }

    private String toOneAutoTag(List<String> tags) {
        List<String> autoTags = new ArrayList<>();
        for (String tag : tags) {
            if (periodHandler.isAutoTag(tag)) {
                autoTags.add(tag);
            }
        }
        return TagAutoCreation.checkAndGetOneAutoTag(autoTags);
    }
}
