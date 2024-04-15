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
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/** A manager to expire tags. */
public class TagAutoExpire {

    private static final Logger LOG = LoggerFactory.getLogger(TagAutoExpire.class);

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagDeletion tagDeletion;
    private final TagPeriodHandler periodHandler;
    private final Integer numRetainedMax;
    private final List<TagCallback> callbacks;

    private TagAutoExpire(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            TagPeriodHandler periodHandler,
            Duration delay,
            Integer numRetainedMax,
            List<TagCallback> callbacks) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.tagDeletion = tagDeletion;
        this.periodHandler = periodHandler;
        this.numRetainedMax = numRetainedMax;
        this.callbacks = callbacks;
        this.periodHandler.validateDelay(delay);
    }

    public void run() {
        Set<String> deleteTags = new HashSet<>();
        deleteTags.addAll(getExpireTagsByNumRetainedMax());
        deleteTags.addAll(getExpireTagsByTimeRetained());
        deleteTags.forEach(
                tag -> tagManager.deleteTag(tag, tagDeletion, snapshotManager, callbacks));
    }

    private Set<String> getExpireTagsByNumRetainedMax() {
        Set<String> deleteTags = new HashSet<>();
        if (numRetainedMax != null) {
            // only handle auto-created tags here
            SortedMap<Snapshot, List<String>> tags = tagManager.tags(periodHandler::isAutoTag);
            if (tags.size() > numRetainedMax) {
                int toDelete = tags.size() - numRetainedMax;
                int i = 0;
                for (List<String> tag : tags.values()) {
                    String tagName = TagAutoCreation.checkAndGetOneAutoTag(tag);
                    LOG.info(
                            "Delete tag {}, because the number of auto-created tags reached numRetainedMax of {}.",
                            tagName,
                            numRetainedMax);
                    deleteTags.add(tagName);
                    i++;
                    if (i == toDelete) {
                        break;
                    }
                }
            }
        }
        return deleteTags;
    }

    private Set<String> getExpireTagsByTimeRetained() {
        // handle auto-created and non-auto-created-tags here
        Set<String> deleteTags = new HashSet<>();
        SortedMap<Tag, List<String>> tags = tagManager.tagsWithTimeRetained();
        for (Map.Entry<Tag, List<String>> entry : tags.entrySet()) {
            Tag tag = entry.getKey();
            LocalDateTime createTime = tag.getTagCreateTime();
            Duration timeRetained = tag.getTagTimeRetained();
            if (createTime == null || timeRetained == null) {
                continue;
            }
            if (LocalDateTime.now().isAfter(createTime.plus(timeRetained))) {
                for (String tagName : entry.getValue()) {
                    LOG.info(
                            "Delete tag {}, because its existence time has reached its timeRetained of {}.",
                            tagName,
                            timeRetained);
                    deleteTags.add(tagName);
                }
            }
        }
        return deleteTags;
    }

    public static TagAutoExpire create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        return new TagAutoExpire(
                snapshotManager,
                tagManager,
                tagDeletion,
                TagPeriodHandler.create(options),
                options.tagCreationDelay(),
                options.tagNumRetainedMax(),
                callbacks);
    }
}
