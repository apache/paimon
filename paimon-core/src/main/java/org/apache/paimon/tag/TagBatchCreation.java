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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Tag creation for batch mode. */
public class TagBatchCreation {

    private static final Logger LOG = LoggerFactory.getLogger(TagBatchCreation.class);

    private static final String BATCH_WRITE_TAG_PREFIX = "batch-write-";
    private final FileStoreTable table;
    private final CoreOptions options;
    private final TagManager tagManager;
    private final SnapshotManager snapshotManager;
    private final TagDeletion tagDeletion;

    public TagBatchCreation(FileStoreTable table) {
        this.table = table;
        this.snapshotManager = table.snapshotManager();
        this.tagManager = table.tagManager();
        this.tagDeletion = table.store().newTagDeletion();
        this.options = table.coreOptions();
    }

    public void createTag() {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null) {
            return;
        }
        Instant instant = Instant.ofEpochMilli(snapshot.timeMillis());
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String tagName =
                options.tagBatchCustomizedName() != null
                        ? options.tagBatchCustomizedName()
                        : BATCH_WRITE_TAG_PREFIX
                                + localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        try {
            // If the tag already exists, delete the tag
            tagManager.deleteTag(
                    tagName, tagDeletion, snapshotManager, table.store().createTagCallbacks());
            // Create a new tag
            tagManager.createTag(
                    snapshot,
                    tagName,
                    table.coreOptions().tagDefaultTimeRetained(),
                    table.store().createTagCallbacks(),
                    false);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to create tag '{}' from '${}', you can create this tag manually.",
                    tagName,
                    snapshot.id(),
                    e);
            if (tagManager.tagExists(tagName)) {
                tagManager.deleteTag(
                        tagName, tagDeletion, snapshotManager, table.store().createTagCallbacks());
            }
        }
        // Expire the tag
        expireTag();
    }

    private void expireTag() {
        Integer tagNumRetainedMax = options.tagNumRetainedMax();
        if (tagNumRetainedMax != null) {
            if (snapshotManager.latestSnapshot() == null) {
                return;
            }
            long tagCount = tagManager.tagCount();

            while (tagCount > tagNumRetainedMax) {
                for (List<String> tagNames : tagManager.tags().values()) {
                    if (tagCount - tagNames.size() >= tagNumRetainedMax) {
                        tagManager.deleteAllTagsOfOneSnapshot(
                                tagNames, tagDeletion, snapshotManager);
                        tagCount = tagCount - tagNames.size();
                    } else {
                        List<String> sortedTagNames = tagManager.sortTagsOfOneSnapshot(tagNames);
                        for (String toBeDeleted : sortedTagNames) {
                            tagManager.deleteTag(
                                    toBeDeleted,
                                    tagDeletion,
                                    snapshotManager,
                                    table.store().createTagCallbacks());
                            tagCount--;
                            if (tagCount == tagNumRetainedMax) {
                                break;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}
