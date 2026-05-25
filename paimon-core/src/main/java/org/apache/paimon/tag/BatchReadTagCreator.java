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

import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

/** Creates temporary tags to protect snapshots from expiration during batch reads. */
public class BatchReadTagCreator {

    private static final Logger LOG = LoggerFactory.getLogger(BatchReadTagCreator.class);

    public static final String BATCH_READ_TAG_PREFIX = "batch-read-";

    private final TagManager tagManager;
    private final SnapshotManager snapshotManager;
    private final Duration timeRetained;

    public BatchReadTagCreator(
            TagManager tagManager, SnapshotManager snapshotManager, Duration timeRetained) {
        this.tagManager = tagManager;
        this.snapshotManager = snapshotManager;
        this.timeRetained = timeRetained;
    }

    @Nullable
    public String createReadTag(long snapshotId) {
        Snapshot snapshot;
        try {
            snapshot = snapshotManager.snapshot(snapshotId);
        } catch (Exception e) {
            LOG.warn("Failed to get snapshot {} for read protection tag.", snapshotId, e);
            return null;
        }

        String tagName = generateTagName(snapshotId);
        try {
            tagManager.createTag(snapshot, tagName, timeRetained, Collections.emptyList(), true);
            LOG.info(
                    "Created batch read protection tag '{}' for snapshot {}.", tagName, snapshotId);
            return tagName;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to create batch read protection tag for snapshot {}. "
                            + "Read will proceed without protection.",
                    snapshotId,
                    e);
            return null;
        }
    }

    public void deleteReadTag(String tagName) {
        try {
            if (tagManager.tagExists(tagName)) {
                // Directly delete the tag metadata file instead of using TagManager.deleteTag(),
                // which would also scan and delete unreferenced data files — too heavyweight for a
                // read-path cleanup. Any orphan data files left behind will be reclaimed by
                // OrphanFilesClean.
                snapshotManager.fileIO().deleteQuietly(tagManager.tagPath(tagName));
                LOG.info("Deleted batch read protection tag '{}'.", tagName);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to delete batch read protection tag '{}'. "
                            + "It will be cleaned up by TTL expiration.",
                    tagName,
                    e);
        }
    }

    public static boolean isBatchReadTag(String tagName) {
        return tagName.startsWith(BATCH_READ_TAG_PREFIX);
    }

    private String generateTagName(long snapshotId) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return BATCH_READ_TAG_PREFIX + snapshotId + "-" + uuid;
    }
}
