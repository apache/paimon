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

import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/** A manager to expire tags by time. */
public class TagTimeExpire {

    private static final Logger LOG = LoggerFactory.getLogger(TagTimeExpire.class);

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagDeletion tagDeletion;
    private final List<TagCallback> callbacks;

    private TagTimeExpire(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.tagDeletion = tagDeletion;
        this.callbacks = callbacks;
    }

    public void run() {
        List<Pair<Tag, String>> tags = tagManager.tagObjects();
        for (Pair<Tag, String> pair : tags) {
            Tag tag = pair.getLeft();
            String tagName = pair.getRight();
            LocalDateTime createTime = tag.getTagCreateTime();
            Duration timeRetained = tag.getTagTimeRetained();
            if (createTime == null || timeRetained == null) {
                continue;
            }
            if (LocalDateTime.now().isAfter(createTime.plus(timeRetained))) {
                LOG.info(
                        "Delete tag {}, because its existence time has reached its timeRetained of {}.",
                        tagName,
                        timeRetained);
                tagManager.deleteTag(tagName, tagDeletion, snapshotManager, callbacks);
            }
        }
    }

    public static TagTimeExpire create(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        return new TagTimeExpire(snapshotManager, tagManager, tagDeletion, callbacks);
    }
}
