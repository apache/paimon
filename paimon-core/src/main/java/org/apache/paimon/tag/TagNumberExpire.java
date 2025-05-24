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
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/** A strategy to expire tags by retain number. */
public class TagNumberExpire extends TagExpire {

    public TagNumberExpire(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        super(options, snapshotManager, tagManager, tagDeletion, callbacks);
    }

    @Override
    public List<String> expire() {
        Integer tagNumRetainedMax = options.tagNumRetainedMax();
        boolean writeOnly = options.writeOnly();
        List<String> expired = new ArrayList<>();
        if (!writeOnly && tagNumRetainedMax != null) {
            if (snapshotManager.latestSnapshot() == null) {
                return expired;
            }
            long tagCount = tagManager.tagCount();

            while (tagCount > tagNumRetainedMax) {
                for (List<String> tagNames : tagManager.tags().values()) {
                    if (tagCount - tagNames.size() > tagNumRetainedMax) {
                        tagManager.deleteAllTagsOfOneSnapshot(
                                tagNames, tagDeletion, snapshotManager);
                        expired.addAll(tagNames);
                        tagCount = tagCount - tagNames.size();
                    } else {
                        List<String> sortedTagNames = tagManager.sortTagsOfOneSnapshot(tagNames);
                        for (String toBeDeleted : sortedTagNames) {
                            tagManager.deleteTag(
                                    toBeDeleted, tagDeletion, snapshotManager, callbacks);
                            expired.add(toBeDeleted);
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
        return expired;
    }

    @Override
    public void withOlderThanTime(LocalDateTime olderThanTime) {
        // do nothing
    }

    public static TagNumberExpire create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        return new TagNumberExpire(options, snapshotManager, tagManager, tagDeletion, callbacks);
    }
}
