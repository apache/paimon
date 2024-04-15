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

import java.util.List;

/** A manager to create and expire tags. */
public class TagAutoManager {

    private final TagAutoCreation tagAutoCreation;
    private final TagAutoExpire tagAutoExpire;

    private TagAutoManager(TagAutoCreation tagAutoCreation, TagAutoExpire tagAutoExpire) {
        this.tagAutoCreation = tagAutoCreation;
        this.tagAutoExpire = tagAutoExpire;
    }

    public void run() {
        if (tagAutoCreation != null) {
            tagAutoCreation.run();
        }
        if (tagAutoExpire != null) {
            tagAutoExpire.run();
        }
    }

    public static TagAutoManager create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        TagTimeExtractor extractor = TagTimeExtractor.createForAutoTag(options);

        return new TagAutoManager(
                extractor == null
                        ? null
                        : TagAutoCreation.create(options, snapshotManager, tagManager, callbacks),
                TagAutoExpire.create(options, snapshotManager, tagManager, tagDeletion, callbacks));
    }

    public TagAutoCreation getTagAutoCreation() {
        return tagAutoCreation;
    }
}
