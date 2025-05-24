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
import java.util.List;

/** A strategy to expire tags by retain time and retain number. */
public class TagHybridExpire extends TagExpire {

    private final TagTimeExpire tagTimeExpire;

    private final TagNumberExpire tagNumberExpire;

    public TagHybridExpire(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        super(options, snapshotManager, tagManager, tagDeletion, callbacks);
        this.tagTimeExpire =
                TagTimeExpire.create(options, snapshotManager, tagManager, tagDeletion, callbacks);
        this.tagNumberExpire =
                TagNumberExpire.create(
                        options, snapshotManager, tagManager, tagDeletion, callbacks);
    }

    @Override
    public List<String> expire() {
        List<String> expired = tagTimeExpire.expire();
        expired.addAll(tagNumberExpire.expire());
        return expired;
    }

    @Override
    public void withOlderThanTime(LocalDateTime olderThanTime) {
        if (tagTimeExpire != null) {
            tagTimeExpire.withOlderThanTime(olderThanTime);
        }
    }

    public static TagHybridExpire create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        return new TagHybridExpire(options, snapshotManager, tagManager, tagDeletion, callbacks);
    }
}
