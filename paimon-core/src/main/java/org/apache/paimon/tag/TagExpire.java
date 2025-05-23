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

/** Strategy for tag expiration. */
public abstract class TagExpire {

    protected final CoreOptions options;

    protected final SnapshotManager snapshotManager;

    protected final TagManager tagManager;

    protected final TagDeletion tagDeletion;

    protected final List<TagCallback> callbacks;

    protected TagExpire(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.tagDeletion = tagDeletion;
        this.callbacks = callbacks;
    }

    public abstract List<String> expire();

    public abstract void withOlderThanTime(LocalDateTime olderThanTime);

    public static TagExpire createTagExpireStrategy(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            List<TagCallback> callbacks) {
        switch (options.tagExpireStrategy()) {
            case RETAIN_TIME:
                return TagTimeExpire.create(
                        options, snapshotManager, tagManager, tagDeletion, callbacks);
            case RETAIN_NUMBER:
                return TagNumberExpire.create(
                        options, snapshotManager, tagManager, tagDeletion, callbacks);
            case HYBRID:
            default:
                return TagHybridExpire.create(
                        options, snapshotManager, tagManager, tagDeletion, callbacks);
        }
    }
}
