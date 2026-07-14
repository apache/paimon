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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link TagTimeExpire}. */
public class TagTimeExpireTest {

    @Test
    public void testOlderThanUsesEmbeddedCreateTimeWithoutRetention() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        TagManager tagManager = mock(TagManager.class);
        TagDeletion tagDeletion = mock(TagDeletion.class);
        Tag tag = mock(Tag.class);
        LocalDateTime createTime = LocalDateTime.of(2025, 1, 1, 0, 0);
        when(tag.getTagCreateTime()).thenReturn(createTime);
        when(tag.getTagTimeRetained()).thenReturn(null);
        when(tagManager.tagObjects())
                .thenReturn(Collections.singletonList(Pair.of(tag, "old-tag")));

        TagTimeExpire expire =
                TagTimeExpire.create(
                                snapshotManager, tagManager, tagDeletion, Collections.emptyList())
                        .withOlderThanTime(createTime.plusDays(1));

        assertThat(expire.expire()).containsExactly("old-tag");
        verify(tagManager)
                .deleteTag("old-tag", tagDeletion, snapshotManager, Collections.emptyList());
    }
}
