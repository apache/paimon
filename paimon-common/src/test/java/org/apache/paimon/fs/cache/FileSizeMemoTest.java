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

package org.apache.paimon.fs.cache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileSizeMemo}. */
class FileSizeMemoTest {

    @Test
    void putsAloneBoundTheMemo() {
        int bound = FileSizeMemo.maxEntries();
        FileSizeMemo memo = new FileSizeMemo();

        for (int i = 0; i < bound; i++) {
            memo.put("file-" + i, i);
        }
        assertThat(memo.size()).isEqualTo(bound);

        // check the first overflow on its own: a step that evicts the wrong number of entries
        // shows up here, where no later put can bring the count back to the bound
        memo.put("over-0", 0L);
        assertThat(memo.size()).isEqualTo(bound);

        for (int i = 1; i < 1024; i++) {
            memo.put("over-" + i, i);
        }
        // no read anywhere above, so the write path is what has to bound it
        assertThat(memo.size()).isEqualTo(bound);
    }

    @Test
    void aReadEntryOutlivesAnUnreadOne() {
        int bound = FileSizeMemo.maxEntries();
        // below 4 the four assertions below are not four distinct keys
        assertThat(bound).isGreaterThanOrEqualTo(4);
        int read = bound / 2;
        int unread = bound - read;
        FileSizeMemo memo = new FileSizeMemo();
        for (int i = 0; i < bound; i++) {
            memo.put("file-" + i, i);
        }

        // reading the older entries makes them the recently used ones
        for (int i = 0; i < read; i++) {
            assertThat(memo.get("file-" + i)).isEqualTo(i);
        }
        // exactly as many new entries as were left unread, so those are what eviction takes
        for (int i = bound; i < bound + unread; i++) {
            memo.put("file-" + i, i);
        }

        assertThat(memo.get("file-0")).isEqualTo(0L);
        assertThat(memo.get("file-" + (read - 1))).isEqualTo(read - 1L);
        assertThat(memo.get("file-" + read)).isEqualTo(-1L);
        assertThat(memo.get("file-" + (bound - 1))).isEqualTo(-1L);
    }

    @Test
    void puttingAnEntryAgainRefreshesIt() {
        int bound = FileSizeMemo.maxEntries();
        // at a bound of 1 the loop below never runs, so nothing would pin the position half
        assertThat(bound).isGreaterThanOrEqualTo(2);
        FileSizeMemo memo = new FileSizeMemo();
        for (int i = 0; i < bound; i++) {
            memo.put("file-" + i, i);
        }

        memo.put("file-0", 100L);
        for (int i = bound; i < bound + bound - 1; i++) {
            memo.put("file-" + i, i);
        }

        // the re-put carried both the newer value and the newer position
        assertThat(memo.get("file-0")).isEqualTo(100L);
        assertThat(memo.get("file-1")).isEqualTo(-1L);
    }

    @Test
    void invalidateRemovesOnlyTheMatchingPrefix() {
        // the fixture below holds four entries, and none of them may be evicted
        assertThat(FileSizeMemo.maxEntries()).isGreaterThanOrEqualTo(4);
        FileSizeMemo memo = new FileSizeMemo();
        memo.put("/a/one", 1L);
        memo.put("/a/two", 2L);
        memo.put("/b/three", 3L);
        // carries the prefix, but not at the front
        memo.put("/b/a/four", 4L);

        memo.invalidate("/a/");

        assertThat(memo.get("/a/one")).isEqualTo(-1L);
        assertThat(memo.get("/a/two")).isEqualTo(-1L);
        assertThat(memo.get("/b/three")).isEqualTo(3L);
        assertThat(memo.get("/b/a/four")).isEqualTo(4L);
    }
}
