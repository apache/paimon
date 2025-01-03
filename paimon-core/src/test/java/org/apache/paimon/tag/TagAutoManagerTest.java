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

import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.CoreOptions.TagCreationPeriod;
import org.apache.paimon.CoreOptions.TagPeriodFormatter;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;

import static org.apache.paimon.CoreOptions.SINK_WATERMARK_TIME_ZONE;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.SNAPSHOT_WATERMARK_IDLE_TIMEOUT;
import static org.apache.paimon.CoreOptions.TAG_AUTOMATIC_COMPLETION;
import static org.apache.paimon.CoreOptions.TAG_AUTOMATIC_CREATION;
import static org.apache.paimon.CoreOptions.TAG_CREATION_DELAY;
import static org.apache.paimon.CoreOptions.TAG_CREATION_PERIOD;
import static org.apache.paimon.CoreOptions.TAG_DEFAULT_TIME_RETAINED;
import static org.apache.paimon.CoreOptions.TAG_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.TAG_PERIOD_FORMATTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for tag automatic creation. */
public class TagAutoManagerTest extends PrimaryKeyTableTestBase {

    @Test
    public void testTag() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test normal creation
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        // test not creation
        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T12:59:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        // test just in time
        commit.commit(new ManifestCommittable(2, utcMills("2023-07-18T13:00:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11", "2023-07-18 12");

        // test expire old tag
        commit.commit(new ManifestCommittable(3, utcMills("2023-07-18T14:00:00")));
        commit.commit(new ManifestCommittable(4, utcMills("2023-07-18T15:00:00")));
        assertThat(tagManager.allTagNames())
                .containsOnly("2023-07-18 12", "2023-07-18 13", "2023-07-18 14");

        // test restore with snapshot expiration
        commit.commit(new ManifestCommittable(5, utcMills("2023-07-18T15:01:00")));
        commit.commit(new ManifestCommittable(6, utcMills("2023-07-18T15:02:00")));

        Options expireSetting = new Options();
        expireSetting.set(SNAPSHOT_NUM_RETAINED_MIN, 1);
        expireSetting.set(SNAPSHOT_NUM_RETAINED_MAX, 1);
        commit.close();

        commit = table.copy(expireSetting.toMap()).newCommit(commitUser).ignoreEmptyCommit(false);

        // trigger snapshot expiration
        commit.commit(new ManifestCommittable(7, utcMills("2023-07-18T15:03:00")));
        commit.commit(new ManifestCommittable(8, utcMills("2023-07-18T15:04:00")));

        // check tags
        commit.commit(new ManifestCommittable(9, utcMills("2023-07-18T16:00:00")));
        assertThat(tagManager.allTagNames())
                .containsOnly("2023-07-18 13", "2023-07-18 14", "2023-07-18 15");
        commit.close();
    }

    @Test
    public void testTagDelay() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_CREATION_DELAY, Duration.ofSeconds(10));
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test first create tag anyway
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:00:09")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        // test not create due to delay
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:09")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        // test create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:10")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11", "2023-07-18 12");
        commit.close();
    }

    @Test
    public void testTagSinkWatermark() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(SINK_WATERMARK_TIME_ZONE, ZoneId.systemDefault().toString());
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test watermark is Long.MIN_VALUE.
        commit.commit(new ManifestCommittable(0, Long.MIN_VALUE));
        assertThat(tagManager.allTagNames()).isEmpty();

        // test first create
        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T12:00:09")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        // test second create
        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T13:00:10")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11", "2023-07-18 12");
        commit.close();
    }

    @Test
    public void testTagTwoHour() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.TWO_HOURS);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test first create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:00:01")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 10");

        // test no create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:01")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 10");

        // test second create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T14:00:09")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 10", "2023-07-18 12");
        commit.close();
    }

    @Test
    public void testTagDaily() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.DAILY);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test first create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:00:01")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-17");

        // test second create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-19T12:00:01")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-17", "2023-07-18");

        // test newCommit create
        commit.close();
        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-20T12:00:01")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-17", "2023-07-18", "2023-07-19");
        commit.close();
    }

    @Test
    public void testModifyTagPeriod() {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        FileStoreTable table;
        TableCommitImpl commit;
        TagManager tagManager;
        table = this.table.copy(options.toMap());
        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        tagManager = table.store().newTagManager();

        // test first create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:00:09")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.DAILY);
        table = table.copy(options.toMap());
        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        tagManager = table.store().newTagManager();

        // test newCommit create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-20T12:00:01")));
        assertThat(tagManager.allTagNames()).contains("2023-07-18 11", "2023-07-19");
    }

    @Test
    public void testSavepointTag() {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        FileStoreTable table;
        TableCommitImpl commit;
        TagManager tagManager;
        table = this.table.copy(options.toMap());

        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        tagManager = table.store().newTagManager();

        // test normal creation
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");

        table.createTag("savepoint-11", 1);

        // test newCommit create
        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T14:00:00")));
        assertThat(tagManager.allTagNames())
                .containsOnly("savepoint-11", "2023-07-18 11", "2023-07-18 13");

        // test expire old tag
        commit.commit(new ManifestCommittable(2, utcMills("2023-07-18T15:00:00")));
        commit.commit(new ManifestCommittable(3, utcMills("2023-07-18T16:00:00")));
        // only handle auto-created tags
        assertThat(tagManager.allTagNames())
                .containsOnly("savepoint-11", "2023-07-18 13", "2023-07-18 14", "2023-07-18 15");
    }

    @Test
    public void testTagDatePeriodFormatter() {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.DAILY);
        options.set(TAG_PERIOD_FORMATTER, TagPeriodFormatter.WITHOUT_DASHES);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("20230717");

        commit.commit(new ManifestCommittable(1, utcMills("2023-07-19T12:12:00")));
        assertThat(tagManager.allTagNames()).contains("20230717", "20230718");
    }

    @Test
    public void testTagHourlyPeriodFormatter() {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_PERIOD_FORMATTER, TagPeriodFormatter.WITHOUT_DASHES);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("20230718 11");

        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T13:13:00")));
        assertThat(tagManager.allTagNames()).contains("20230718 11", "20230718 12");
    }

    @Test
    public void testOnlyExpireAutoCreatedTag() {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 1);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        table.createTag("many-tags-test");
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11", "many-tags-test");

        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T13:13:00")));
        assertThat(tagManager.allTagNames()).contains("2023-07-18 12", "many-tags-test");
    }

    @Test
    public void testWatermarkIdleTimeoutForceCreatingSnapshot() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(SINK_WATERMARK_TIME_ZONE, ZoneId.systemDefault().toString());
        options.set(SNAPSHOT_WATERMARK_IDLE_TIMEOUT.key(), "10 s");
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);

        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T12:00:00")));
        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T12:00:10")));

        Long watermark1 = table.snapshotManager().snapshot(1).watermark();
        Long watermark2 = table.snapshotManager().snapshot(2).watermark();
        assertThat(watermark2 - watermark1).isEqualTo(10000);
        commit.close();
    }

    @Test
    public void testAutoCreateTagNotExpiredByTimeRetained() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        options.set(TAG_DEFAULT_TIME_RETAINED, Duration.ofMillis(500));
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test normal creation
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T14:00:00")));
        commit.commit(new ManifestCommittable(2, utcMills("2023-07-18T15:12:00")));
        commit.commit(new ManifestCommittable(3, utcMills("2023-07-18T16:00:00")));

        // test expire old tag by time-retained
        Thread.sleep(1000);
        commit.commit(new ManifestCommittable(4, utcMills("2023-07-18T19:00:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 18");

        commit.close();
    }

    @Test
    public void testExpireTagsByTimeRetained() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        options.set(TAG_DEFAULT_TIME_RETAINED, Duration.ofMillis(500));
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test normal creation
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T14:00:00")));
        commit.commit(new ManifestCommittable(2, utcMills("2023-07-18T15:12:00")));
        commit.commit(new ManifestCommittable(3, utcMills("2023-07-18T16:00:00")));

        Snapshot snapshot1 =
                new Snapshot(
                        4,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        0L,
                        Snapshot.CommitKind.APPEND,
                        1000,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        tagManager.createTag(
                snapshot1,
                "non-auto-create-tag-shoule-expire",
                Duration.ofMillis(500),
                Collections.emptyList(),
                false);

        Snapshot snapshot2 =
                new Snapshot(
                        5,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        0L,
                        Snapshot.CommitKind.APPEND,
                        1000,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        tagManager.createTag(
                snapshot2,
                "non-auto-create-tag-shoule-not-expire",
                Duration.ofDays(1),
                Collections.emptyList(),
                false);

        // test expire old tag by time-retained
        Thread.sleep(1000);
        commit.commit(new ManifestCommittable(6, utcMills("2023-07-18T19:00:00")));
        assertThat(tagManager.allTagNames())
                .containsOnly("2023-07-18 18", "non-auto-create-tag-shoule-not-expire");
        commit.close();
    }

    @Test
    public void testAutoCompleteTags() throws Exception {
        Options options = new Options();
        options.set(TAG_AUTOMATIC_CREATION, TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        options.set(TAG_AUTOMATIC_COMPLETION, true);
        FileStoreTable table = this.table.copy(options.toMap());
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        TagManager tagManager = table.store().newTagManager();

        // test normal creation
        commit.commit(new ManifestCommittable(0, utcMills("2024-06-26T16:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2024-06-26 15");

        // task stop 2h...

        // task restart after 18:00
        // first commit, add tag 2024-06-26 16
        commit.commit(new ManifestCommittable(1, utcMills("2024-06-26T18:05:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2024-06-26 15", "2024-06-26 16");

        // second commit, add tag 2024-06-26 17
        commit.commit(new ManifestCommittable(2, utcMills("2024-06-26T18:10:00")));
        assertThat(tagManager.allTagNames())
                .containsOnly("2024-06-26 15", "2024-06-26 16", "2024-06-26 17");

        commit.close();
    }

    private long localZoneMills(String timestamp) {
        return LocalDateTime.parse(timestamp)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}
