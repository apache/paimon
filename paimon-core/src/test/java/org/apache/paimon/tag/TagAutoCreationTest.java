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

import static org.apache.paimon.CoreOptions.SINK_WATERMARK_TIME_ZONE;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.TAG_AUTOMATIC_CREATION;
import static org.apache.paimon.CoreOptions.TAG_CREATION_DELAY;
import static org.apache.paimon.CoreOptions.TAG_CREATION_PERIOD;
import static org.apache.paimon.CoreOptions.TAG_NUM_RETAINED_MAX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for tag automatic creation. */
public class TagAutoCreationTest extends PrimaryKeyTableTestBase {

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
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11");

        // test not creation
        commit.commit(new ManifestCommittable(1, utcMills("2023-07-18T12:59:00")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11");

        // test just in time
        commit.commit(new ManifestCommittable(2, utcMills("2023-07-18T13:00:00")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11", "2023-07-18 12");

        // test expire old tag
        commit.commit(new ManifestCommittable(3, utcMills("2023-07-18T14:00:00")));
        commit.commit(new ManifestCommittable(4, utcMills("2023-07-18T15:00:00")));
        assertThat(tagManager.tags().values())
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
        assertThat(tagManager.tags().values())
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
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11");

        // test not create due to delay
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:09")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11");

        // test create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:10")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11", "2023-07-18 12");
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

        // test first create
        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T12:00:09")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11");

        // test second create
        commit.commit(new ManifestCommittable(0, localZoneMills("2023-07-18T13:00:10")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 11", "2023-07-18 12");
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
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 10");

        // test no create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:00:01")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 10");

        // test second create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T14:00:09")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-18 10", "2023-07-18 12");
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
        assertThat(tagManager.tags().values()).containsOnly("2023-07-17");

        // test second create
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-19T12:00:01")));
        assertThat(tagManager.tags().values()).containsOnly("2023-07-17", "2023-07-18");

        // test newCommit create
        commit.close();
        commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-20T12:00:01")));
        assertThat(tagManager.tags().values())
                .containsOnly("2023-07-17", "2023-07-18", "2023-07-19");
        commit.close();
    }

    private long localZoneMills(String timestamp) {
        return LocalDateTime.parse(timestamp)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}
