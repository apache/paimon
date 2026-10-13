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
import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.TableCommitImpl;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.apache.paimon.CoreOptions.METASTORE_TAG_TO_PARTITION_PREVIEW;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TagPreview}. */
public class TagPreviewTest extends PrimaryKeyTableTestBase {

    @Test
    public void testExtractTag() {
        TagPreview preview = create();
        Optional<String> tag = preview.extractTag(-1, utcMills("2023-07-18T12:12:00"));
        assertThat(tag).hasValue("2023-07-18");
    }

    @Test
    public void testTimeTravel() {
        TagPreview preview = create();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        dynamicOptions.put(SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        TableCommitImpl commit =
                table.copy(dynamicOptions).newCommit(commitUser).ignoreEmptyCommit(false);

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(preview.timeTravel(table, "2023-07-18"))
                .containsAllEntriesOf(singletonMap(SCAN_SNAPSHOT_ID.key(), "1"));

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T13:12:00")));
        assertThat(preview.timeTravel(table, "2023-07-18"))
                .containsAllEntriesOf(singletonMap(SCAN_SNAPSHOT_ID.key(), "2"));

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-19T12:12:00")));
        assertThat(preview.timeTravel(table, "2023-07-18"))
                .containsAllEntriesOf(singletonMap(SCAN_SNAPSHOT_ID.key(), "2"));
        assertThat(preview.timeTravel(table, "2023-07-19"))
                .containsAllEntriesOf(singletonMap(SCAN_SNAPSHOT_ID.key(), "3"));
        assertThat(preview.timeTravel(table, "2023-07-20"))
                .containsAllEntriesOf(singletonMap(SCAN_SNAPSHOT_ID.key(), "3"));

        table.createTag("2023-07-17", 1);
        table.createTag("2023-07-18", 2);
        assertThat(preview.timeTravel(table, "2023-07-18"))
                .containsAllEntriesOf(singletonMap(SCAN_TAG_NAME.key(), "2023-07-18"));

        // expire 2023-07-19
        for (int i = 0; i < 5; i++) {
            commit.commit(new ManifestCommittable(0, utcMills("2023-07-21T12:12:00")));
        }
        table.createTag("2023-07-20", 7);
        assertThat(preview.timeTravel(table, "2023-07-20"))
                .containsAllEntriesOf(singletonMap(SCAN_TAG_NAME.key(), "2023-07-20"));

        assertThat(preview.timeTravel(table, "2023-07-19"))
                .containsAllEntriesOf(singletonMap(SCAN_TAG_NAME.key(), "2023-07-18"));
    }

    @Test
    public void testTimeTravelSkipsManualOnlySnapshots() throws Exception {
        TagPreview preview = create();
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);

        // the only tag is a manual one and no snapshot is early enough, so nothing resolves
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        table.createTag("my-manual-tag", 1);

        assertThatThrownBy(() -> preview.timeTravel(table, "2023-07-01"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot find snapshot or tag");

        commit.close();
    }

    @Test
    public void testTimeTravelResolvesAutoTagDespiteManualOnlyGroup() throws Exception {
        TagPreview preview = create();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        dynamicOptions.put(SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        TableCommitImpl commit =
                table.copy(dynamicOptions).newCommit(commitUser).ignoreEmptyCommit(false);

        // snapshot 1 has an auto tag, snapshot 2 only a manual tag that sorts after it
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        commit.commit(new ManifestCommittable(0, utcMills("2023-07-19T12:12:00")));
        table.createTag("2023-07-18", 1);
        table.createTag("2023-07-18-backup", 2);

        // expire snapshots 1 and 2, so no retained snapshot is early enough and the lookup
        // falls back to the tags
        for (int i = 0; i < 5; i++) {
            commit.commit(new ManifestCommittable(0, utcMills("2023-07-21T12:12:00")));
        }

        assertThat(preview.timeTravel(table, "2023-07-19"))
                .containsAllEntriesOf(singletonMap(SCAN_TAG_NAME.key(), "2023-07-18"));

        commit.close();
    }

    private TagPreview create() {
        Options options = new Options();
        options.set(METASTORE_TAG_TO_PARTITION_PREVIEW, TagCreationMode.WATERMARK);
        TagPreview preview = TagPreview.create(new CoreOptions(options));
        assertThat(preview).isNotNull();
        return preview;
    }
}
