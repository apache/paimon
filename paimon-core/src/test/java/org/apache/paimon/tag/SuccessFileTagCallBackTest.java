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
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.CoreOptions.TAG_AUTOMATIC_CREATION;
import static org.apache.paimon.CoreOptions.TAG_CREATION_PERIOD;
import static org.apache.paimon.CoreOptions.TAG_NUM_RETAINED_MAX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SuccessFileTagCallback}. */
public class SuccessFileTagCallBackTest extends PrimaryKeyTableTestBase {

    @Test
    public void testWithSuccessFile() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.TAG_CREATE_SUCCESS_FILE, true);
        options.set(TAG_AUTOMATIC_CREATION, CoreOptions.TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, CoreOptions.TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        FileStoreTable table = this.table.copy(options.toMap());
        FileIO fileIO = table.fileIO();
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        FileStore<?> fileStore = table.store();
        TagManager tagManager = fileStore.newTagManager();
        SuccessFileTagCallback successFileTagCallback = null;

        List<TagCallback> tagCallbacks = fileStore.createTagCallbacks();
        assertThat(tagCallbacks).hasAtLeastOneElementOfType(SuccessFileTagCallback.class);

        for (TagCallback tagCallback : tagCallbacks) {
            if (tagCallback instanceof SuccessFileTagCallback) {
                successFileTagCallback = (SuccessFileTagCallback) tagCallback;
            }
        }

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");
        assertThat(fileIO.exists(successFileTagCallback.tagSuccessFilePath("2023-07-18 11")))
                .isTrue();

        table.deleteTag("2023-07-18 11");
        assertThat(tagManager.get("2023-07-18 11")).isEmpty();
        assertThat(fileIO.exists(successFileTagCallback.tagSuccessFilePath("2023-07-18 11")))
                .isFalse();
    }

    @Test
    public void testWithoutSuccessFile() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.TAG_CREATE_SUCCESS_FILE, false);
        options.set(TAG_AUTOMATIC_CREATION, CoreOptions.TagCreationMode.WATERMARK);
        options.set(TAG_CREATION_PERIOD, CoreOptions.TagCreationPeriod.HOURLY);
        options.set(TAG_NUM_RETAINED_MAX, 3);
        FileStoreTable table = this.table.copy(options.toMap());
        FileIO fileIO = table.fileIO();
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        FileStore<?> fileStore = table.store();
        TagManager tagManager = fileStore.newTagManager();
        List<TagCallback> tagCallbacks = fileStore.createTagCallbacks();
        assertThat(tagCallbacks).doesNotHaveAnyElementsOfTypes(SuccessFileTagCallback.class);

        commit.commit(new ManifestCommittable(0, utcMills("2023-07-18T12:12:00")));
        assertThat(tagManager.allTagNames()).containsOnly("2023-07-18 11");
        assertThat(fileIO.exists(new Path(tagManager.tagPath("2023-07-18 11"), "tag-success-file")))
                .isFalse();

        table.deleteTag("2023-07-18 11");
        assertThat(tagManager.get("2023-07-18 11")).isEmpty();
        assertThat(fileIO.exists(new Path(tagManager.tagPath("2023-07-18 11"), "tag-success-file")))
                .isFalse();
    }
}
