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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChangelogManager}. */
public class ChangelogManagerTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private ChangelogManager changelogManager;

    @BeforeEach
    public void before() {
        fileIO = LocalFileIO.create();
        changelogManager = new ChangelogManager(fileIO, new Path(tempDir.toUri().toString()), null);
    }

    @Test
    public void testCommitChangelogWritesAtomically() throws Exception {
        FileIO spyIO = Mockito.spy(fileIO);
        ChangelogManager spyManager =
                new ChangelogManager(spyIO, new Path(tempDir.toUri().toString()), null);
        Changelog changelog = new Changelog(createSnapshotWithMillis(1, 1000));
        Path changelogPath = spyManager.longLivedChangelogPath(1);

        spyManager.commitChangelog(changelog, 1);

        // the target must never be opened for a direct overwrite: a crash midway would
        // leave readers with an empty or partial changelog file
        Mockito.verify(spyIO, Mockito.never())
                .writeFile(
                        ArgumentMatchers.eq(changelogPath),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.eq(true));
        // readFileUtf8 strips newlines, so compare through a parse round-trip
        assertThat(Changelog.fromJson(fileIO.readFileUtf8(changelogPath))).isEqualTo(changelog);

        // retrying the same commit is idempotent, a different content for the same id fails
        spyManager.commitChangelog(changelog, 1);
        Changelog other = new Changelog(createSnapshotWithMillis(1, 2000));
        assertThatThrownBy(() -> spyManager.commitChangelog(other, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exists with different content");
    }

    @Test
    public void testSafelyGetAllChangelogsSkipsEmptyFile() throws Exception {
        Snapshot snapshot = createSnapshotWithMillis(1, 1000);
        changelogManager.commitChangelog(new Changelog(snapshot), 1);

        // a torn write can leave an empty changelog file behind
        fileIO.writeFile(changelogManager.longLivedChangelogPath(2), "", true);

        assertThat(changelogManager.safelyGetAllChangelogs())
                .extracting(Changelog::id)
                .containsExactly(1L);
    }
}
