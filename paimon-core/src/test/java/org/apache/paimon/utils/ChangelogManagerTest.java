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

import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;

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
