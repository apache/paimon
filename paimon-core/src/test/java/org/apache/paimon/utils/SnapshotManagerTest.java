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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotManager}. */
public class SnapshotManagerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSnapshotPath() {
        SnapshotManager snapshotManager =
                new SnapshotManager(LocalFileIO.create(), new Path(tempDir.toString()));
        for (int i = 0; i < 20; i++) {
            assertThat(snapshotManager.snapshotPath(i))
                    .isEqualTo(new Path(tempDir.toString() + "/snapshot/snapshot-" + i));
        }
    }

    @Test
    public void testEarlierOrEqualTimeMills() throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot =
                    new Snapshot(
                            i,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            0L,
                            Snapshot.CommitKind.APPEND,
                            millis + i * 1000,
                            null,
                            null,
                            null,
                            null,
                            null);
            localFileIO.writeFileUtf8(snapshotManager.snapshotPath(i), snapshot.toJson());
        }
        // smaller than the second snapshot return the first snapshot
        assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 999).timeMillis())
                .isEqualTo(millis);
        // equal to the second snapshot return the second snapshot
        assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1000).timeMillis())
                .isEqualTo(millis + 1000);
        // larger than the second snapshot return the second snapshot
        assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1001).timeMillis())
                .isEqualTo(millis + 1000);
    }
}
