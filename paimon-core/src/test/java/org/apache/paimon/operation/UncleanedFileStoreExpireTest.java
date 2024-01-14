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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FileStoreExpireImpl}. Some files not in use may still remain after the test due
 * to the testing methods.
 */
public class UncleanedFileStoreExpireTest extends FileStoreExpireTestBase {

    @Test
    public void testExpireWithMissingFiles() throws Exception {
        FileStoreExpire expire = store.newExpire(1, 1, 1);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        Set<Path> filesInUse = store.getFilesInUse(latestSnapshotId);
        List<Path> unusedFileList =
                Files.walk(Paths.get(tempDir.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("snapshot"))
                        .filter(p -> !p.getFileName().toString().startsWith("schema"))
                        .map(p -> new Path(p.toString()))
                        .filter(p -> !filesInUse.contains(p))
                        .collect(Collectors.toList());

        // shuffle list
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = unusedFileList.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            Collections.swap(unusedFileList, i, j);
        }

        // delete some unused files
        int numFilesToDelete = random.nextInt(unusedFileList.size());
        for (int i = 0; i < numFilesToDelete; i++) {
            fileIO.deleteQuietly(unusedFileList.get(i));
        }

        expire.expire();

        for (int i = 1; i < latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        assertThat(snapshotManager.snapshotExists(latestSnapshotId)).isTrue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
    }

    @Test
    public void testMixedSnapshotAndTagDeletion() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        commit(random.nextInt(10) + 30, allData, snapshotPositions);
        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        TagManager tagManager = store.newTagManager();

        // create tags for each snapshot
        for (int id = 1; id <= latestSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            tagManager.createTag(snapshot, "tag" + id, Collections.emptyList());
        }

        // randomly expire snapshots
        int expired = random.nextInt(latestSnapshotId / 2) + 1;
        int retained = latestSnapshotId - expired;
        store.newExpire(retained, retained, Long.MAX_VALUE).expire();

        // randomly delete tags
        for (int id = 1; id <= latestSnapshotId; id++) {
            if (random.nextBoolean()) {
                tagManager.deleteTag("tag" + id, store.newTagDeletion(), snapshotManager);
            }
        }

        // check snapshots and tags
        Set<Snapshot> allSnapshots = new HashSet<>();
        snapshotManager.snapshots().forEachRemaining(allSnapshots::add);
        allSnapshots.addAll(tagManager.taggedSnapshots());

        for (Snapshot snapshot : allSnapshots) {
            assertSnapshot(snapshot, allData, snapshotPositions);
        }
    }
}
