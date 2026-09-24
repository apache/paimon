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

package org.apache.paimon.table;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.PojoManifestEntry;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests partition-layout metadata derived while scanning manifests. */
public class PartitionLayoutScanTest extends TableTestBase {

    @Test
    public void testPartitionLayoutIgnoresDeletedFileInSameManifest() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeDataDefault(Collections.singletonList(dataDefault(1, 1)));
        writeDataDefault(Collections.singletonList(dataDefault(2, 2)));

        FileStore<?> store = table.store();
        List<ManifestEntry> existingEntries =
                store.manifestListFactory().create()
                        .readDataManifests(store.snapshotManager().latestSnapshot()).stream()
                        .flatMap(
                                manifest ->
                                        store.manifestFileFactory().create()
                                                .read(manifest.fileName(), manifest.fileSize())
                                                .stream())
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .collect(Collectors.toList());

        ManifestEntry oldLayoutFile = existingEntries.get(0);
        ManifestEntry liveLayoutFile = existingEntries.get(1);
        List<ManifestFileMeta> manifests =
                store.manifestFileFactory()
                        .create()
                        .write(
                                Arrays.asList(
                                        new PojoManifestEntry(
                                                FileKind.ADD,
                                                oldLayoutFile.partition(),
                                                oldLayoutFile.bucket(),
                                                4,
                                                oldLayoutFile.file()),
                                        new PojoManifestEntry(
                                                FileKind.ADD,
                                                liveLayoutFile.partition(),
                                                liveLayoutFile.bucket(),
                                                2,
                                                liveLayoutFile.file()),
                                        new PojoManifestEntry(
                                                FileKind.DELETE,
                                                oldLayoutFile.partition(),
                                                oldLayoutFile.bucket(),
                                                4,
                                                oldLayoutFile.file())));

        Pair<String, Long> baseManifestList = store.manifestListFactory().create().write(manifests);
        Pair<String, Long> emptyManifestList =
                store.manifestListFactory().create().write(Collections.emptyList());
        long snapshotId = store.snapshotManager().latestSnapshotId() + 1;
        Snapshot snapshot =
                new Snapshot(
                        snapshotId,
                        table.schema().id(),
                        baseManifestList.getKey(),
                        baseManifestList.getValue(),
                        emptyManifestList.getKey(),
                        emptyManifestList.getValue(),
                        null,
                        null,
                        null,
                        commitUser,
                        null,
                        snapshotId,
                        Snapshot.CommitKind.OVERWRITE,
                        System.currentTimeMillis(),
                        liveLayoutFile.file().rowCount(),
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        store.snapshotManager()
                .fileIO()
                .tryToWriteAtomic(
                        store.snapshotManager().snapshotPath(snapshotId), snapshot.toJson());

        assertThat(table.newSnapshotReader().withSnapshot(snapshotId).partitionEntries())
                .extracting(PartitionEntry::totalBuckets)
                .containsExactly(2);
    }
}
