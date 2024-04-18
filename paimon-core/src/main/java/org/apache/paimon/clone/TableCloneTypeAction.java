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

package org.apache.paimon.clone;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;
import static org.apache.paimon.utils.TagManager.TAG_PREFIX;

/** The action of table clone. */
public class TableCloneTypeAction extends AbstractCloneTypeAction {

    public TableCloneTypeAction() {}

    @Override
    public List<CloneFileInfo> findCloneFilePaths(
            Map<String, Pair<Path, Long>> tableAllFilesExcludeTableRoot,
            SnapshotManager snapshotManager,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler,
            TagManager tagManager) {
        return tableAllFilesExcludeTableRoot.entrySet().stream()
                .map(
                        entry ->
                                new CloneFileInfo(
                                        entry.getValue().getLeft(),
                                        entry.getValue().getRight(),
                                        entry.getKey().startsWith(SNAPSHOT_PREFIX)
                                                || entry.getKey().startsWith(TAG_PREFIX)))
                .collect(Collectors.toList());
    }

    @Override
    public void commitSnapshotHintInTargetTable(SnapshotManager snapshotManager)
            throws IOException {
        long maxSnapshotId = Long.MIN_VALUE;
        long minSnapshotId = Long.MAX_VALUE;
        List<Snapshot> snapshots = snapshotManager.safelyGetAllSnapshots();
        for (Snapshot snapshot : snapshots) {
            maxSnapshotId = Math.max(maxSnapshotId, snapshot.id());
            minSnapshotId = Math.min(minSnapshotId, snapshot.id());
        }
        snapshotManager.commitEarliestHint(minSnapshotId);
        snapshotManager.commitLatestHint(maxSnapshotId);
    }

    @Override
    public void checkFileCount(
            int shouldCopyFileCount,
            int receivedFileCount,
            FileIO tableFileIO,
            Path tableRootPath) {
        // do nothing
    }
}
