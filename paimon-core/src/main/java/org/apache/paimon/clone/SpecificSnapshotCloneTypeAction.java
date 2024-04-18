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

import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.PickFilesUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;

/** The action of SpecificSnapshot Clone. */
public class SpecificSnapshotCloneTypeAction extends AbstractCloneTypeAction {

    private final Long snapshotId;

    public SpecificSnapshotCloneTypeAction(Long snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public List<CloneFileInfo> findCloneFilePaths(
            Map<String, Pair<Path, Long>> tableAllFilesExcludeTableRoot,
            SnapshotManager snapshotManager,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler,
            TagManager tagManager) {
        return getFilePathsForSnapshotOrTag(
                PickFilesUtil.getUsedFilesForSnapshot(
                        snapshotManager.snapshot(snapshotId),
                        snapshotManager,
                        manifestList,
                        manifestFile,
                        schemaManager,
                        indexFileHandler),
                tableAllFilesExcludeTableRoot,
                fileName -> fileName.startsWith(SNAPSHOT_PREFIX));
    }
}
