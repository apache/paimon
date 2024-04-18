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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.TagManager.TAG_PREFIX;

/** The action of tag clone. */
public class TagCloneTypeAction extends AbstractCloneTypeAction {

    private final String tagName;

    public TagCloneTypeAction(String tagName) {
        this.tagName = tagName;
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
        checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);
        return getFilePathsForSnapshotOrTag(
                PickFilesUtil.getUsedFilesForTag(
                        tagManager.taggedSnapshot(tagName),
                        tagManager,
                        tagName,
                        manifestList,
                        manifestFile,
                        schemaManager,
                        indexFileHandler),
                tableAllFilesExcludeTableRoot,
                fileName -> fileName.startsWith(TAG_PREFIX));
    }

    @Override
    public void commitSnapshotHintInTargetTable(SnapshotManager snapshotManager)
            throws IOException {
        // do nothing, because after clone job, target table only table a tag, has no snapshot
    }
}
