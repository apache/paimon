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
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Define behavior for different clone types. */
public interface CloneTypeAction extends Serializable {

    /**
     * Find the files to be copied in different cloneType.
     *
     * @return all cloneFileInfo to be copied
     */
    List<CloneFileInfo> findCloneFilePaths(
            Map<String, Pair<Path, Long>> tableAllFilesExcludeTableRoot,
            SnapshotManager snapshotManager,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler,
            TagManager tagManager);

    /**
     * commit Snapshot earliestHint and latestHint in targetTable in different cloneTypes.
     *
     * @throws IOException
     */
    void commitSnapshotHintInTargetTable(SnapshotManager snapshotManager) throws IOException;

    /**
     * check whether file deleted when clone.
     *
     * @param oldFileSize
     * @param targetFilePath
     * @param tableFileIO
     * @param tableRootPath
     */
    void checkFileSize(
            long oldFileSize, Path targetFilePath, FileIO tableFileIO, Path tableRootPath)
            throws IOException;

    /**
     * check whether file size dismatch when clone.
     *
     * @param shouldCopyFileCount
     * @param receivedFileCount
     * @param tableFileIO
     * @param tableRootPath
     */
    void checkFileCount(
            int shouldCopyFileCount, int receivedFileCount, FileIO tableFileIO, Path tableRootPath);
}
