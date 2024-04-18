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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Define some common funtion for different clone types. */
public abstract class AbstractCloneTypeAction implements CloneTypeAction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCloneTypeAction.class);

    /**
     * @param fileNames all fileNames of a snapshot or tag
     * @param tableAllFiles all files of a table. path is the relative path
     * @param snapshotOrTagFile filter by snapshot or tag file
     * @return
     */
    protected List<CloneFileInfo> getFilePathsForSnapshotOrTag(
            List<String> fileNames,
            Map<String, Pair<Path, Long>> tableAllFiles,
            Predicate<String> snapshotOrTagFile) {
        List<CloneFileInfo> cloneFileInfos = new ArrayList<>();

        for (String fileName : fileNames) {
            Pair<Path, Long> filePathAndSize = tableAllFiles.get(fileName);
            checkNotNull(
                    filePathAndSize,
                    "Error this is a bug, please report. fileName is "
                            + fileName
                            + ", tableAllFiles is : "
                            + tableAllFiles);
            cloneFileInfos.add(
                    new CloneFileInfo(
                            filePathAndSize.getKey(),
                            filePathAndSize.getValue(),
                            snapshotOrTagFile.test(fileName)));
        }
        return cloneFileInfos;
    }

    @Override
    public void commitSnapshotHintInTargetTable(SnapshotManager snapshotManager)
            throws IOException {
        checkState(
                1 == snapshotManager.snapshotCount(),
                "snapshot count is not equal to 1 "
                        + "when cloneType is LatestSnapshot / SpecificSnapshot / FromTimestamp.");
        long snapshotId = snapshotManager.safelyGetAllSnapshots().get(0).id();
        snapshotManager.commitEarliestHint(snapshotId);
        snapshotManager.commitLatestHint(snapshotId);
    }

    @Override
    public void checkFileSize(
            long oldFileSize, Path targetFilePath, FileIO tableFileIO, Path tableRootPath)
            throws IOException {
        if (oldFileSize != tableFileIO.getFileSize(targetFilePath)) {
            String errorMsg =
                    "Copy file error. Flink batch job go to failover. clone file origin size is : "
                            + oldFileSize
                            + ", target file path is : "
                            + targetFilePath
                            + ", and target file size is  : "
                            + tableFileIO.getFileSize(targetFilePath);
            checkFileInternal(errorMsg, tableFileIO, tableRootPath);
        }
    }

    @Override
    public void checkFileCount(
            int shouldCopyFileCount,
            int receivedFileCount,
            FileIO tableFileIO,
            Path tableRootPath) {
        if (shouldCopyFileCount != receivedFileCount) {
            String errorMsg =
                    "Copy file error. Flink batch job go to failover."
                            + " shouldCopyFileCount should be equal to receivedFileCount"
                            + " when cloneType is LatestSnapshot / SpecificSnapshot / FromTimestamp / Tag.";
            checkFileInternal(errorMsg, tableFileIO, tableRootPath);
        }
    }

    private void checkFileInternal(String errorMsg, FileIO tableFileIO, Path tableRootPath) {
        LOG.error(errorMsg);
        tableFileIO.deleteDirectoryQuietly(tableRootPath);
        throw new RuntimeException(errorMsg);
    }
}
