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

package org.apache.paimon.flink.clone;

import org.apache.paimon.clone.CloneFileInfo;
import org.apache.paimon.clone.CloneTypeAction;
import org.apache.paimon.clone.CopyFileUtils;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An Operator to check copy files data size and final copy snapshot or tag file. */
public class CheckCloneResultAndCopySnapshotOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CheckCloneResultAndCopySnapshotOperator.class);

    private final FileIO sourceTableFileIO;
    private final FileIO targetTableFileIO;
    private final SnapshotManager targetTableSnapshotManager;
    private final Path sourceTableRootPath;
    private final Path targetTableRootPath;
    private final List<CloneFileInfo> snapshotOrTagFiles;
    private final CloneTypeAction cloneTypeAction;
    private final int shouldCopyFileCount;
    private int receivedFileCount;

    public CheckCloneResultAndCopySnapshotOperator(
            FileStoreTable sourceTable,
            FileStoreTable targetTable,
            CloneTypeAction cloneTypeAction,
            Integer shouldCopyFileCount) {
        super();
        this.sourceTableFileIO = sourceTable.fileIO();
        this.targetTableFileIO = targetTable.fileIO();
        this.targetTableSnapshotManager = targetTable.snapshotManager();
        this.sourceTableRootPath = sourceTable.location();
        this.targetTableRootPath = targetTable.location();
        this.snapshotOrTagFiles = new ArrayList<>();
        this.receivedFileCount = 0;
        this.cloneTypeAction = cloneTypeAction;
        this.shouldCopyFileCount = shouldCopyFileCount;
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        receivedFileCount++;
        output.collect(streamRecord);

        CloneFileInfo cloneFileInfo = streamRecord.getValue();
        if (cloneFileInfo.isSnapshotOrTagFile()) {
            snapshotOrTagFiles.add(cloneFileInfo);
            return;
        }

        Path targetFilePath =
                new Path(
                        targetTableRootPath.toString()
                                + cloneFileInfo.getFilePathExcludeTableRoot());

        LOG.debug(
                "Begin check the {} file's size, targetFilePath is {}.",
                receivedFileCount,
                targetFilePath);

        cloneTypeAction.checkFileSize(
                cloneFileInfo.getFileSize(),
                targetFilePath,
                targetTableFileIO,
                targetTableRootPath);
    }

    @Override
    public void finish() throws Exception {
        super.finish();

        cloneTypeAction.checkFileCount(
                shouldCopyFileCount, receivedFileCount, targetTableFileIO, targetTableRootPath);
        copySnapshotOrTag();
        cloneTypeAction.commitSnapshotHintInTargetTable(targetTableSnapshotManager);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (snapshotOrTagFiles != null) {
            snapshotOrTagFiles.clear();
        }
    }

    /**
     * Copy snapshot or tag in this latest operator of flink batch job.
     *
     * @throws IOException
     */
    private void copySnapshotOrTag() {
        for (CloneFileInfo cloneFileInfo : snapshotOrTagFiles) {
            try {
                CopyFileUtils.copyFile(
                        cloneFileInfo,
                        sourceTableFileIO,
                        targetTableFileIO,
                        sourceTableRootPath,
                        targetTableRootPath);
            } catch (Exception e) {
                LOG.warn(
                        "The source file {} is expiring and may be deleted. ignored it.",
                        new Path(
                                sourceTableRootPath.toString()
                                        + cloneFileInfo.getFilePathExcludeTableRoot()));
            }
        }
    }
}
