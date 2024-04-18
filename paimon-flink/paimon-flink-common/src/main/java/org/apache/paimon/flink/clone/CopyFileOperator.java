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
import org.apache.paimon.clone.CopyFileUtils;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An Operator to copy files. */
public class CopyFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFileOperator.class);

    private final FileIO sourceTableFileIO;
    private final FileIO targetTableFileIO;
    private final Path sourceTableRootPath;
    private final Path targetTableRootPath;

    public CopyFileOperator(FileStoreTable sourceTable, FileStoreTable targetTable) {
        super();
        this.sourceTableFileIO = sourceTable.fileIO();
        this.targetTableFileIO = targetTable.fileIO();
        this.sourceTableRootPath = sourceTable.location();
        this.targetTableRootPath = targetTable.location();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        CloneFileInfo cloneFileInfo = streamRecord.getValue();

        // snapshot or tag file coped in latest operator(CheckCloneResultAndCopySnapshotOperator)
        if (cloneFileInfo.isSnapshotOrTagFile()) {
            output.collect(streamRecord);
            return;
        }

        try {
            CopyFileUtils.copyFile(
                    cloneFileInfo,
                    sourceTableFileIO,
                    targetTableFileIO,
                    sourceTableRootPath,
                    targetTableRootPath);
        } catch (Exception e) {
            LOG.warn(
                    "The source file {} is expiring and thus is deleted. ignored it.",
                    new Path(
                            sourceTableRootPath.toString()
                                    + cloneFileInfo.getFilePathExcludeTableRoot()));
            return;
        }

        output.collect(streamRecord);
    }
}
