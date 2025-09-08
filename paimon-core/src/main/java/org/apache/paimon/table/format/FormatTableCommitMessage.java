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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Commit message for format table atomic operations. Contains metadata about files that were
 * committed atomically, including multipart upload information for object stores.
 */
public class FormatTableCommitMessage implements CommitMessage {

    private static final long serialVersionUID = 1L;

    private final BinaryRow partition;
    private final int bucket;
    private final Path filePath;
    private final long fileSize;
    private final CommitType commitType;

    // For multipart uploads (S3/OSS)
    private final String objectName;
    private final String uploadId;
    private final List<String> partETags;

    // For HDFS temp files
    private final Path tempPath;

    // Additional metadata
    private final Map<String, String> metadata;

    /** Type of commit operation performed. */
    public enum CommitType {
        /** Standard rename-based commit for HDFS. */
        RENAME,
        /** S3 multipart upload commit. */
        S3_MULTIPART,
        /** OSS multipart upload commit. */
        OSS_MULTIPART
    }

    // Constructor for HDFS rename-based commits
    public FormatTableCommitMessage(
            BinaryRow partition,
            int bucket,
            Path filePath,
            Path tempPath,
            long fileSize,
            Map<String, String> metadata) {
        this.partition = partition;
        this.bucket = bucket;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.commitType = CommitType.RENAME;
        this.tempPath = tempPath;
        this.objectName = null;
        this.uploadId = null;
        this.partETags = null;
        this.metadata = metadata;
    }

    // Constructor for S3/OSS multipart upload commits
    public FormatTableCommitMessage(
            BinaryRow partition,
            int bucket,
            Path filePath,
            long fileSize,
            CommitType commitType,
            String objectName,
            String uploadId,
            List<String> partETags,
            Map<String, String> metadata) {
        this.partition = partition;
        this.bucket = bucket;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.commitType = commitType;
        this.objectName = objectName;
        this.uploadId = uploadId;
        this.partETags = partETags;
        this.tempPath = null;
        this.metadata = metadata;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public @Nullable Integer totalBuckets() {
        return null;
    }

    public Path getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public CommitType getCommitType() {
        return commitType;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getUploadId() {
        return uploadId;
    }

    public List<String> getPartETags() {
        return partETags;
    }

    public Path getTempPath() {
        return tempPath;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return String.format(
                "FormatTableCommitMessage{partition=%s, bucket=%d, filePath=%s, fileSize=%d, commitType=%s}",
                partition, bucket, filePath, fileSize, commitType);
    }
}
