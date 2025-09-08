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

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** {@link TableWrite} implementation for format table. */
public class FormatTableWrite implements InnerTableWrite, Restorable<List<State<InternalRow>>> {

    /** Filesystem types for atomic write strategies. */
    public enum FileSystemType {
        HDFS, // Local and HDFS filesystems
        S3, // S3 and S3-compatible filesystems
        OSS // Alibaba Cloud OSS
    }

    private final RowType rowType;
    private final FormatTableFileWrite write;
    private final RowPartitionKeyExtractor partitionKeyExtractor;

    private final int[] notNullFieldIndex;
    private final @Nullable DefaultValueRow defaultValueRow;

    // Atomic write support
    private final FileSystemType fileSystemType;
    private final List<FormatTableCommitMessage> pendingCommits;
    private final Path tempDirectory;
    private final FileIO fileIO;

    public FormatTableWrite(
            RowType rowType,
            FormatTableFileWrite write,
            RowPartitionKeyExtractor partitionKeyExtractor,
            boolean ignoreDelete) {
        this.rowType = rowType;
        this.write = write;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.fileIO = write.getFileIO();
        this.pendingCommits = new ArrayList<>();

        // Detect filesystem type from table root path
        Path tablePath = write.getPathFactory().root();
        this.fileSystemType = detectFileSystemType(tablePath);

        // Initialize temp directory for HDFS and configure FormatTableFileWrite
        if (fileSystemType == FileSystemType.HDFS) {
            String tempDirName = ".tmp_" + UUID.randomUUID().toString().replace("-", "");
            this.tempDirectory = new Path(tablePath, tempDirName);
            try {
                fileIO.mkdirs(tempDirectory);
                // Configure FormatTableFileWrite to use temp directory
                write.setTempDirectory(tempDirectory);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temp directory: " + tempDirectory, e);
            }
        } else {
            this.tempDirectory = null;
        }

        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
    }

    @Override
    public InnerTableWrite withWriteRestore(WriteRestore writeRestore) {
        this.write.withWriteRestore(writeRestore);
        return this;
    }

    @Override
    public FormatTableWrite withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    @Override
    public FormatTableWrite withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public BatchTableWrite withWriteType(RowType writeType) {
        write.withWriteType(writeType);
        return this;
    }

    @Override
    public FormatTableWrite withMemoryPool(MemorySegmentPool memoryPool) {
        write.withMemoryPool(memoryPool);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        return partitionKeyExtractor.partition(row);
    }

    @Override
    public void write(InternalRow row) throws Exception {
        checkNullability(row);
        row = wrapDefaultValue(row);
        BinaryRow partition = partitionKeyExtractor.partition(row);
        write.write(partition, row);
    }

    @Override
    public void close() throws Exception {
        try {
            // Cleanup temp directory for HDFS
            if (tempDirectory != null && fileIO.exists(tempDirectory)) {
                fileIO.delete(tempDirectory, true);
            }
        } catch (IOException e) {
            // Log error but don't fail close operation
            System.err.println("Failed to cleanup temp directory: " + tempDirectory);
        } finally {
            write.close();
        }
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        write.flush();

        // Collect files and prepare commit messages based on filesystem type
        switch (fileSystemType) {
            case HDFS:
                return prepareHdfsCommit();
            case S3:
                return prepareS3Commit();
            case OSS:
                return prepareOssCommit();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported filesystem type: " + fileSystemType);
        }
    }

    public void commit() throws Exception {
        List<CommitMessage> commitMessages = this.prepareCommit();

        // Execute atomic commit based on filesystem type
        for (CommitMessage message : commitMessages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                switch (formatMessage.getCommitType()) {
                    case RENAME:
                        commitHdfsFile(formatMessage);
                        break;
                    case S3_MULTIPART:
                        commitS3MultipartUpload(formatMessage);
                        break;
                    case OSS_MULTIPART:
                        commitOssMultipartUpload(formatMessage);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported commit type: " + formatMessage.getCommitType());
                }
            }
        }

        // Clear pending commits after successful commit
        pendingCommits.clear();
    }

    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
    }

    private InternalRow wrapDefaultValue(InternalRow row) {
        return defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public FormatTableWrite withMetricRegistry(MetricRegistry metricRegistry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBucket(InternalRow row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<State<InternalRow>> checkpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restore(List<State<InternalRow>> state) {
        throw new UnsupportedOperationException();
    }

    /** Detect filesystem type from path scheme. */
    private FileSystemType detectFileSystemType(Path path) {
        String scheme = path.toUri().getScheme();
        if (scheme == null || "file".equals(scheme) || "hdfs".equals(scheme)) {
            return FileSystemType.HDFS;
        } else if ("s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme)) {
            return FileSystemType.S3;
        } else if ("oss".equals(scheme)) {
            return FileSystemType.OSS;
        } else {
            // Default to HDFS for unknown schemes
            return FileSystemType.HDFS;
        }
    }

    /** Prepare commit messages for HDFS filesystem using temp files. */
    private List<CommitMessage> prepareHdfsCommit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();

        // Collect all temp files written by FormatTableFileWrite
        List<FormatTableFileInfo> tempFiles = write.getTempFileList();
        write.flush();
        for (FormatTableFileInfo fileInfo : tempFiles) {
            Path tempPath = fileInfo.getTempPath();
            Path targetPath = fileInfo.getTargetPath();
            BinaryRow partition = fileInfo.getPartition();

            Map<String, String> metadata = new HashMap<>();
            metadata.put("commit_type", "hdfs_rename");
            metadata.put("temp_file", tempPath.toString());
            metadata.put("target_file", targetPath.toString());

            FormatTableCommitMessage commitMessage =
                    new FormatTableCommitMessage(
                            partition,
                            0, // bucket not used for format table
                            targetPath,
                            tempPath,
                            fileIO.getFileSize(tempPath),
                            metadata);

            commitMessages.add(commitMessage);
        }

        return commitMessages;
    }

    /** Prepare commit messages for S3 filesystem using multipart uploads. */
    private List<CommitMessage> prepareS3Commit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();

        // For S3, files are written directly to target using multipart upload
        // The commit message contains the upload metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("commit_type", "s3_multipart");

        // This is a placeholder - real implementation would collect
        // multipart upload metadata (objectName, uploadId, partETags)

        return commitMessages;
    }

    /** Prepare commit messages for OSS filesystem using multipart uploads. */
    private List<CommitMessage> prepareOssCommit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();

        // For OSS, similar to S3 with multipart uploads
        Map<String, String> metadata = new HashMap<>();
        metadata.put("commit_type", "oss_multipart");

        // This is a placeholder - real implementation would collect
        // multipart upload metadata

        return commitMessages;
    }

    /** Commit HDFS file by renaming from temp to target location. */
    private void commitHdfsFile(FormatTableCommitMessage commitMessage) throws IOException {
        Path tempPath = commitMessage.getTempPath();
        Path targetPath = commitMessage.getFilePath();

        if (tempPath != null && fileIO.exists(tempPath)) {
            // Ensure target directory exists
            fileIO.mkdirs(targetPath.getParent());

            // Atomic rename from temp to target
            boolean renamed = fileIO.rename(tempPath, targetPath);
            if (!renamed) {
                throw new IOException(
                        "Failed to rename file from " + tempPath + " to " + targetPath);
            }
        }
    }

    /** Commit S3 multipart upload by calling completeMultipartUpload. */
    private void commitS3MultipartUpload(FormatTableCommitMessage commitMessage)
            throws IOException {
        String objectName = commitMessage.getObjectName();
        String uploadId = commitMessage.getUploadId();
        List<String> partETags = commitMessage.getPartETags();

        if (objectName == null || uploadId == null || partETags == null || partETags.isEmpty()) {
            throw new IOException("Invalid S3 multipart upload metadata in commit message");
        }

        // This is a placeholder for actual S3 SDK call
        // Real implementation would use AWS SDK:
        // s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
        //     .bucket(bucket)
        //     .key(objectName)
        //     .uploadId(uploadId)
        //     .multipartUpload(CompletedMultipartUpload.builder()
        //         .parts(convertPartETags(partETags))
        //         .build())
        //     .build());
    }

    /** Commit OSS multipart upload by calling completeMultipartUpload. */
    private void commitOssMultipartUpload(FormatTableCommitMessage commitMessage)
            throws IOException {
        String objectName = commitMessage.getObjectName();
        String uploadId = commitMessage.getUploadId();
        List<String> partETags = commitMessage.getPartETags();

        if (objectName == null || uploadId == null || partETags == null || partETags.isEmpty()) {
            throw new IOException("Invalid OSS multipart upload metadata in commit message");
        }

        // This is a placeholder for actual OSS SDK call
        // Real implementation would use Aliyun OSS SDK:
        // ossClient.completeMultipartUpload(new CompleteMultipartUploadRequest(
        //     bucketName, objectName, uploadId, convertPartETags(partETags)));
    }
}
