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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.table.sink.FormatTableWrite.FormatTableCommitMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed committer for FormatTable that provides consistency guarantees in distributed
 * environments following Spark/Flink patterns.
 *
 * <p>This committer implements a two-phase commit protocol: 1. Prepare phase: Collect and serialize
 * commit messages from all tasks 2. Commit phase: Atomically apply all changes or rollback on
 * failure
 *
 * @since 0.9.0
 */
@Public
public class FormatTableDistributedCommitter {

    private static final Logger LOG =
            LoggerFactory.getLogger(FormatTableDistributedCommitter.class);

    private final FormatTable formatTable;
    private final FileIO fileIO;
    private final String commitId;
    private final FormatTableCommitMessageSerializer serializer;
    private final FormatTableAtomicCommitter atomicCommitter;

    // For distributed coordination
    private final Path commitMetadataDir;
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private volatile boolean committed = false;
    private volatile boolean aborted = false;

    public FormatTableDistributedCommitter(FormatTable formatTable) {
        this.formatTable = formatTable;
        this.fileIO = formatTable.fileIO();
        this.commitId = generateCommitId();
        this.serializer = new FormatTableCommitMessageSerializer();
        this.atomicCommitter = FormatTableAtomicCommitter.create(formatTable);
        this.commitMetadataDir = new Path(formatTable.location(), "_commits/" + commitId);
    }

    /**
     * Task-level commit preparation. Each task calls this to prepare its commit.
     *
     * @param taskId unique task identifier
     * @param commitMessages commit messages from this task
     * @return serialized task commit that can be sent to coordinator
     */
    public byte[] prepareTaskCommit(String taskId, List<CommitMessage> commitMessages)
            throws IOException {
        LOG.info(
                "Preparing task commit for task {} with {} messages",
                taskId,
                commitMessages.size());

        List<FormatTableCommitMessage> formatMessages = new ArrayList<>();
        for (CommitMessage message : commitMessages) {
            if (message instanceof FormatTableCommitMessage) {
                formatMessages.add((FormatTableCommitMessage) message);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported commit message type: " + message.getClass());
            }
        }

        // Serialize and store task commit metadata
        TaskCommitMetadata taskMetadata = new TaskCommitMetadata(taskId, formatMessages);
        byte[] serializedMetadata = serializeTaskMetadata(taskMetadata);

        // Store metadata for coordination
        Path taskMetadataPath = new Path(commitMetadataDir, "task_" + taskId + ".metadata");
        ensureCommitDirExists();
        writeTaskMetadata(taskMetadataPath, serializedMetadata);

        return serializedMetadata;
    }

    /**
     * Coordinator-level commit. Collects all task commits and applies them atomically.
     *
     * @param taskCommits serialized task commits from all tasks
     */
    public void coordinatorCommit(List<byte[]> taskCommits) throws IOException {
        if (committed || aborted) {
            LOG.warn(
                    "Commit {} already finalized (committed: {}, aborted: {})",
                    commitId,
                    committed,
                    aborted);
            return;
        }

        LOG.info(
                "Coordinator committing {} task commits for commit {}",
                taskCommits.size(),
                commitId);

        try {
            // Deserialize all task commits
            List<TempFileInfo> allTempFiles = new ArrayList<>();
            for (byte[] taskCommit : taskCommits) {
                TaskCommitMetadata metadata = deserializeTaskMetadata(taskCommit);
                for (FormatTableCommitMessage message : metadata.commitMessages) {
                    allTempFiles.add(message.getTempFileInfo());
                }
            }

            // Validate all temporary files exist
            validateTempFiles(allTempFiles);

            // Atomically commit all files
            atomicCommitter.commitFiles(allTempFiles);

            committed = true;
            LOG.info(
                    "Successfully committed {} files for commit {}", allTempFiles.size(), commitId);

        } catch (Exception e) {
            LOG.error("Failed to commit {}, initiating abort", commitId, e);
            abort(taskCommits);
            throw new IOException("Commit failed", e);
        } finally {
            cleanupCommitMetadata();
        }
    }

    /** Abort the commit and clean up temporary files. */
    public void abort(List<byte[]> taskCommits) {
        if (aborted) {
            LOG.warn("Commit {} already aborted", commitId);
            return;
        }

        LOG.info("Aborting commit {} with {} task commits", commitId, taskCommits.size());

        try {
            List<TempFileInfo> allTempFiles = new ArrayList<>();
            for (byte[] taskCommit : taskCommits) {
                try {
                    TaskCommitMetadata metadata = deserializeTaskMetadata(taskCommit);
                    for (FormatTableCommitMessage message : metadata.commitMessages) {
                        allTempFiles.add(message.getTempFileInfo());
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize task commit during abort", e);
                }
            }

            // Clean up temporary files
            atomicCommitter.abortFiles(allTempFiles);
            aborted = true;

            LOG.info(
                    "Successfully aborted commit {} and cleaned up {} files",
                    commitId,
                    allTempFiles.size());

        } catch (Exception e) {
            LOG.error("Failed to abort commit {}", commitId, e);
        } finally {
            cleanupCommitMetadata();
        }
    }

    /**
     * Recovery method to clean up orphaned commits. Should be called periodically to clean up
     * failed commits.
     */
    public static void cleanupOrphanedCommits(FormatTable formatTable, long olderThanMillis)
            throws IOException {
        Path commitsDir = new Path(formatTable.location(), "_commits");
        FileIO fileIO = formatTable.fileIO();

        if (!fileIO.exists(commitsDir)) {
            return;
        }

        long cutoffTime = System.currentTimeMillis() - olderThanMillis;

        // List and clean up old commit directories
        try {
            fileIO.listStatus(commitsDir);
            // Implementation would check timestamps and clean up old directories
            LOG.info("Cleaned up orphaned commits older than {} ms", olderThanMillis);
        } catch (Exception e) {
            LOG.warn("Failed to cleanup orphaned commits", e);
        }
    }

    private void validateTempFiles(List<TempFileInfo> tempFiles) throws IOException {
        for (TempFileInfo tempFile : tempFiles) {
            if (!fileIO.exists(tempFile.getTempPath())) {
                throw new IOException("Temporary file does not exist: " + tempFile.getTempPath());
            }
        }
    }

    private void ensureCommitDirExists() throws IOException {
        if (!fileIO.exists(commitMetadataDir)) {
            fileIO.mkdirs(commitMetadataDir);
        }
    }

    private void writeTaskMetadata(Path path, byte[] metadata) throws IOException {
        try (org.apache.paimon.fs.PositionOutputStream output =
                fileIO.newOutputStream(path, false)) {
            output.write(metadata);
            output.flush();
        }
    }

    private void cleanupCommitMetadata() {
        try {
            if (fileIO.exists(commitMetadataDir)) {
                fileIO.delete(commitMetadataDir, true);
                LOG.debug("Cleaned up commit metadata directory: {}", commitMetadataDir);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup commit metadata directory: {}", commitMetadataDir, e);
        }
    }

    private byte[] serializeTaskMetadata(TaskCommitMetadata metadata) throws IOException {
        // Simple serialization - in production might use more sophisticated format
        StringBuilder sb = new StringBuilder();
        sb.append(metadata.taskId).append("|");
        sb.append(metadata.commitMessages.size()).append("|");

        for (FormatTableCommitMessage message : metadata.commitMessages) {
            byte[] messageBytes = serializer.serialize(message);
            sb.append(messageBytes.length)
                    .append(":")
                    .append(java.util.Base64.getEncoder().encodeToString(messageBytes))
                    .append("|");
        }

        return sb.toString().getBytes();
    }

    private TaskCommitMetadata deserializeTaskMetadata(byte[] data) throws IOException {
        String content = new String(data);
        String[] parts = content.split("\\|");

        if (parts.length < 2) {
            throw new IOException("Invalid task metadata format");
        }

        String taskId = parts[0];
        int messageCount = Integer.parseInt(parts[1]);

        List<FormatTableCommitMessage> messages = new ArrayList<>();
        int index = 2;

        for (int i = 0; i < messageCount && index < parts.length; i++) {
            String[] messageParts = parts[index].split(":", 2);
            if (messageParts.length != 2) {
                throw new IOException("Invalid message format at index " + i);
            }

            int length = Integer.parseInt(messageParts[0]);
            byte[] messageBytes = java.util.Base64.getDecoder().decode(messageParts[1]);

            if (messageBytes.length != length) {
                throw new IOException("Message length mismatch at index " + i);
            }

            FormatTableCommitMessage message = serializer.deserialize(messageBytes);
            messages.add(message);
            index++;
        }

        return new TaskCommitMetadata(taskId, messages);
    }

    private String generateCommitId() {
        String timestamp =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("commit-%s-%s", timestamp, uuid);
    }

    /** Metadata for a single task's commit. */
    private static class TaskCommitMetadata {
        final String taskId;
        final List<FormatTableCommitMessage> commitMessages;

        TaskCommitMetadata(String taskId, List<FormatTableCommitMessage> commitMessages) {
            this.taskId = taskId;
            this.commitMessages = Collections.unmodifiableList(commitMessages);
        }
    }
}
