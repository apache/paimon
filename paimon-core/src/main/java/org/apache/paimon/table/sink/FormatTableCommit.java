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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.table.sink.FormatTableWrite.FormatTableCommitMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BatchTableCommit} implementation for {@link FormatTable} that handles committing writes
 * to format tables (ORC, Parquet, CSV, JSON).
 *
 * @since 0.9.0
 */
@Public
public class FormatTableCommit implements BatchTableCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableCommit.class);

    // Configuration options
    private static final ConfigOption<Boolean> FAILURE_RECOVERY_ENABLED =
            ConfigOptions.key("format-table.failure-recovery.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable failure recovery for format tables");

    private static final ConfigOption<Boolean> DISTRIBUTED_COMMIT_ENABLED =
            ConfigOptions.key("format-table.distributed-commit.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable distributed commit for format tables");

    private final FormatTable formatTable;
    private final String commitUser;
    private final Map<String, String> staticPartition;
    private final FileIO fileIO;
    private final FormatTableAtomicCommitter atomicCommitter;

    private boolean ignoreEmptyCommit = true;

    public FormatTableCommit(
            FormatTable formatTable,
            String commitUser,
            @Nullable Map<String, String> staticPartition) {
        this.formatTable = formatTable;
        this.commitUser = commitUser;
        this.staticPartition = staticPartition;
        this.fileIO = formatTable.fileIO();
        this.atomicCommitter = FormatTableAtomicCommitter.create(formatTable);

        // Perform recovery check if enabled
        Options options = Options.fromMap(formatTable.options());
        boolean enableRecovery = options.get(FAILURE_RECOVERY_ENABLED);

        if (enableRecovery) {
            performRecoveryCheck();
        }
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        if (commitMessages.isEmpty() && ignoreEmptyCommit) {
            LOG.info("Ignoring empty commit for format table {}", formatTable.name());
            return;
        }

        try {
            // Handle overwrite mode
            if (staticPartition != null) {
                handleOverwrite(commitMessages);
            } else {
                handleAppend(commitMessages);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to commit format table write", e);
        }

        LOG.info(
                "Successfully committed {} files to format table {}",
                commitMessages.size(),
                formatTable.name());
    }

    @Override
    public void truncateTable() {
        try {
            Path tablePath = new Path(formatTable.location());

            if (fileIO.exists(tablePath)) {
                if (formatTable.partitionKeys().isEmpty()) {
                    // Non-partitioned table: delete all data files
                    deleteDataFiles(tablePath);
                } else {
                    // Partitioned table: delete all partition directories
                    deletePartitionDirectories(tablePath);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate format table", e);
        }

        LOG.info("Truncated format table {}", formatTable.name());
    }

    @Override
    public void close() throws Exception {
        // No resources to clean up for format table commit
    }

    @Override
    public void truncatePartitions(List<Map<String, String>> partitions) {
        try {
            // Delete data in specified partitions
            for (Map<String, String> partition : partitions) {
                String partitionKey = createPartitionKey(partition);
                deletePartitionData(partitionKey);
            }
            LOG.info(
                    "Truncated {} partitions for format table {}",
                    partitions.size(),
                    formatTable.name());
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate partitions", e);
        }
    }

    @Override
    public void updateStatistics(Statistics statistics) {
        // Format tables don't support statistics updates
        LOG.debug("Statistics update not supported for format table {}", formatTable.name());
    }

    @Override
    public FormatTableCommit withMetricRegistry(MetricRegistry registry) {
        // Format tables don't currently use metrics
        return this;
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        try {
            List<TempFileInfo> tempFiles = new ArrayList<>();

            // Collect temp files from commit messages
            for (CommitMessage message : commitMessages) {
                if (message instanceof FormatTableCommitMessage) {
                    FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                    tempFiles.add(formatMessage.getTempFileInfo());
                }
            }

            // Use atomic committer to clean up temporary files
            atomicCommitter.abortFiles(tempFiles);

            LOG.info(
                    "Aborted commit for format table {} and cleaned up {} temporary files",
                    formatTable.name(),
                    tempFiles.size());
        } catch (Exception e) {
            LOG.error("Failed to abort commit for format table " + formatTable.name(), e);
            throw new RuntimeException("Failed to abort commit", e);
        }
    }

    @Override
    public void compactManifests() {
        // Format tables don't have manifests to compact
        LOG.debug("Manifest compaction not applicable for format table {}", formatTable.name());
    }

    /** Set whether to ignore empty commits. */
    public FormatTableCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    private void handleOverwrite(List<CommitMessage> commitMessages) throws Exception {
        Set<String> affectedPartitions = new HashSet<>();
        List<TempFileInfo> tempFiles = new ArrayList<>();

        // Collect affected partitions and temp files from commit messages
        for (CommitMessage message : commitMessages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                affectedPartitions.add(formatMessage.getPartition());
                tempFiles.add(formatMessage.getTempFileInfo());
            }
        }

        // Delete existing data in affected partitions
        for (String partition : affectedPartitions) {
            deletePartitionData(partition);
        }

        // Atomically commit the new files
        atomicCommitter.commitFiles(tempFiles);

        LOG.info("Overwrite completed for partitions: {}", affectedPartitions);
    }

    private void handleAppend(List<CommitMessage> commitMessages) throws Exception {
        List<TempFileInfo> tempFiles = new ArrayList<>();

        // Collect temp files from commit messages
        for (CommitMessage message : commitMessages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                TempFileInfo tempFileInfo = formatMessage.getTempFileInfo();

                // Validate that the temporary file exists
                if (!fileIO.exists(tempFileInfo.getTempPath())) {
                    throw new IOException(
                            "Commit failed: temporary file does not exist: "
                                    + tempFileInfo.getTempPath());
                }

                tempFiles.add(tempFileInfo);
            }
        }

        // Atomically commit all files
        atomicCommitter.commitFiles(tempFiles);

        LOG.info("Append completed for {} files", commitMessages.size());
    }

    private void deletePartitionData(String partition) throws IOException {
        Path tablePath = new Path(formatTable.location());

        if ("default".equals(partition) && formatTable.partitionKeys().isEmpty()) {
            // Non-partitioned table
            deleteDataFiles(tablePath);
        } else {
            // Partitioned table
            Path partitionPath = getPartitionPath(partition);
            if (fileIO.exists(partitionPath)) {
                deleteDataFiles(partitionPath);
            }
        }
    }

    private void deleteDataFiles(Path directory) throws IOException {
        if (!fileIO.exists(directory)) {
            return;
        }

        FileStatus[] files = fileIO.listStatus(directory);
        if (files != null) {
            for (FileStatus file : files) {
                if (!file.isDir() && isDataFile(file.getPath())) {
                    fileIO.delete(file.getPath(), false);
                    LOG.debug("Deleted file: {}", file.getPath());
                }
            }
        }
    }

    private void deletePartitionDirectories(Path tablePath) throws IOException {
        FileStatus[] partitions = fileIO.listStatus(tablePath);
        if (partitions != null) {
            for (FileStatus partition : partitions) {
                if (partition.isDir()) {
                    fileIO.delete(partition.getPath(), true);
                    LOG.debug("Deleted partition directory: {}", partition.getPath());
                }
            }
        }
    }

    private Path getPartitionPath(String partition) {
        Path tablePath = new Path(formatTable.location());

        if ("default".equals(partition)) {
            return tablePath;
        }

        // Parse partition string back to path
        String[] partitionValues = partition.split(",");
        StringBuilder pathBuilder = new StringBuilder();

        for (int i = 0;
                i < Math.min(partitionValues.length, formatTable.partitionKeys().size());
                i++) {
            if (i > 0) {
                pathBuilder.append("/");
            }
            pathBuilder
                    .append(formatTable.partitionKeys().get(i))
                    .append("=")
                    .append(partitionValues[i]);
        }

        return new Path(tablePath, pathBuilder.toString());
    }

    private boolean isDataFile(Path path) {
        String fileName = path.getName();
        String extension = getExpectedExtension();

        return fileName.endsWith("." + extension)
                && !fileName.startsWith(".")
                && !fileName.startsWith("_");
    }

    private String getExpectedExtension() {
        switch (formatTable.format()) {
            case PARQUET:
                return "parquet";
            case ORC:
                return "orc";
            case CSV:
                return "csv";
            case JSON:
                return "json";
            default:
                return "data";
        }
    }

    private String createPartitionKey(Map<String, String> partition) {
        if (partition.isEmpty()) {
            return "default";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : partition.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append(entry.getValue());
            first = false;
        }
        return sb.toString();
    }

    /** Performs recovery check to clean up orphaned resources from previous failed operations. */
    private void performRecoveryCheck() {
        try {
            FormatTableFailureRecovery recovery = new FormatTableFailureRecovery(formatTable);
            if (recovery.isRecoveryNeeded()) {
                LOG.info(
                        "Recovery needed for format table {}, performing cleanup",
                        formatTable.name());
                FormatTableFailureRecovery.RecoveryStats stats = recovery.performRecovery();
                LOG.info("Recovery completed for format table {}: {}", formatTable.name(), stats);
            }
        } catch (Exception e) {
            LOG.warn("Failed to perform recovery check for format table {}", formatTable.name(), e);
            // Don't fail the commit operation due to recovery issues
        }
    }

    /** Enhanced commit method that supports distributed consistency if enabled. */
    public void commitWithDistributedConsistency(
            List<CommitMessage> commitMessages, String taskId) {
        Options options = Options.fromMap(formatTable.options());
        boolean useDistributedCommit = options.get(DISTRIBUTED_COMMIT_ENABLED);

        if (useDistributedCommit) {
            try {
                FormatTableDistributedCommitter distributedCommitter =
                        new FormatTableDistributedCommitter(formatTable);

                // Prepare task commit
                byte[] taskCommit = distributedCommitter.prepareTaskCommit(taskId, commitMessages);

                // In a real distributed environment, these would be collected by a coordinator
                List<byte[]> allTaskCommits = Collections.singletonList(taskCommit);

                // Coordinator commit
                distributedCommitter.coordinatorCommit(allTaskCommits);

                LOG.info(
                        "Distributed commit completed for format table {} task {}",
                        formatTable.name(),
                        taskId);

            } catch (Exception e) {
                LOG.error(
                        "Distributed commit failed for format table {} task {}",
                        formatTable.name(),
                        taskId,
                        e);
                throw new RuntimeException("Distributed commit failed", e);
            }
        } else {
            // Fall back to regular commit
            commit(commitMessages);
        }
    }
}
