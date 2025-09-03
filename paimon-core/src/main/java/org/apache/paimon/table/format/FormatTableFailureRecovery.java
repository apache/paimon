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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FormatTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Failure recovery manager for FormatTable operations. Handles cleanup of orphaned temporary files
 * and directories, and recovery from failed write operations.
 *
 * <p>This component is designed to run periodically or on-demand to maintain system health by
 * cleaning up resources from failed operations.
 */
@Public
public class FormatTableFailureRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableFailureRecovery.class);

    private final FormatTable formatTable;
    private final FileIO fileIO;

    // Default cleanup thresholds
    private static final long DEFAULT_TEMP_FILE_TTL_HOURS = 6; // 6 hours
    private static final long DEFAULT_COMMIT_METADATA_TTL_HOURS = 24; // 24 hours
    private static final int DEFAULT_MAX_CLEANUP_FILES = 1000; // Prevent excessive cleanup

    public FormatTableFailureRecovery(FormatTable formatTable) {
        this.formatTable = formatTable;
        this.fileIO = formatTable.fileIO();
    }

    /**
     * Performs comprehensive cleanup of orphaned resources.
     *
     * @return recovery statistics
     */
    public RecoveryStats performRecovery() throws IOException {
        return performRecovery(DEFAULT_TEMP_FILE_TTL_HOURS, DEFAULT_COMMIT_METADATA_TTL_HOURS);
    }

    /**
     * Performs comprehensive cleanup of orphaned resources with custom TTL.
     *
     * @param tempFileTtlHours TTL for temporary files in hours
     * @param commitMetadataTtlHours TTL for commit metadata in hours
     * @return recovery statistics
     */
    public RecoveryStats performRecovery(long tempFileTtlHours, long commitMetadataTtlHours)
            throws IOException {
        LOG.info("Starting failure recovery for format table: {}", formatTable.name());

        RecoveryStats stats = new RecoveryStats();
        Path tablePath = new Path(formatTable.location());

        if (!fileIO.exists(tablePath)) {
            LOG.debug("Table path does not exist: {}", tablePath);
            return stats;
        }

        long currentTime = System.currentTimeMillis();
        long tempFileCutoff = currentTime - TimeUnit.HOURS.toMillis(tempFileTtlHours);
        long commitMetadataCutoff = currentTime - TimeUnit.HOURS.toMillis(commitMetadataTtlHours);

        try {
            // Clean up temporary files and staging directories
            cleanupTemporaryFiles(tablePath, tempFileCutoff, stats);

            // Clean up orphaned commit metadata
            cleanupCommitMetadata(tablePath, commitMetadataCutoff, stats);

            // Clean up empty directories
            cleanupEmptyDirectories(tablePath, stats);

            LOG.info("Recovery completed for table {}: {}", formatTable.name(), stats);

        } catch (Exception e) {
            LOG.error("Recovery failed for table: {}", formatTable.name(), e);
            stats.errors++;
            throw new IOException("Recovery failed", e);
        }

        return stats;
    }

    /** Cleans up temporary files and staging directories older than the cutoff time. */
    private void cleanupTemporaryFiles(Path tablePath, long cutoffTime, RecoveryStats stats)
            throws IOException {
        // Clean up staging directories
        Path stagingPath = new Path(tablePath, "_staging");
        if (fileIO.exists(stagingPath)) {
            cleanupStagingDirectory(stagingPath, cutoffTime, stats);
        }

        // Clean up legacy temp directories (old format)
        FileStatus[] tableContents = fileIO.listStatus(tablePath);
        if (tableContents != null) {
            for (FileStatus status : tableContents) {
                if (status.isDir() && status.getPath().getName().startsWith("_temp_")) {
                    if (status.getModificationTime() < cutoffTime) {
                        LOG.info("Cleaning up orphaned temp directory: {}", status.getPath());
                        fileIO.delete(status.getPath(), true);
                        stats.tempDirectoriesRemoved++;
                    }
                }
            }
        }
    }

    /** Cleans up staging directory contents. */
    private void cleanupStagingDirectory(Path stagingPath, long cutoffTime, RecoveryStats stats)
            throws IOException {
        FileStatus[] stagingContents = fileIO.listStatus(stagingPath);
        if (stagingContents == null) {
            return;
        }

        for (FileStatus status : stagingContents) {
            if (status.isDir() && status.getPath().getName().startsWith("_temp_")) {
                if (status.getModificationTime() < cutoffTime) {
                    LOG.info("Cleaning up orphaned staging directory: {}", status.getPath());
                    fileIO.delete(status.getPath(), true);
                    stats.tempDirectoriesRemoved++;
                }
            }
        }

        // Try to clean up empty staging directory
        try {
            FileStatus[] remainingContents = fileIO.listStatus(stagingPath);
            if (remainingContents.length == 0) {
                fileIO.delete(stagingPath, false);
                LOG.debug("Removed empty staging directory: {}", stagingPath);
            }
        } catch (Exception e) {
            LOG.debug("Could not remove staging directory (may not be empty): {}", stagingPath);
        }
    }

    /** Cleans up orphaned commit metadata directories. */
    private void cleanupCommitMetadata(Path tablePath, long cutoffTime, RecoveryStats stats)
            throws IOException {
        Path commitsPath = new Path(tablePath, "_commits");
        if (!fileIO.exists(commitsPath)) {
            return;
        }

        FileStatus[] commitDirs = fileIO.listStatus(commitsPath);
        if (commitDirs == null) {
            return;
        }

        for (FileStatus commitDir : commitDirs) {
            if (commitDir.isDir() && commitDir.getModificationTime() < cutoffTime) {
                LOG.info("Cleaning up orphaned commit metadata: {}", commitDir.getPath());
                fileIO.delete(commitDir.getPath(), true);
                stats.commitMetadataRemoved++;
            }
        }

        // Try to clean up empty commits directory
        try {
            FileStatus[] remainingCommits = fileIO.listStatus(commitsPath);
            if (remainingCommits.length == 0) {
                fileIO.delete(commitsPath, false);
                LOG.debug("Removed empty commits directory: {}", commitsPath);
            }
        } catch (Exception e) {
            LOG.debug("Could not remove commits directory (may not be empty): {}", commitsPath);
        }
    }

    /** Cleans up empty directories that may have been left behind. */
    private void cleanupEmptyDirectories(Path tablePath, RecoveryStats stats) throws IOException {
        try {
            // This is a best-effort operation
            cleanupEmptyDirectoriesRecursive(tablePath, stats, 0);
        } catch (Exception e) {
            LOG.debug("Error during empty directory cleanup", e);
            stats.errors++;
        }
    }

    private void cleanupEmptyDirectoriesRecursive(Path directory, RecoveryStats stats, int depth)
            throws IOException {
        // Prevent infinite recursion
        if (depth > 10) {
            return;
        }

        FileStatus[] contents = fileIO.listStatus(directory);
        if (contents == null) {
            return;
        }

        // First, recurse into subdirectories
        for (FileStatus status : contents) {
            if (status.isDir() && !status.getPath().getName().startsWith("_")) {
                cleanupEmptyDirectoriesRecursive(status.getPath(), stats, depth + 1);
            }
        }

        // Then check if this directory is now empty (excluding system directories)
        contents = fileIO.listStatus(directory);
        if (contents != null) {
            boolean hasUserFiles = false;
            for (FileStatus status : contents) {
                if (!status.getPath().getName().startsWith("_")) {
                    hasUserFiles = true;
                    break;
                }
            }

            if (!hasUserFiles && !directory.equals(new Path(formatTable.location()))) {
                try {
                    fileIO.delete(directory, false);
                    stats.emptyDirectoriesRemoved++;
                    LOG.debug("Removed empty directory: {}", directory);
                } catch (Exception e) {
                    LOG.debug("Could not remove directory (may not be empty): {}", directory);
                }
            }
        }
    }

    /** Statistics from recovery operations. */
    public static class RecoveryStats {
        public int tempDirectoriesRemoved = 0;
        public int commitMetadataRemoved = 0;
        public int emptyDirectoriesRemoved = 0;
        public int errors = 0;

        @Override
        public String toString() {
            return String.format(
                    "RecoveryStats{tempDirs=%d, commitMetadata=%d, emptyDirs=%d, errors=%d}",
                    tempDirectoriesRemoved, commitMetadataRemoved, emptyDirectoriesRemoved, errors);
        }
    }

    /**
     * Checks if recovery is needed for a format table. This is a lightweight operation that can be
     * used to determine if full recovery should be run.
     */
    public boolean isRecoveryNeeded() throws IOException {
        Path tablePath = new Path(formatTable.location());
        if (!fileIO.exists(tablePath)) {
            return false;
        }

        // Check for staging directories
        Path stagingPath = new Path(tablePath, "_staging");
        if (fileIO.exists(stagingPath)) {
            return true;
        }

        // Check for orphaned temp directories
        FileStatus[] contents = fileIO.listStatus(tablePath);
        if (contents != null) {
            for (FileStatus status : contents) {
                if (status.isDir() && status.getPath().getName().startsWith("_temp_")) {
                    return true;
                }
            }
        }

        // Check for commit metadata
        Path commitsPath = new Path(tablePath, "_commits");
        if (fileIO.exists(commitsPath)) {
            return true;
        }

        return false;
    }
}
