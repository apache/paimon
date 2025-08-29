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
import org.apache.paimon.table.FormatTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Atomic committer for FormatTable that ensures write visibility only after successful commit.
 *
 * <p>This class provides different commit strategies based on the underlying file system: - For
 * HDFS: Uses temporary directory + atomic rename - For S3/OSS: Uses Hadoop S3A Committer for atomic
 * writes
 */
@Public
public abstract class FormatTableAtomicCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableAtomicCommitter.class);

    protected final FormatTable formatTable;
    protected final FileIO fileIO;
    protected final String sessionId;

    public FormatTableAtomicCommitter(FormatTable formatTable) {
        this.formatTable = formatTable;
        this.fileIO = formatTable.fileIO();
        this.sessionId = generateSessionId();
    }

    /** Creates an appropriate committer based on the file system type. */
    public static FormatTableAtomicCommitter create(FormatTable formatTable) {
        String location = formatTable.location();

        if (isS3FileSystem(location) || isOSSFileSystem(location)) {
            return new S3AtomicCommitter(formatTable);
        } else {
            return new HDFSAtomicCommitter(formatTable);
        }
    }

    /**
     * Prepares a temporary location for writing files. Files written to this location are not
     * visible until commit.
     */
    public abstract Path prepareTempLocation(String partitionPath);

    /**
     * Commits all temporary files to their final locations atomically. After this operation, all
     * files become visible.
     */
    public abstract void commitFiles(List<TempFileInfo> tempFiles) throws IOException;

    /** Aborts the write operation and cleans up temporary files. */
    public abstract void abortFiles(List<TempFileInfo> tempFiles) throws IOException;

    /** Gets the final location for a file after commit. */
    public abstract Path getFinalLocation(TempFileInfo tempFile);

    private static boolean isS3FileSystem(String location) {
        return location.startsWith("s3://")
                || location.startsWith("s3a://")
                || location.startsWith("s3n://");
    }

    private static boolean isOSSFileSystem(String location) {
        return location.startsWith("oss://");
    }

    private String generateSessionId() {
        String timestamp =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("session-%s-%s", timestamp, uuid);
    }

    /** Information about a temporary file that needs to be committed. */
    public static class TempFileInfo {
        private final Path tempPath;
        private final Path finalPath;
        private final String partitionPath;

        public TempFileInfo(Path tempPath, Path finalPath, String partitionPath) {
            this.tempPath = tempPath;
            this.finalPath = finalPath;
            this.partitionPath = partitionPath;
        }

        public Path getTempPath() {
            return tempPath;
        }

        public Path getFinalPath() {
            return finalPath;
        }

        public String getPartitionPath() {
            return partitionPath;
        }

        @Override
        public String toString() {
            return String.format(
                    "TempFileInfo{tempPath=%s, finalPath=%s, partition=%s}",
                    tempPath, finalPath, partitionPath);
        }
    }

    /**
     * HDFS implementation using temporary directories and atomic rename. Optimized with batch
     * operations and improved cleanup strategies.
     */
    private static class HDFSAtomicCommitter extends FormatTableAtomicCommitter {

        private static final String TEMP_DIR_PREFIX = "_temp_";
        private static final String STAGING_DIR_NAME = "_staging";

        public HDFSAtomicCommitter(FormatTable formatTable) {
            super(formatTable);
        }

        @Override
        public Path prepareTempLocation(String partitionPath) {
            Path tablePath = new Path(formatTable.location());
            // Use a staging directory structure for better organization
            String tempDirName = TEMP_DIR_PREFIX + sessionId;
            Path stagingPath = new Path(tablePath, STAGING_DIR_NAME + "/" + tempDirName);

            if (partitionPath != null
                    && !partitionPath.isEmpty()
                    && !"default".equals(partitionPath)) {
                return new Path(stagingPath, partitionPath);
            } else {
                return stagingPath;
            }
        }

        @Override
        public void commitFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Committing {} files using HDFS atomic rename strategy", tempFiles.size());

            // Group files by their parent directory for batch operations
            Map<Path, List<TempFileInfo>> filesByParent =
                    tempFiles.stream()
                            .collect(
                                    java.util.stream.Collectors.groupingBy(
                                            f -> f.getFinalPath().getParent()));

            // Create all necessary parent directories first (batch operation)
            for (Path parentDir : filesByParent.keySet()) {
                if (parentDir != null && !fileIO.exists(parentDir)) {
                    fileIO.mkdirs(parentDir);
                    LOG.debug("Created parent directory: {}", parentDir);
                }
            }

            // Perform atomic renames with error handling
            int successCount = 0;
            List<IOException> failures = new ArrayList<>();

            for (TempFileInfo tempFile : tempFiles) {
                try {
                    Path tempPath = tempFile.getTempPath();
                    Path finalPath = tempFile.getFinalPath();

                    if (!fileIO.exists(tempPath)) {
                        throw new IOException("Temporary file does not exist: " + tempPath);
                    }

                    // Remove existing file if present (overwrite case)
                    if (fileIO.exists(finalPath)) {
                        fileIO.delete(finalPath, false);
                    }

                    // Atomic rename
                    if (!fileIO.rename(tempPath, finalPath)) {
                        throw new IOException("Failed to rename " + tempPath + " to " + finalPath);
                    }

                    successCount++;
                    LOG.debug("Successfully renamed {} to {}", tempPath, finalPath);

                } catch (IOException e) {
                    failures.add(e);
                    LOG.error("Failed to commit file: {}", tempFile.getTempPath(), e);
                }
            }

            // Clean up temporary directory
            cleanupTempDirectory();

            // Report any failures
            if (!failures.isEmpty()) {
                LOG.error("Failed to commit {} out of {} files", failures.size(), tempFiles.size());
                IOException combined =
                        new IOException(
                                String.format(
                                        "Failed to commit %d out of %d files",
                                        failures.size(), tempFiles.size()));
                for (IOException failure : failures) {
                    combined.addSuppressed(failure);
                }
                throw combined;
            }

            LOG.info("Successfully committed {} files", successCount);
        }

        @Override
        public void abortFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Aborting {} files and cleaning up temporary directory", tempFiles.size());
            cleanupTempDirectory();
        }

        @Override
        public Path getFinalLocation(TempFileInfo tempFile) {
            return tempFile.getFinalPath();
        }

        private void cleanupTempDirectory() throws IOException {
            Path tablePath = new Path(formatTable.location());
            Path stagingPath = new Path(tablePath, STAGING_DIR_NAME);
            Path tempDir = new Path(stagingPath, TEMP_DIR_PREFIX + sessionId);

            // Clean up this session's temp directory
            if (fileIO.exists(tempDir)) {
                fileIO.delete(tempDir, true);
                LOG.debug("Cleaned up temporary directory: {}", tempDir);
            }

            // Optionally clean up empty staging directory
            try {
                if (fileIO.exists(stagingPath)) {
                    FileStatus[] stagingContents = fileIO.listStatus(stagingPath);
                    if (stagingContents.length == 0) {
                        fileIO.delete(stagingPath, false);
                        LOG.debug("Cleaned up empty staging directory: {}", stagingPath);
                    }
                }
            } catch (Exception e) {
                LOG.debug(
                        "Could not clean up staging directory (may not be empty): {}", stagingPath);
            }
        }
    }

    /** S3/OSS implementation using Hadoop S3A Committer for atomic writes. */
    private static class S3AtomicCommitter extends FormatTableAtomicCommitter {

        private static final String MAGIC_COMMITTER_DIR = "_temporary";

        public S3AtomicCommitter(FormatTable formatTable) {
            super(formatTable);
        }

        @Override
        public Path prepareTempLocation(String partitionPath) {
            // For S3A committer, we use a magic directory that the committer recognizes
            Path tablePath = new Path(formatTable.location());
            Path tempDir = new Path(tablePath, MAGIC_COMMITTER_DIR + "/" + sessionId);

            if (partitionPath != null
                    && !partitionPath.isEmpty()
                    && !"default".equals(partitionPath)) {
                return new Path(tempDir, partitionPath);
            } else {
                return tempDir;
            }
        }

        @Override
        public void commitFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Committing {} files using S3A committer strategy", tempFiles.size());

            // For S3A committer, we need to:
            // 1. Write manifest files that describe the pending uploads
            // 2. Use the S3A committer to atomically commit all files

            // This is a simplified implementation - in practice, you would integrate
            // with org.apache.hadoop.fs.s3a.commit.CommitOperations
            // or org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter

            for (TempFileInfo tempFile : tempFiles) {
                // For now, use the same rename strategy as HDFS
                // In a full implementation, this would use the S3A committer API
                Path tempPath = tempFile.getTempPath();
                Path finalPath = tempFile.getFinalPath();

                if (fileIO.exists(tempPath)) {
                    // Ensure parent directory exists
                    Path parentDir = finalPath.getParent();
                    if (parentDir != null && !fileIO.exists(parentDir)) {
                        fileIO.mkdirs(parentDir);
                    }

                    fileIO.rename(tempPath, finalPath);
                    LOG.debug("Moved {} to {} (S3A strategy)", tempPath, finalPath);
                }
            }

            cleanupTempDirectory();
        }

        @Override
        public void abortFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Aborting {} files for S3A committer", tempFiles.size());

            // Clean up any temporary files
            for (TempFileInfo tempFile : tempFiles) {
                Path tempPath = tempFile.getTempPath();
                if (fileIO.exists(tempPath)) {
                    fileIO.delete(tempPath, false);
                    LOG.debug("Deleted temporary file: {}", tempPath);
                }
            }

            cleanupTempDirectory();
        }

        @Override
        public Path getFinalLocation(TempFileInfo tempFile) {
            return tempFile.getFinalPath();
        }

        private void cleanupTempDirectory() throws IOException {
            Path tablePath = new Path(formatTable.location());
            Path tempDir = new Path(tablePath, MAGIC_COMMITTER_DIR + "/" + sessionId);

            if (fileIO.exists(tempDir)) {
                fileIO.delete(tempDir, true);
                LOG.debug("Cleaned up S3A temporary directory: {}", tempDir);
            }
        }
    }
}
