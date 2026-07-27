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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * A {@link TwoPhaseOutputStream} implementation that writes to a temporary file and commits by
 * renaming to the target path. This follows HDFS-style commit semantics.
 */
@Public
public class RenamingTwoPhaseOutputStream extends TwoPhaseOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(RenamingTwoPhaseOutputStream.class);

    private static final String TEMP_DIR_NAME = "_temporary";

    private final Path targetPath;
    private final Path tempPath;
    private final PositionOutputStream tempOutputStream;

    public RenamingTwoPhaseOutputStream(FileIO fileIO, Path targetPath, boolean overwrite)
            throws IOException {
        if (!overwrite && fileIO.exists(targetPath)) {
            throw new IOException("File " + targetPath + " already exists.");
        }
        this.targetPath = targetPath;
        this.tempPath = generateTempPath(targetPath);

        // Create temporary file
        this.tempOutputStream = fileIO.newOutputStream(tempPath, overwrite);
    }

    @Override
    public void write(int b) throws IOException {
        tempOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        tempOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        tempOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        tempOutputStream.flush();
    }

    @Override
    public long getPos() throws IOException {
        return tempOutputStream.getPos();
    }

    @Override
    public void close() throws IOException {
        tempOutputStream.close();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        close();
        return new TempFileCommitter(tempPath, targetPath);
    }

    /**
     * Generate a temporary file path based on the target path. The temp file will be in the same
     * directory as the target with a unique suffix.
     */
    private Path generateTempPath(Path targetPath) {
        String tempFileName = TEMP_DIR_NAME + "/.tmp." + UUID.randomUUID();
        return new Path(targetPath.getParent(), tempFileName);
    }

    /** Committer implementation that renames temporary file to target path. */
    private static class TempFileCommitter implements Committer {

        private static final long serialVersionUID = 1L;

        private final Path tempPath;
        private final Path targetPath;

        private TempFileCommitter(Path tempPath, Path targetPath) {
            this.tempPath = tempPath;
            this.targetPath = targetPath;
        }

        @Override
        public void commit(FileIO fileIO) throws IOException {
            Path parentDir = targetPath.getParent();
            if (parentDir != null && !fileIO.exists(parentDir)) {
                fileIO.mkdirs(parentDir);
            }
            if (!fileIO.rename(tempPath, targetPath)) {
                throw new IOException("Failed to rename " + tempPath + " to " + targetPath);
            }
            if (fileIO.exists(tempPath)) {
                fileIO.deleteQuietly(tempPath);
            }
        }

        @Override
        public void discard(FileIO fileIO) throws IOException {
            if (fileIO.exists(targetPath)) {
                fileIO.deleteQuietly(targetPath);
            }
            if (fileIO.exists(tempPath)) {
                fileIO.deleteQuietly(tempPath);
            }
        }

        @Override
        public Path targetPath() {
            return targetPath;
        }

        @Override
        public void clean(FileIO fileIO) {
            fileIO.deleteQuietly(tempPath);
            // '_temporary' is shared with every other writer of this directory, Paimon or not, so
            // it can only be removed once nothing is staged there any more: deleting it outright
            // would throw away the pending files of jobs that are still running.
            Path stagingDir = tempPath.getParent();
            if (stagingDir == null) {
                return;
            }
            try {
                FileStatus[] staged = fileIO.listStatus(stagingDir);
                if (staged != null && staged.length == 0) {
                    fileIO.deleteQuietly(stagingDir);
                }
            } catch (IOException e) {
                // Already gone, or not listable: an empty staging directory left behind is
                // skipped by readers anyway.
                LOG.debug("Could not check whether {} is still in use", stagingDir, e);
            }
        }
    }
}
