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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/** Executes a full-history copy plan with explicit source and target {@link FileIO}s. */
public class FullHistoryFileCopier {

    private static final int COPY_BUFFER_SIZE = 1 << 20;

    public static void copy(
            FileIO sourceFileIO, FileIO targetFileIO, FullHistoryCopyPlan plan, boolean overwrite)
            throws IOException {
        for (FullHistoryCopyPlan.FileCopy file : plan.files()) {
            copyFile(sourceFileIO, targetFileIO, file, overwrite);
        }
    }

    public static void copyFile(
            FileIO sourceFileIO,
            FileIO targetFileIO,
            FullHistoryCopyPlan.FileCopy file,
            boolean overwrite)
            throws IOException {
        long sourceSize = sourceFileIO.getFileSize(file.source());
        long expectedSize = file.expectedSize() < 0 ? sourceSize : file.expectedSize();
        if (sourceSize != expectedSize) {
            throw new IOException(
                    String.format(
                            "Source file %s has size %s but clone plan expects %s.",
                            file.source(), sourceSize, expectedSize));
        }

        if (!overwrite && targetFileIO.exists(file.target())) {
            long targetSize = targetFileIO.getFileSize(file.target());
            if (targetSize != expectedSize) {
                throw new IOException(
                        String.format(
                                "Target file %s already exists with different size: expected %s, actual %s.",
                                file.target(), expectedSize, targetSize));
            }
            return;
        }

        if (overwrite) {
            try (SeekableInputStream input = sourceFileIO.newInputStream(file.source());
                    PositionOutputStream output =
                            targetFileIO.newOutputStream(file.target(), true)) {
                copyBytes(input, output, expectedSize);
            }
        } else {
            copyTwoPhase(sourceFileIO, targetFileIO, file, expectedSize);
        }

        if (overwrite) {
            long targetSize = targetFileIO.getFileSize(file.target());
            if (targetSize != expectedSize) {
                throw new IOException(
                        String.format(
                                "Copied target file %s has unexpected size: expected %s, actual %s.",
                                file.target(), expectedSize, targetSize));
            }
        }
    }

    private static void copyTwoPhase(
            FileIO sourceFileIO,
            FileIO targetFileIO,
            FullHistoryCopyPlan.FileCopy file,
            long expectedSize)
            throws IOException {
        TwoPhaseOutputStream output = null;
        TwoPhaseOutputStream.Committer committer = null;
        boolean published = false;
        try (SeekableInputStream input = sourceFileIO.newInputStream(file.source())) {
            output = targetFileIO.newTwoPhaseOutputStream(file.target(), false);
            copyBytes(input, output, expectedSize);
            committer = output.closeForCommit();
            output = null;
            committer.commit(targetFileIO);
            published = true;
            long targetSize = targetFileIO.getFileSize(file.target());
            if (targetSize != expectedSize) {
                throw new IOException(
                        String.format(
                                "Copied target file %s has unexpected size: expected %s, actual %s.",
                                file.target(), expectedSize, targetSize));
            }
        } catch (Throwable failure) {
            if (!published) {
                discard(targetFileIO, output, committer, failure);
            }
            if (committer != null && failure instanceof IOException) {
                try {
                    if (targetFileIO.exists(file.target())
                            && targetFileIO.getFileSize(file.target()) == expectedSize) {
                        return;
                    }
                } catch (Throwable validationFailure) {
                    failure.addSuppressed(validationFailure);
                }
            }
            if (failure instanceof IOException) {
                throw (IOException) failure;
            } else if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            } else if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new IOException(failure);
        }
    }

    private static void copyBytes(SeekableInputStream input, OutputStream output, long expectedSize)
            throws IOException {
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        long copied = 0;
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (read > 0) {
                output.write(buffer, 0, read);
                copied += read;
            }
        }
        if (copied != expectedSize) {
            throw new IOException(
                    String.format(
                            "Copied %s bytes from source but clone plan expects %s.",
                            copied, expectedSize));
        }
    }

    private static void discard(
            FileIO targetFileIO,
            TwoPhaseOutputStream output,
            TwoPhaseOutputStream.Committer committer,
            Throwable failure) {
        if (committer == null && output != null) {
            try {
                committer = output.closeForCommit();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        if (committer != null) {
            try {
                committer.discard(targetFileIO);
            } catch (Throwable discardFailure) {
                failure.addSuppressed(discardFailure);
            }
        }
    }

    private FullHistoryFileCopier() {}
}
