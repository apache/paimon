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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Executes a full-history copy plan with explicit source and target {@link FileIO}s.
 *
 * <p>Non-overwrite copies stream directly to clone-owned target paths. The clone protocol keeps
 * those paths private until metadata validation succeeds and attempts to clean files created by a
 * failed copy.
 */
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
        long expectedSize =
                file.expectedSize() < 0
                        ? sourceFileIO.getFileSize(file.source())
                        : file.expectedSize();

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
            copyToOwnedTarget(sourceFileIO, targetFileIO, file, expectedSize);
        }

        long targetSize = targetFileIO.getFileSize(file.target());
        if (targetSize != expectedSize) {
            throw new IOException(
                    String.format(
                            "Copied target file %s has unexpected size: expected %s, actual %s.",
                            file.target(), expectedSize, targetSize));
        }
    }

    private static void copyToOwnedTarget(
            FileIO sourceFileIO,
            FileIO targetFileIO,
            FullHistoryCopyPlan.FileCopy file,
            long expectedSize)
            throws IOException {
        boolean targetWriteAttempted = false;
        try (SeekableInputStream input = sourceFileIO.newInputStream(file.source())) {
            targetWriteAttempted = true;
            try (PositionOutputStream output = targetFileIO.newOutputStream(file.target(), false)) {
                copyBytes(input, output, expectedSize);
            }
        } catch (IOException | RuntimeException failure) {
            try {
                if (targetWriteAttempted
                        && targetFileIO.exists(file.target())
                        && !targetFileIO.delete(file.target(), false)) {
                    failure.addSuppressed(
                            new IOException(
                                    "Failed to clean failed clone target " + file.target()));
                }
            } catch (IOException | RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
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

    private FullHistoryFileCopier() {}
}
