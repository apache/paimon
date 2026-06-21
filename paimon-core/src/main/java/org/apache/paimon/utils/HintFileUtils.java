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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Utils for hint files. */
public class HintFileUtils {

    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";

    private static final int READ_HINT_RETRY_NUM = 3;
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    /** Lookup mode for discovering the latest snapshot. */
    public enum LatestLookupMode {
        NORMAL,
        RECOVERY_REQUIRING_LIST
    }

    @Nullable
    public static Long findLatest(FileIO fileIO, Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return findLatest(fileIO, dir, prefix, file, LatestLookupMode.NORMAL);
    }

    @Nullable
    public static Long findLatest(
            FileIO fileIO,
            Path dir,
            String prefix,
            Function<Long, Path> file,
            LatestLookupMode mode)
            throws IOException {
        if (mode == LatestLookupMode.RECOVERY_REQUIRING_LIST) {
            return findLatestByListForRecovery(fileIO, dir, prefix);
        }

        if (fileIO.isObjectStore()
                && fileIO.supportsAtomicCreateWithoutOverwrite(
                        file.apply(Snapshot.FIRST_SNAPSHOT_ID))) {
            Optional<Long> latestHint = readLatestHintForNoListObjectStore(fileIO, LATEST, dir);
            Long snapshotId = latestHint.orElse(null);
            if (supportsNoListSnapshotCommit(fileIO, file, snapshotId)) {
                return snapshotId == null
                        ? null
                        : findLatestForObjectStore(fileIO, file, snapshotId);
            }
        }

        return findLatestByHintThenList(fileIO, dir, prefix, file);
    }

    @Nullable
    private static Long findLatestByHintThenList(
            FileIO fileIO, Path dir, String prefix, Function<Long, Path> file) throws IOException {
        Long snapshotId = readHint(fileIO, LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(nextSnapshot))) {
                return snapshotId;
            }
        }
        return findByListFiles(fileIO, Math::max, dir, prefix);
    }

    private static boolean supportsNoListSnapshotCommit(
            FileIO fileIO, Function<Long, Path> file, @Nullable Long latestHint)
            throws IOException {
        long nextSnapshot = latestHint == null ? Snapshot.FIRST_SNAPSHOT_ID : latestHint + 1;
        return fileIO.supportsAtomicCreateWithoutOverwrite(file.apply(nextSnapshot));
    }

    private static Long findLatestForObjectStore(
            FileIO fileIO, Function<Long, Path> file, Long latestHint) {
        Path snapshotPath = file.apply(latestHint);
        String snapshotJson;
        try {
            snapshotJson = fileIO.readFileUtf8(snapshotPath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(
                    "Cannot safely use LATEST hint "
                            + latestHint
                            + " because "
                            + snapshotPath
                            + " is missing.",
                    e);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot safely use LATEST hint "
                            + latestHint
                            + " because "
                            + snapshotPath
                            + " is unreadable.",
                    e);
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "Cannot safely use LATEST hint "
                            + latestHint
                            + " because "
                            + snapshotPath
                            + " is unreadable.",
                    e);
        }

        Snapshot snapshot;
        try {
            snapshot = Snapshot.fromJson(snapshotJson);
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "Cannot safely use LATEST hint "
                            + latestHint
                            + " because "
                            + snapshotPath
                            + " is malformed.",
                    e);
        }

        if (snapshot.id() != latestHint) {
            throw new RuntimeException(
                    "Cannot safely use LATEST hint "
                            + latestHint
                            + " because "
                            + snapshotPath
                            + " contains snapshot "
                            + snapshot.id()
                            + ".");
        }

        return latestHint;
    }

    @Nullable
    private static Long findLatestByListForRecovery(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        try {
            return findByListFiles(fileIO, Math::max, dir, prefix);
        } catch (IOException e) {
            throw new IOException(
                    "Recovery requires ListBucket permission to list table prefix "
                            + dir
                            + " for files with prefix "
                            + prefix
                            + ", or restoring a valid LATEST hint.",
                    e);
        }
    }

    @Nullable
    public static Long findEarliest(
            FileIO fileIO, Path dir, String prefix, Function<Long, Path> file) throws IOException {
        Long snapshotId = readHint(fileIO, EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(fileIO, Math::min, dir, prefix);
    }

    public static Long readHint(FileIO fileIO, String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private static Optional<Long> readLatestHintForNoListObjectStore(
            FileIO fileIO, String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                Optional<String> hint = fileIO.readOverwrittenFileUtf8(path);
                if (!hint.isPresent()) {
                    return Optional.empty();
                }

                long snapshotId = Long.parseLong(hint.get());
                if (snapshotId <= 0) {
                    throw new NumberFormatException("Latest snapshot id must be positive.");
                }
                return Optional.of(snapshotId);
            } catch (NumberFormatException e) {
                throw new RuntimeException(
                        "Cannot safely use LATEST hint because it is malformed.", e);
            } catch (FileNotFoundException e) {
                return Optional.empty();
            } catch (IOException | RuntimeException e) {
                exception = e;
                if (retryNumber < READ_HINT_RETRY_NUM) {
                    sleepBeforeRetry();
                }
            }
        }

        throw new RuntimeException(
                "Cannot safely determine latest snapshot because LATEST hint is unreadable.",
                exception);
    }

    private static void sleepBeforeRetry() {
        try {
            TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static Long findByListFiles(
            FileIO fileIO, BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    public static void commitLatestHint(FileIO fileIO, long id, Path dir) throws IOException {
        commitHint(fileIO, id, LATEST, dir);
    }

    public static void commitEarliestHint(FileIO fileIO, long id, Path dir) throws IOException {
        commitHint(fileIO, id, EARLIEST, dir);
    }

    public static void deleteLatestHint(FileIO fileIO, Path dir) throws IOException {
        Path hintFile = new Path(dir, LATEST);
        fileIO.delete(hintFile, false);
    }

    public static void deleteEarliestHint(FileIO fileIO, Path dir) throws IOException {
        Path hintFile = new Path(dir, EARLIEST);
        fileIO.delete(hintFile, false);
    }

    public static void commitHint(FileIO fileIO, long id, String fileName, Path dir)
            throws IOException {
        Path hintFile = new Path(dir, fileName);
        int loopTime = 3;
        while (loopTime-- > 0) {
            try {
                fileIO.overwriteHintFile(hintFile, String.valueOf(id));
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1000) + 500);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    // throw root cause
                    throw new RuntimeException(e);
                }
                if (loopTime == 0) {
                    throw e;
                }
            }
        }
    }
}
