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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
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

    @Nullable
    public static Long findLatest(FileIO fileIO, Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
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
                if (fileIO.exists(path)) {
                    return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
                }
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
