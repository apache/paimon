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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.function.BinaryOperator;

/** Find latest and earliest snapshot. */
public class SnapshotFinder {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotFinder.class);

    public static final String SNAPSHOT_PREFIX = "snapshot-";

    public static final String EARLIEST = "EARLIEST";

    public static final String LATEST = "LATEST";

    public static Long findLatest(Path snapshotDir) throws IOException {
        return find(snapshotDir, LATEST, Math::max);
    }

    public static Long findEarliest(Path snapshotDir) throws IOException {
        return find(snapshotDir, EARLIEST, Math::min);
    }

    private static Long find(Path snapshotDir, String hintFile, BinaryOperator<Long> reducer)
            throws IOException {
        FileSystem fs = snapshotDir.getFileSystem();
        if (!fs.exists(snapshotDir)) {
            LOG.debug("The snapshot director '{}' is not exist.", snapshotDir);
            return null;
        }

        Path hint = new Path(snapshotDir, hintFile);
        try {
            return Long.parseLong(FileUtils.readFileUtf8(hint));
        } catch (Exception ignore) {
            FileStatus[] statuses = fs.listStatus(snapshotDir);
            if (statuses == null) {
                throw new RuntimeException(
                        "The return value is null of the listStatus for the snapshot directory.");
            }

            Long result = null;
            for (FileStatus status : statuses) {
                String fileName = status.getPath().getName();
                if (fileName.startsWith(SNAPSHOT_PREFIX)) {
                    try {
                        long id = Long.parseLong(fileName.substring(SNAPSHOT_PREFIX.length()));
                        result = result == null ? id : reducer.apply(result, id);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException(
                                "Invalid snapshot file name found " + fileName, e);
                    }
                }
            }
            return result;
        }
    }

    public static void commitLatestHint(Path snapshotDir, long snapshotId) throws IOException {
        commitHint(snapshotDir, snapshotId, LATEST);
    }

    public static void commitEarliestHint(Path snapshotDir, long snapshotId) throws IOException {
        commitHint(snapshotDir, snapshotId, EARLIEST);
    }

    private static void commitHint(Path snapshotDir, long snapshotId, String fileName)
            throws IOException {
        FileSystem fs = snapshotDir.getFileSystem();
        Path hintFile = new Path(snapshotDir, fileName);
        Path tempFile = new Path(snapshotDir, UUID.randomUUID() + "-" + fileName + ".temp");
        FileUtils.writeFileUtf8(tempFile, String.valueOf(snapshotId));
        fs.delete(hintFile, false);
        fs.rename(tempFile, hintFile);
    }
}
