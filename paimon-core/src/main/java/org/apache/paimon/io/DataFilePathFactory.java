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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;

import javax.annotation.concurrent.ThreadSafe;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory which produces new {@link Path}s for data files. */
@ThreadSafe
public class DataFilePathFactory {

    public static final String DATA_FILE_PREFIX = "data-";

    public static final String CHANGELOG_FILE_PREFIX = "changelog-";

    public static final String INDEX_PATH_SUFFIX = ".index";

    private final Path parent;
    private final String uuid;

    private final AtomicInteger pathCount;
    private final String formatIdentifier;

    public DataFilePathFactory(Path parent, String formatIdentifier) {
        this.parent = parent;
        this.uuid = UUID.randomUUID().toString();

        this.pathCount = new AtomicInteger(0);
        this.formatIdentifier = formatIdentifier;
    }

    public Path newPath() {
        return newPath(DATA_FILE_PREFIX);
    }

    public Path newChangelogPath() {
        return newPath(CHANGELOG_FILE_PREFIX);
    }

    private Path newPath(String prefix) {
        String name = prefix + uuid + "-" + pathCount.getAndIncrement() + "." + formatIdentifier;
        return new Path(parent, name);
    }

    public Path toPath(String fileName) {
        return new Path(parent + "/" + fileName);
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    public static Path dataFileToFileIndexPath(Path dataFilePath) {
        return new Path(dataFilePath.getParent(), dataFilePath.getName() + INDEX_PATH_SUFFIX);
    }

    public static Path createNewFileIndexFilePath(Path filePath) {
        String fileName = filePath.getName();
        int dot = fileName.lastIndexOf(".");
        int dash = fileName.lastIndexOf("-");

        if (dash != -1) {
            try {
                int num = Integer.parseInt(fileName.substring(dash + 1, dot));
                return new Path(
                        filePath.getParent(),
                        fileName.substring(0, dash + 1) + (num + 1) + INDEX_PATH_SUFFIX);
            } catch (NumberFormatException ignore) {
                // it is the first index file, has no number
            }
        }
        return new Path(
                filePath.getParent(), fileName.substring(0, dot) + "-" + 1 + INDEX_PATH_SUFFIX);
    }

    public static String formatIdentifier(String fileName) {
        int index = fileName.lastIndexOf('.');
        if (index == -1) {
            throw new IllegalArgumentException(fileName + " is not a legal file name.");
        }

        return fileName.substring(index + 1);
    }
}
