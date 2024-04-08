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

    public static final String INDEX_PATH_PREFIX = "index-";

    public static final String INDEX_PATH_SUFFIX = "index";

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

    public Path newIndexPath() {
        return newPath(INDEX_PATH_PREFIX, INDEX_PATH_SUFFIX);
    }

    private Path newPath(String prefix, String subffix) {
        String name = prefix + uuid + "-" + pathCount.getAndIncrement() + "." + subffix;
        return new Path(parent, name);
    }

    private Path newPath(String prefix) {
        return newPath(prefix, formatIdentifier);
    }

    public Path toPath(String fileName) {
        return new Path(parent + "/" + fileName);
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    public static String formatIdentifier(String fileName) {
        int index = fileName.lastIndexOf('.');
        if (index == -1) {
            throw new IllegalArgumentException(fileName + " is not a legal file name.");
        }

        return fileName.substring(index + 1);
    }
}
