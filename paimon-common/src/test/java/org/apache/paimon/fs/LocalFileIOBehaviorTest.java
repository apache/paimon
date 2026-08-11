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

import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LocalFileIO}. */
public class LocalFileIOBehaviorTest extends FileIOBehaviorTestBase {

    @TempDir private java.nio.file.Path tmp;

    @Override
    protected FileIO getFileSystem() {
        return new LocalFileIO();
    }

    @Override
    protected Path getBasePath() {
        return new Path(tmp.toUri());
    }

    @Test
    void testIsObjectStoreReturnsFalse() {
        assertThat(getFileSystem().isObjectStore()).isFalse();
    }

    @Test
    void testFileStatusSnapshotsModificationTime() throws IOException {
        java.nio.file.Path file = Files.createFile(tmp.resolve("snapshot"));
        FileTime firstTimestamp = FileTime.fromMillis(1_000_000L);
        FileTime secondTimestamp = FileTime.fromMillis(2_000_000L);
        Files.setLastModifiedTime(file, firstTimestamp);

        FileIO fileIO = getFileSystem();
        Path path = new Path(file.toUri());
        FileStatus snapshot = fileIO.getFileStatus(path);

        Files.setLastModifiedTime(file, secondTimestamp);

        assertThat(snapshot.getModificationTime()).isEqualTo(firstTimestamp.toMillis());
        assertThat(fileIO.getFileStatus(path).getModificationTime())
                .isEqualTo(secondTimestamp.toMillis());
    }
}
