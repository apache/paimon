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

package org.apache.paimon.fs.local;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.OperatingSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link LocalFileIO}. */
public class LocalFileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCopy() throws Exception {
        Path srcFile = new Path(tempDir.resolve("src.txt").toUri());
        Path dstFile = new Path(tempDir.resolve("dst.txt").toUri());

        FileIO fileIO = new LocalFileIO();
        fileIO.tryToWriteAtomic(srcFile, "foobar");

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
    }

    @Test
    public void testListStatusWithColonInChildName() throws Exception {
        assumeThat(OperatingSystem.isWindows())
                .describedAs("Windows does not allow ':' in file names.")
                .isFalse();
        Files.createFile(tempDir.resolve("tag-demo:old"));
        Files.createFile(tempDir.resolve("plain.txt"));

        FileIO fileIO = new LocalFileIO();
        FileStatus[] statuses = fileIO.listStatus(new Path(tempDir.toString()));

        assertThat(statuses)
                .extracting(status -> status.getPath().getName())
                .containsExactlyInAnyOrder("tag-demo:old", "plain.txt");
    }
}
