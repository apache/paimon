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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RenamingTwoPhaseOutputStream}. */
public class RenamingTwoPhaseOutputStreamTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path targetPath;

    @BeforeEach
    void setup() {
        fileIO = new LocalFileIO();
        targetPath = new Path(tempDir.resolve("target-file.txt").toString());
    }

    @Test
    void testSuccessfulCommit() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        // Write some data
        String testData = "Hello, World!";
        stream.write(testData.getBytes());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Target file should not exist yet
        assertThat(fileIO.exists(targetPath)).isFalse();

        // Commit the file
        committer.commit(fileIO);

        // Now target file should exist with correct content
        assertThat(fileIO.exists(targetPath)).isTrue();

        // Read and verify content
        byte[] content = Files.readAllBytes(Paths.get(targetPath.toString()));
        assertThat(new String(content)).isEqualTo(testData);
    }

    @Test
    void testDiscard() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        // Write some data
        stream.write("Some data".getBytes());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Discard instead of commit
        committer.discard(fileIO);

        // Target file should not exist
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testCloseWithoutCommit() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        // Write some data
        stream.write("Some data".getBytes());

        // Just close (not closeForCommit)
        stream.close();

        // Target file should not exist (temp file cleaned up)
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testDoubleCommitThrows() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // First commit should succeed
        committer.commit(fileIO);

        // Second commit should throw
        assertThatThrownBy(() -> committer.commit(fileIO)).isInstanceOf(IOException.class);
    }

    @Test
    void testPositionTracking() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        assertThat(stream.getPos()).isEqualTo(0);

        stream.write("Hello".getBytes());
        assertThat(stream.getPos()).isEqualTo(5);

        stream.write(" World!".getBytes());
        assertThat(stream.getPos()).isEqualTo(12);

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        committer.commit(fileIO);

        // Verify final content
        byte[] content = Files.readAllBytes(Paths.get(targetPath.toString()));
        assertThat(new String(content)).isEqualTo("Hello World!");
    }
}
